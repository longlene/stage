%% @doc
%% A GenStage producer that creates a producer from lists or generator functions.
%% This is an Erlang-native replacement for Elixir's GenStage.Streamer.
%% @end
-module(gen_stage_list_producer).

-behaviour(gen_stage).

-export([start_link/1, start_link/2, start/1, start/2]).

%% gen_stage callbacks
-export([init/1, handle_demand/2, handle_subscribe/4, handle_cancel/3, handle_info/2]).

-record(state, {
    stack :: list(),
    continuation :: function() | atom(),
    on_cancel :: #{reference() => pid()} | undefined
}).

%% @doc
%% Start a streamer with just the enumerable function and default options.
%% @end
-spec start_link(function() | {from_list, list()}) -> gen_stage:on_start().
start_link(EnumFun) ->
    start_link(EnumFun, []).

%% @doc
%% Start a streamer with enumerable function and options.
%% @end
-spec start_link(function() | {from_list, list()}, proplists:proplist()) -> gen_stage:on_start().
start_link(EnumFun, Opts) ->
    Stack = get_stacktrace(),
    gen_stage:start_link(?MODULE, {EnumFun, Stack, Opts}, Opts).

%% @doc
%% Start a streamer without linking.
%% @end
-spec start(function() | {from_list, list()}) -> gen_stage:on_start().
start(EnumFun) ->
    start(EnumFun, []).

%% @doc
%% Start a streamer with enumerable function and options without linking.
%% @end
-spec start(function() | {from_list, list()}, proplists:proplist()) -> gen_stage:on_start().
start(EnumFun, Opts) ->
    Stack = get_stacktrace(),
    gen_stage:start(?MODULE, {EnumFun, Stack, Opts}, Opts).

%% @private
get_stacktrace() ->
    try throw(stack) 
    catch 
        throw:stack:Stacktrace -> Stacktrace
    end.

%% @doc
%% Initialize the streamer with the enumerable function or list.
%% @end
init({{from_list, List}, Stack, Opts}) ->
    %% Create a continuation function for the list
    Continuation = create_list_continuation(List),
    
    OnCancel = case proplists:get_value(on_cancel, Opts, continue) of
        continue -> undefined;
        stop -> #{}
    end,
    
    State = #state{
        stack = Stack,
        continuation = Continuation,
        on_cancel = OnCancel
    },
    
    %% Only keep dispatcher and demand options for gen_stage
    GenStageOpts = lists:filter(fun({K, _}) -> 
        lists:member(K, [dispatcher, demand]) 
    end, Opts),
    
    {producer, State, GenStageOpts};
init({EnumFun, Stack, Opts}) ->
    %% Create a continuation function that will be called with demand
    Continuation = create_continuation(EnumFun),
    
    OnCancel = case proplists:get_value(on_cancel, Opts, continue) of
        continue -> undefined;
        stop -> #{}
    end,
    
    State = #state{
        stack = Stack,
        continuation = Continuation,
        on_cancel = OnCancel
    },
    
    %% Only keep dispatcher and demand options for gen_stage
    GenStageOpts = lists:filter(fun({K, _}) -> 
        lists:member(K, [dispatcher, demand]) 
    end, Opts),
    
    {producer, State, GenStageOpts}.

%% @private
create_list_continuation(List) ->
    %% Create a continuation that manages the list state
    create_list_cont(List).

%% @private
create_list_cont([]) ->
    fun(_) -> {done, {[], 0}} end;
create_list_cont(List) ->
    fun({cont, {Events, Demand}}) when Demand > 0 ->
        get_list_events(List, Events, Demand, []);
       (_) ->
        {done, {[], 0}}
    end.

%% @private
get_list_events([], Events, _Demand, Acc) ->
    {done, {lists:reverse(Acc) ++ Events, 0}};
get_list_events(Remaining, Events, 0, Acc) ->
    NextCont = create_list_cont(Remaining),
    {suspended, {lists:reverse(Acc) ++ Events, 0}, NextCont};
get_list_events([H|T], Events, Demand, Acc) ->
    get_list_events(T, Events, Demand - 1, [H | Acc]).

%% @private
create_continuation(EnumFun) ->
    fun({cont, {Events, Demand}}) when Demand > 0 ->
            get_events(EnumFun, Events, Demand, []);
       (_) ->
            {done, {[], 0}}
    end.

%% @private
%% Exceptions raised by EnumFun are not caught: like Elixir's
%% GenStage.Streamer, the producer crashes instead of silently
%% treating the error as the end of the stream.
get_events(EnumFun, Events, 0, Acc) ->
    {suspended, {lists:reverse(Acc) ++ Events, 0}, create_continuation(EnumFun)};
get_events(EnumFun, Events, Demand, Acc) ->
    case EnumFun() of
        {value, Value} ->
            get_events(EnumFun, Events, Demand - 1, [Value | Acc]);
        done ->
            {done, {lists:reverse(Acc) ++ Events, 0}}
    end.

%% @doc
%% Handle consumer subscription.
%% @end
handle_subscribe(consumer, _Opts, {Pid, Ref}, State) ->
    case State#state.on_cancel of
        undefined ->
            {automatic, State};
        OnCancel ->
            NewOnCancel = maps:put(Ref, Pid, OnCancel),
            {automatic, State#state{on_cancel = NewOnCancel}}
    end.

%% @doc
%% Handle consumer cancellation.
%% @end
handle_cancel(_Reason, {_, Ref}, State) ->
    case State#state.on_cancel of
        #{} = OnCancel when map_size(OnCancel) =:= 1 ->
            case maps:is_key(Ref, OnCancel) of
                true ->
                    {stop, normal, State#state{on_cancel = maps:remove(Ref, OnCancel)}};
                false ->
                    {noreply, [], State}
            end;
        #{} = OnCancel ->
            case maps:is_key(Ref, OnCancel) of
                true ->
                    {noreply, [], State#state{on_cancel = maps:remove(Ref, OnCancel)}};
                false ->
                    {noreply, [], State}
            end;
        undefined ->
            {noreply, [], State}
    end.

%% @doc
%% Handle demand from consumers.
%% @end
handle_demand(_Demand, #state{continuation = done} = State) ->
    {noreply, [], State};
handle_demand(Demand, #state{continuation = Continuation} = State) when Demand > 0 ->
    case Continuation({cont, {[], Demand}}) of
        {suspended, {List, 0}, NewContinuation} ->
            {noreply, List, State#state{continuation = NewContinuation}};
        {done, {List, _}} ->
            gen_stage:async_info(self(), stop),
            {noreply, List, State#state{continuation = done}}
    end.

%% @doc
%% Handle info messages.
%% @end
handle_info(stop, State) ->
    {stop, normal, State};
handle_info(Msg, #state{stack = Stack} = State) ->
    error_logger:warning_msg(
        "** Undefined handle_info in ~p~n** Unhandled message: ~p~n** Stream started at:~n~p~n",
        [?MODULE, Msg, Stack]
    ),
    {noreply, [], State}.