-module(gen_stage).
%%% ---------------------------------------------------
%%%
%%% Stages are data-exchange steps that send and/or receive data
%%% from other stages.
%%%
%%% When a stage sends data, it acts as a producer. When it receives
%%% data, it acts as a consumer. Stages may take both producer and
%%% consumer roles at once.
%%%
%%% ## Stage types
%%%
%%% Besieds taking both producer and consumer roles, a stage may be
%%% called "source" if it only produces items or called "sink" if it
%%% only cousumes items.
%%%
%%% For example, imagine the stages below where A sends data to B
%%% that sends data to C:
%%%
%%%     [A] -> [B] -> [C]
%%%
%%% we conclude that:
%%%
%%%   * A is only a producer (and therefore a source)





-behaviour(gen_server).

-export([
         start_link/2,
         start_link/3,
         start_link/4,
         start/2,
         start/3,
         start/4,
         sync_info/2,
         sync_info/3,
         async_info/2,
         demand/1,
         demand/2,
         sync_subscribe/2,
         sync_subscribe/3,
         sync_resubscribe/4,
         sync_resubscribe/5,
         async_subscribe/2,
         async_resubscribe/4,
         ask/2,
         ask/3,
         cancel/2,
         cancel/3,
         call/2,
         call/3,
         cast/2,
         reply/2,
         stop/1,
         stop/3,

         consumer_receive/4,
         consumer_subscribe/4
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TIMEOUT, 5000).

-record(stage, {
          mod,
          state,
          type,
          dispatcher_mod,
          dispatcher_state,
          buffer,
          buffer_keep,
          events = forward,
          monitors = #{},
          producers = #{},
          consumers = #{}
         }).

% gen_server type
-type server() :: pid() | atom() | {global, term()} | {via, module(), term()} | {atom(), node()}.

-type on_start() :: {ok, pid()} | ignore | {error, {already_started, pid()} | term()}.

-type name() :: atom() | {global, term()} | {via, module(), term()}.

-type options() :: [option()].
-type option() ::
{debug, [debug()]}
| {timeout, timeout()}
| {spawn_opt, [term()]}
| {hibernate_after, timeout()}.

-type debug() :: trace | log | statistics | {log_to_file, iolist() | binary()}.

%-type type() :: producer | consumer | producer_consumer.

-type subscription_option() ::
{cancel, permanent | transient | temporary}
| {to, server()}
| {min_demand, integer()}
| {max_demand, integer()}
| {atom(), term()}.

-type subscription_options() :: [subscription_option()].

-type producer_only_option() :: {demand, forward | accumulate}.

-type producer_and_producer_consumer_option() ::
{buffer_size, non_neg_integer() | infinity}
| {buffer_keep, first | last}
| {dispatcher, module() | {module(), gen_stage_dispatcher:options()}}.

-type consumer_and_producer_consumer_option() ::
{subscribe_to, [module() | {module(), subscription_options()}]}.

-type producer_option() :: producer_only_option() | producer_and_producer_consumer_option().

-type consumer_option() :: consumer_and_producer_consumer_option().

-type producer_consumer_option() ::
producer_and_producer_consumer_option() | consumer_and_producer_consumer_option().

-type stage() :: pid() | atom() | {global, term()} | {via, module(), term()} | {atom(), node()}.

-type subscription_tag() :: reference().

-type from() :: {pid(), subscription_tag()}.

-callback init(Args :: term()) -> 
    {producer, State}
    | {producer, State :: any(), [producer_option()]}
    | {producer_consumer, State}
    | {producer_consumer, State, [producer_consumer_option()]}
    | {consumer, State}
    | {consumer, State, [consumer_option()]}
    | ignore
    | {stop, Reason :: any()}
      when State :: any().

-callback handle_demand(Demand :: pos_integer(), State :: term()) ->
    {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason, NewState}
      when NewState :: term(), Reason :: term(), Event :: term().

-callback handle_subscribe(Type :: producer | consumer, subscription_options(), from(), State :: term()) ->
    {automatic | manual, NewState}
    | {stop, Reason, NewState}
      when NewState :: term(), Reason :: term().

-callback handle_cancel(CancellationReason :: {cancel | down, Reason}, from(), State :: term()) ->
    {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason, NewState}
      when Event :: term(), NewState :: term(), Reason :: term().

-callback handle_events(Events :: [Event], from(), State :: term()) ->
    {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason, NewState}
      when NewState :: term(), Reason :: term(), Event :: term().

-callback handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
    {reply, Reply, [Event], NewState}
    | {reply, Reply, [Event], NewState, hibernate}
    | {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason, Reply, NewState}
    | {stop, Reason, NewState}
      when Reply :: term(), NewState :: term(), Reason :: term(), Event :: term().

-callback handle_cast(Request :: term(), State :: term()) ->
    {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason :: term(), NewState}
      when NewState :: term(), Event :: term().

-callback handle_info(Message :: term(), State :: term()) ->
    {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason :: term(), NewState}
      when NewState :: term(), Event :: term().

-callback terminate(Reason, State :: term()) -> term()
                                                  when Reason :: normal | shutdown | {shutdown, term()} | term().

-callback code_change(OldVsn :: term(), State :: term(), Extra :: term()) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.

-callback format_status(normal | terminate, [{term(), term()} | (State :: term())]) ->
    Status :: term().

-optional_callbacks([
                     % gen_stage
                     handle_subscribe/4,
                     handle_cancel/3,
                     handle_demand/2,
                     handle_events/3,
                     % gen_server
                     code_change/3,
                     format_status/2,
                     handle_call/3,
                     handle_cast/2,
                     handle_info/2,
                     terminate/2
                    ]).

-spec start_link(module(), term()) -> on_start().
start_link(Mod, Args) ->
    start_link(Mod, Args, []).

-spec start_link(module(), term(), options()) -> on_start().
start_link(Mod, Args, Options) ->
    gen_server:start_link(?MODULE, {Mod, Args}, Options).

-spec start_link(name(), module(), term(), options()) -> on_start().
start_link(Name, Mod, Args, Options) when is_atom(Mod), is_list(Options) ->
    gen_server:start_link(Name, ?MODULE, {Mod, Args}, Options).

-spec start(module(), term()) -> on_start().
start(Mod, Args) ->
    start(Mod, Args, []).

-spec start(module(), term(), options()) -> on_start().
start(Mod, Args, Options) ->
    gen_server:start(?MODULE, {Mod, Args}, Options).

-spec start(name(), module(), term(), options()) -> on_start().
start(Name, Mod, Args, Options) when is_atom(Mod), is_list(Options) ->
    gen_server:start(Name, ?MODULE, {Mod, Args}, Options).

-spec sync_info(stage(), term()) -> ok.
sync_info(Stage, Msg) ->
    sync_info(Stage, Msg, ?TIMEOUT).

-spec sync_info(stage(), term(), timeout()) -> ok.
sync_info(Stage, Msg, Timeout) ->
    call(Stage, {'$info', Msg}, Timeout).

-spec async_info(stage(), term()) -> ok.
async_info(Stage, Msg) ->
    cast(Stage, {'$info', Msg}).

-spec demand(stage()) -> forward | accumulate.
demand(Stage) ->
    call(Stage, '$demand').

-spec demand(stage(), forward | accumulate) -> ok.
demand(Stage, Mode) ->
    cast(Stage, {'$demand', Mode}).

-spec sync_subscribe(stage(), subscription_options()) -> {ok, subscription_tag()} | {error, not_a_consumer} | {error, {bad_opts, iolist() | binary()}}.
sync_subscribe(Stage, Opts) ->
    sync_subscribe(Stage, Opts, ?TIMEOUT).

-spec sync_subscribe(stage(), subscription_options(), timeout()) -> {ok, subscription_tag()} | {error, not_a_consumer} | {error, {bad_opts, iolist() | binary()}}.
sync_subscribe(Stage, Opts, Timeout) ->
    sync_subscribe(Stage, undefined, Opts, Timeout).
%% TODO check paramter

-spec sync_resubscribe(stage(), subscription_tag(), term(), subscription_options()) -> {ok, subscription_tag()} | {error, not_a_consumer} | {error, {bad_opts, iolist() | binary()}}.
sync_resubscribe(Stage, SubscriptionTag, Reason, Opts) ->
    sync_resubscribe(Stage, SubscriptionTag, Reason, Opts, ?TIMEOUT).

-spec sync_resubscribe(stage(), subscription_tag(), term(), subscription_options(), timeout()) -> {ok, subscription_tag()} | {error, not_a_consumer} | {error, {bad_opts, iolist() | binary()}}.
sync_resubscribe(Stage, SubscriptionTag, Reason, Opts, Timeout) ->
    sync_subscribe(Stage, {SubscriptionTag, Reason}, Opts, Timeout).

sync_subscribe(Stage, Cancel, Opts, Timeout) ->
    Fun = fun() -> throw(iolist_to_binary(io_lib:format("expected to argument in sync_(re)subscribe", []))) end,
    {To, NewOpts} = stage_keyword:pop_lazy(Opts, to, Fun),
    call(Stage, {'$subscribe', Cancel, To, NewOpts}, Timeout).

-spec async_subscribe(stage(), subscription_options()) -> ok.
async_subscribe(Stage, Opts) ->
    async_subscribe(Stage, undefined, Opts).

-spec async_resubscribe(stage(), subscription_tag(), term(), subscription_options()) -> ok.
async_resubscribe(Stage, SubscriptionTag, Reason, Opts) ->
    async_subscribe(Stage, {SubscriptionTag, Reason}, Opts).

async_subscribe(Stage, Cancel, Opts) ->
    Fun = fun() -> throw(iolist_to_binary(io_lib:format("expected to argument in async_(re)subscribe", []))) end,
    {To, NewOpts} = stage_keyword:pop_lazy(Opts, to, Fun),
    cast(Stage, {'$subscribe', Cancel, To, NewOpts}).

-spec ask(from(), non_neg_integer()) -> ok | noconnect | nosuspend.
ask(ProducerSubscription, Demand) ->
    ask(ProducerSubscription, Demand, []).

-spec ask(from(), non_neg_integer(), [noconnect | nosuspend]) -> ok | noconnect | nosuspend.
ask({_Pid, _Ref}, 0, _Opts) ->
    ok;
ask({Pid, Ref}, Demand, Opts) when is_integer(Demand), Demand > 0 ->
    erlang:send(Pid, {'$gen_producer', {self(), Ref}, {ask, Demand}}, Opts).

-spec cancel(from(), term()) -> ok | noconnect | nosuspend.
cancel({Pid, Ref} = _ProducerSubscription, Reason) ->
    cancel({Pid, Ref}, Reason, []).

-spec cancel(from(), term(), [noconnect | nosuspend]) -> ok | noconnect | nosuspend.
cancel({Pid, Ref} = _ProducerSubscription, Reason, Opts) ->
    erlang:send(Pid, {'$gen_producer', {self(), Ref}, {cancel, Reason}}, Opts).

send_noconnect(Pid, Msg) ->
    erlang:send(Pid, Msg, [noconnect]).

-spec call(stage(), term()) -> term().
call(Stage, Request) ->
    call(Stage, Request, ?TIMEOUT).

-spec call(stage(), term(), timeout()) -> term().
call(Stage, Request, Timeout) ->
    gen_server:call(Stage, Request, Timeout).

-spec cast(stage(), term()) -> ok.
cast(Stage, Request) ->
    gen_server:cast(Stage, Request).

reply({To, Tag}, Reply) when is_pid(To) ->
    try
        erlang:send(To, {Tag, Reply}),
        ok
    catch
        _:_ -> ok
    end.

stop(Stage) ->
    stop(Stage, normal, infinity).

-spec stop(stage(), term(), timeout()) -> ok.
stop(Stage, Reason, Timeout) ->
    gen:stop(Stage, Reason, Timeout).

% useless
% from_enumerable(Stream, Opts)
% stream(Subscriptions, Options) when is_list(Subscriptions) 

init({Mod, Args}) ->
    case Mod:init(Args) of
        {producer, State} ->
            init_producer(Mod, [], State);
        {producer, State, Opts} ->
            init_producer(Mod, Opts, State);
        {producer_consumer, State} ->
            init_producer_consumer(Mod, [], State);
        {producer_consumer, State, Opts} when is_list(Opts) ->
            init_producer_consumer(Mod, Opts, State);
        {consumer, State} ->
            init_consumer(Mod, [], State);
        {consumer, State, Opts} when is_list(Opts) ->
            init_consumer(Mod, Opts, State);
        {stop, _} = Stop ->
            Stop;
        ignore ->
            ignore;
        Other ->
            {stop, {bad_return_value, Other}}
    end.

init_producer(Mod, Opts, State) ->
    case init_dispatcher(Opts) of
        {ok, DispatcherMod, DispatcherState, Opts1} ->
            case stage_util:validate_integer(Opts1, buffer_size, 10000, 0, infinity, true) of
                {ok, BufferSize, Opts2} ->
                    case stage_util:validate_in(Opts2, buffer_keep, last, [first, last]) of
                        {ok, BufferKeep, Opts3} ->
                            case stage_util:validate_in(Opts3, demand, forward, [accumulate, forward]) of
                                {ok, Demand, Opts4} ->
                                    case stage_util:validate_no_opts(Opts4) of
                                        ok ->
                                            Events =
                                            case Demand of
                                                accumulate -> [];
                                                _ -> forward
                                            end,

                                            Stage = #stage{
                                                       mod = Mod,
                                                       state = State,
                                                       type = producer,
                                                       buffer = stage_buffer:new(BufferSize),
                                                       buffer_keep = BufferKeep,
                                                       events = Events,
                                                       dispatcher_mod = DispatcherMod,
                                                       dispatcher_state = DispatcherState
                                                      },
                                            {ok, Stage};
                                        {error, Message} ->
                                            {stop, {bad_opts, Message}}
                                    end;
                                {error, Message} ->
                                    {stop, {bad_opts, Message}}
                            end;
                        {error, Message} ->
                            {stop, {bad_opts, Message}}
                    end;
                {error, Message} ->
                    {stop, {bad_opts, Message}}
            end;
        {error, Message} ->
            {stop, {bad_opts, Message}}
    end.

init_dispatcher(Opts) ->
    case stage_keyword:pop(Opts, dispatcher, stage_demand_dispatcher) of
        {Dispatcher, NewOpts} when is_atom(Dispatcher) ->
            {ok, DispatcherState} = Dispatcher:init([]),
            {ok, Dispatcher, DispatcherState, NewOpts};
        {{Dispatcher, DispatcherOpts}, NewOpts} when is_atom(Dispatcher), is_list(DispatcherOpts) ->
            {ok, DispatcherState} = Dispatcher:init(DispatcherOpts),
            {ok, Dispatcher, DispatcherState, NewOpts};
        {Other, _NewOpts} ->
            {error, io_lib:format("expected dispatcher to be an atom or a {atom(), list()}, got ~p", [Other])}
    end.

init_producer_consumer(Mod, Opts, State) ->
    case init_dispatcher(Opts) of
        {ok, DispatcherMod, DispatcherState, Opts1} ->
            case stage_util:validate_list(Opts1, subscribe_to, []) of
                {ok, SubscribeTo, Opts2} ->
                    case stage_util:validate_integer(Opts2, buffer_size, infinity, 0, infinity, true) of
                        {ok, BufferSize, Opts3} ->
                            case stage_util:validate_in(Opts3, buffer_keep, last, [first, last]) of
                                {ok, BufferKeep, Opts4} ->
                                    case stage_util:validate_no_opts(Opts4) of
                                        ok ->
                                            Stage = #stage{
                                                       mod = Mod,
                                                       state = State,
                                                       type = producer_consumer,
                                                       buffer = stage_buffer:new(BufferSize),
                                                       buffer_keep = BufferKeep,
                                                       events = {queue:new(), 0},
                                                       dispatcher_mod = DispatcherMod,
                                                       dispatcher_state = DispatcherState
                                                      },
                                            consumer_init_subscribe(SubscribeTo, Stage);
                                        {error, Message} ->
                                            {stop, {bad_opts, Message}}
                                    end;
                                {error, Message} ->
                                    {stop, {bad_opts, Message}}
                            end;
                        {error, Message} ->
                            {stop, {bad_opts, Message}}
                    end;
                {error, Msg} ->
                    {stop, {bad_opts, Msg}}
            end;
        {error, Message} ->
            {stop, {bad_opts, Message}}
    end.

init_consumer(Mod, Opts, State) ->
    case stage_util:validate_list(Opts, subscribe_to, []) of
        {ok, SubscribeTo, NewOpts} ->
            case stage_util:validate_no_opts(NewOpts) of
                ok ->
                    Stage = #stage{mod = Mod, state = State, type = consumer},
                    consumer_init_subscribe(SubscribeTo, Stage);
                {error, Msg} ->
                    {stop, {bad_opts, Msg}}
            end;
        {error, Msg} ->
            {stop, {bad_opts, Msg}}
    end.

handle_call({'$info', Msg}, _From, Stage) ->
    producer_info(Msg, Stage);
handle_call('$demand', _From, Stage) ->
    producer_demand(Stage);
handle_call({'$subscribe', Current, To, Opts}, _From, Stage) ->
    consumer_subscribe(Current, To, Opts, Stage);
handle_call(Msg, From, #stage{mod = Mod, state = State} = Stage) ->
    case Mod:handle_call(Msg, From, State) of
        {reply, Reply, Events, NewState} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            {reply, Reply, NewStage#stage{state = NewState}};
        {reply, Reply, Events, NewState, hibernate} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            {reply, Reply, NewStage#stage{state = NewState}, hibernate};
        {stop, Reason, Reply, NewState} ->
            {stop, Reason, Reply, Stage#stage{state = NewState}};
        Return ->
            handle_noreply_callback(Return, Stage)
    end.

handle_cast({'$info', Msg}, Stage) ->
    {reply, _, NewStage} = producer_info(Msg, Stage),
    {noreply, NewStage};
handle_cast({'$demand', Mode}, Stage) ->
    producer_demand(Mode, Stage);
handle_cast({'$subscribe', Current, To, Opts}, Stage) ->
    case consumer_subscribe(Current, To, Opts, Stage) of
        {reply, _, NewStage} -> {noreply, NewStage};
        {stop, Reason, _, NewStage} -> {stop, Reason, NewStage};
        {stop, _, _} = Stop -> Stop
    end;
handle_cast(Msg, #stage{state = State} = Stage) ->
    noreply_callback(handle_cast, [Msg, State], Stage).

handle_info({'DOWN', Ref, _, _, Reason} = Msg, Stage) ->
    #stage{producers = Producers, monitors = Monitors, state = State} = Stage,
    case Producers of
        #{Ref := _} ->
            consumer_cancel(Ref, down, Reason, Stage);
        #{} ->
            case Monitors of
                #{Ref := ConsumerRef} ->
                    producer_cancel(ConsumerRef, down, Reason, Stage);
                #{} ->
                    noreply_callback(handle_info, [Msg, State], Stage)
            end
    end;
handle_info({'$gen_producer', _, _} = Msg, #stage{type = consumer} = Stage) ->
    ErrMsg = "gen_stage consumer ~tp received $gen_producer message: ~tp~n",
    error_logger:error_msg(ErrMsg, [stage_util:self_name(), Msg]),
    {noreply, Stage};
handle_info({'$gen_producer', {ConsumerPid, Ref} = From, {subscribe, Cancel, Opts}}, #stage{consumers = Consumers} = Stage) ->
    case Consumers of
        #{Ref := _} ->
            ErrMsg = "gen_stage producer ~tp received duplicated subscription from: ~tp~n",
            error_logger:error_msg(ErrMsg, [stage_util:self_name(), From]),
            Msg = {'$gen_consumer', {self(), Ref}, {cancel, duplicated_subscription}},
            send_noconnect(ConsumerPid, Msg),
            {noreply, Stage};
        #{} ->
            case maybe_producer_cancel(Cancel, Stage) of
                {noreply, Stage1} ->
                    MonRef = erlang:monitor(process, ConsumerPid),
                    NewMon = maps:put(MonRef, Ref, Stage1#stage.monitors),
                    NewCon = maps:put(Ref, {ConsumerPid, MonRef}, Stage1#stage.consumers),
                    producer_subscribe(Opts, From, Stage1#stage{monitors = NewMon, consumers = NewCon});
                Other ->
                    Other
            end
    end;
handle_info({'$gen_producer', {ConsumerPid, Ref} = From, {ask, Counter}}, #stage{consumers = Consumers} = Stage) when is_integer(Counter) ->
    case Consumers of
        #{Ref := _} ->
            dispatcher_callback(ask, [Counter, From, Stage#stage.dispatcher_state], Stage);
        #{} ->
            Msg = {'$gen_consumer', {self(), Ref}, {cancel, unknown_subscription}},
            send_noconnect(ConsumerPid, Msg),
            {noreply, Stage}
    end;
handle_info({'$gen_producer', {_, Ref}, {cancel, Reason}}, Stage) ->
    producer_cancel(Ref, cancel, Reason, Stage);
handle_info({'$gen_consumer', _, _} = Msg, #stage{type = producer} = Stage) ->
    ErrMsg = "stage producer ~tp received $gen_consumer message: ~tp~n",
    error_logger:error_msg(ErrMsg, [stage_util:self_name(), Msg]),
    {noreply, Stage};
handle_info({'$gen_consumer', {ProducerPid, Ref}, Events}, #stage{type = producer_consumer, events = {Queue, Counter}, producers = Producers} = Stage) when is_list(Events) ->
    case maps:is_key(Ref, Producers) of
        true ->
            NewQueue = put_pc_events(Events, Ref, Queue),
            take_pc_events(NewQueue, Counter, Stage);
        false ->
            Msg = {'$gen_producer', {self(), Ref}, {cancel, unknown_subscription}},
            send_noconnect(ProducerPid, Msg),
            {noreply, Stage}
    end;
handle_info({'$gen_consumer', {ProducerPid, Ref} = From, Events}, #stage{type = consumer, producers = Producers, mod = Mod, state = State} = Stage) when is_list(Events) ->
    case Producers of
        #{Ref := Entry} ->
            {Batches, NewStage} = consumer_receive(From, Entry, Events, Stage),
            consumer_dispatch(Batches, From, Mod, State, NewStage, false);
        _ ->
            Msg = {'$gen_producer', {self(), Ref}, {cancel, unknown_subscription}},
            send_noconnect(ProducerPid, Msg),
            {noreply, Stage}
    end;
handle_info({'$gen_consumer', {_, Ref}, {cancel, Reason}}, Stage) ->
    consumer_cancel(Ref, cancel, Reason, Stage);
handle_info(Msg, #stage{state = State} = Stage) ->
    noreply_callback(handle_info, [Msg, State], Stage).

terminate(Reason, #stage{mod = Mod, state = State}) ->
    case erlang:function_exported(Mod, terminate, 2) of
        true -> Mod:terminate(Reason, State);
        false -> ok
    end.

code_change(OldVsn, #stage{mod = Mod, state = State} = Stage, Extra) ->
    case erlang:function_exported(Mod, code_change, 3)  of
        true ->
            case Mod:code_change(OldVsn, State, Extra) of
                {ok, NewState} -> {ok, Stage#stage{state = NewState}};
                Other -> Other
            end;
        false ->
            {ok, Stage}
    end.

% format_status

%% Shared helpers
noreply_callback(handle_info, [Msg, State], #stage{mod = Mod} = Stage) ->
    case erlang:function_exported(Mod, handle_info, 2) of
        true ->
            handle_noreply_callback(Mod:handle_info(Msg, State), Stage);
        false ->
            Log = "undefined handle_info in ~tp~nunhandled message: ~tp~n",
            error_logger:warning_msg(Log, [Mod, Msg]),
            {noreply, Stage#stage{state = State}}
    end;
noreply_callback(handle_cancel, [Subscription, From, State], #stage{mod = Mod} = Stage) ->
    case erlang:function_exported(Mod, handle_cancel, 3) of
        true ->
            handle_noreply_callback(Mod:handle_cancel(Subscription, From, State), Stage);
        false ->
            {noreply, Stage#stage{state = State}}
    end;
noreply_callback(Callback, Args, #stage{mod = Mod} = Stage) ->
    handle_noreply_callback(apply(Mod, Callback, Args), Stage).

handle_noreply_callback(Return, Stage) ->
    case Return of
        {noreply, Events, State} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            {noreply, NewStage#stage{state = State}};
        {noreply, Events, State, hibernate} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            {noreply, NewStage#stage{state = State}, hibernate};
        {stop, Reason, State} ->
            {stop, Reason, Stage#stage{state = State}};
        Other ->
            {stop, {bad_return_value, Other}, Stage}
    end.


%% Producer helpers
producer_demand(#stage{events = forward} = Stage) ->
    {reply, forward, Stage};
producer_demand(#stage{events = Events} = Stage) when is_list(Events) ->
    {reply, accumulate, Stage}.

producer_demand(forward, #stage{type = producer_consumer} = Stage) ->
    {noreply, Stage};
producer_demand(_Mode, #stage{type = Type} = Stage) when type =/= producer ->
    ErrorMsg = "Demand mode can only be set for producers, gen_stage ~tp is a ~ts",
    error_logger:error_msg(ErrorMsg, [stage_util:self_name(), Type]),
    {noreply, Stage};
producer_demand(forward, #stage{events = Events} = Stage) ->
    NewStage = Stage#stage{events = forward},
    if
        is_list(Events) ->
            Fun =
            fun
                (D, {noreply, #stage{state = State} = StageAcc}) ->
                        noreply_callback(handle_demand, [D, State], StageAcc);
                    (D, {noreply, #stage{state = State} = StageAcc, _}) ->
                        noreply_callback(handle_demand, [D, State], StageAcc);
                    (_, {stop, _, _} = Acc) ->
                        Acc
            end,
            lists:foldl(Fun, {noreply, NewStage}, lists:reverse(Events));
        true ->
            {noreply, NewStage}
    end;
producer_demand(accumulate, #stage{events = Events} = Stage) ->
    if
        is_list(Events) -> {noreply, Stage};
        true -> {noreply, Stage#stage{events = []}}
    end.

producer_subscribe(Opts, From, Stage) ->
    #stage{mod = Mod, state = State, dispatcher_mod = DispatcherMod, dispatcher_state = DispatcherState}  = Stage,
    case maybe_subscribe(Mod, consumer, Opts, From, State) of
        {automatic, NewState} ->
            NewStage = Stage#stage{state = NewState},
            case DispatcherMod:subscribe(Opts, From, DispatcherState) of
                {ok, _, _} = Result -> handle_dispatcher_result(Result, NewStage);
                {error, Reason} -> producer_cancel(element(1, From), cancel, Reason, NewStage)
            end;
        {stop, Reason, State} ->
            {stop, Reason, Stage#stage{state = State}};
        Other ->
            {stop, {bad_return_value, Other}, Stage}
    end.

maybe_subscribe(Mod, Type, Opts, From, State) ->
    case erlang:function_exported(Mod, handle_subscribe, 4) of
        true ->
            Mod:handle_subscribe(Type, Opts, From, State);
        false ->
            {automatic, State}
    end.

maybe_producer_cancel({Ref, Reason}, Stage) ->
    producer_cancel(Ref, cancel, Reason, Stage);
maybe_producer_cancel(undefined, Stage) ->
    {noreply, Stage}.

producer_cancel(Ref, Kind, Reason, Stage) ->
    case maps:take(Ref, Stage#stage.consumers) of
        error ->
            {noreply, Stage};
        {{Pid, MonRef}, Consumers} ->
            erlang:demonitor(MonRef, [flush]),
            send_noconnect(Pid, {'$gen_consumer', {self(), Ref}, {cancel, Reason}}),
            Stage1 = Stage#stage{consumers = Consumers, monitors = maps:remove(MonRef, Stage#stage.monitors)},
            case noreply_callback(handle_cancel, [{Kind, Reason}, {Pid, Ref}, Stage1#stage.state], Stage1) of
                {noreply, #stage{dispatcher_state = DispatcherState} = Stage2} ->
                    dispatcher_callback(cancel, [{Pid, Ref}, DispatcherState], Stage2);
                {stop, _, _} = Stop ->
                    Stop
            end
    end.

dispatcher_callback(Callback, Args, #stage{dispatcher_mod = DispatcherMod} = Stage) ->
    Result = apply(DispatcherMod, Callback, Args),
    handle_dispatcher_result(Result, Stage).

handle_dispatcher_result({ok, Counter, DispatcherState}, Stage) ->
    case Stage of
        #stage{type = producer_consumer, events = {Queue, Demand}} ->
            NewCounter = Counter + Demand,
            Stage1 = Stage#stage{dispatcher_state = DispatcherState, events = {Queue, NewCounter}},
            {ok, _, Stage2} = take_from_buffer(NewCounter, Stage1),
            #stage{events = {Queue1, Counter2}} = Stage2,
            take_pc_events(Queue1, Counter2, Stage2);
        _ ->
            case take_from_buffer(Counter, Stage#stage{dispatcher_state = DispatcherState}) of
                {ok, 0, NewStage} ->
                    {noreply, NewStage};
                {ok, NewCounter, #stage{events = forward, state = State} = NewStage} ->
                    noreply_callback(handle_demand, [NewCounter, State], NewStage);
                {ok, NewCounter, #stage{events = Events} = NewStage} when is_list(Events) ->
                    {noreply, NewStage#stage{events = [NewCounter | Events]}}
            end
    end.

dispatch_events([], _Len, Stage) ->
    Stage;
dispatch_events(Events, _Len, #stage{type = consumer} = Stage) ->
    ErrMsg = "stage consumer ~tp cannot dispatch events (an empty list must be returned): ~tp~n",
    error_logger:error_msg(ErrMsg, [stage_util:self_name(), Events]),
    Stage;
dispatch_events(Events, _Len, #stage{consumers = Consumers} = Stage) when map_size(Consumers) =:= 0 ->
    buffer_events(Events, Stage);
dispatch_events(Events, Len, Stage) ->
    #stage{dispatcher_mod = DispatcherMod, dispatcher_state = DispatcherState} = Stage,
    {ok, Events1, DispatcherState1} = DispatcherMod:dispatch(Events, Len, DispatcherState),
    NewStage =
    case Stage of
        #stage{type = producer_consumer, events = {Queue, Demand}} ->
            NewDemand = Demand - (Len - length(Events1)),
            Stage#stage{dispatcher_state = DispatcherState1, events = {Queue, max(NewDemand, 0)}};
        _ ->
            Stage#stage{dispatcher_state = DispatcherState1}
    end,
    buffer_events(Events1, NewStage).

take_from_buffer(Counter, #stage{buffer = Buffer} = Stage) ->
    case stage_buffer:take_count_or_until_permanent(Buffer, Counter) of
        empty ->
            {ok, Counter, Stage};
        {ok, NewBuffer, NewCounter, Temps, Perms} ->
            Stage1 = dispatch_events(Temps, Counter - NewCounter, Stage#stage{buffer = NewBuffer}),
            Stage2 = lists:foldl(fun dispatch_info/2, Stage1, Perms),
            take_from_buffer(NewCounter, Stage2)
    end.

buffer_events([], Stage) ->
    Stage;
buffer_events(Events, #stage{buffer = Buffer, buffer_keep = Keep} = Stage) ->
    {NewBuffer, Excess, Perms} = stage_buffer:store_temporary(Buffer, Events, Keep),
    case Excess of
        0 -> ok;
        _ ->
            ErrMsg = "stage producer ~tp has discarded ~tp events from buffer",
            error_logger:warning_msg(ErrMsg, [stage_util:self_name(), Excess])
    end,
    lists:foldl(fun dispatch_info/2, Stage#stage{buffer = NewBuffer}, Perms).

%% Info helpers
producer_info(Msg, #stage{type = consumer} = Stage) ->
    erlang:send(self(), Msg),
    {reply, ok, Stage};
producer_info(Msg, #stage{type = producer_consumer, events = {Queue, Demand}} = Stage) ->
    NewStage =
    case queue:is_empty(Queue) of
        true ->
            buffer_or_dispatch_info(Msg, Stage);
        false ->
            Stage#stage{events = {queue:in({info, Msg}, Queue), Demand}}
    end,
    {reply, ok, NewStage};
producer_info(Msg, #stage{type = produer} = Stage) ->
    {reply, ok, buffer_or_dispatch_info(Msg, Stage)}.

buffer_or_dispatch_info(Msg, #stage{buffer = Buffer} = Stage) ->
    case stage_buffer:store_permanent_unless_empty(Buffer, Msg) of
        empty -> dispatch_info(Msg, Stage);
        {ok, NewBuffer} -> Stage#stage{buffer = NewBuffer}
    end.

dispatch_info(Msg, Stage) ->
    #stage{dispatcher_mod = DispatcherMod, dispatcher_state = DispatcherState} = Stage,
    {ok, NewState} = DispatcherMod:info(Msg, DispatcherState),
    Stage#stage{dispatcher_state = NewState}.

%% Consumer helpers
consumer_init_subscribe(Producers, Stage) ->
    Fun =
    fun
        (To, {ok, StageAcc}) ->
            case consumer_subscribe(To, StageAcc) of
                {reply, _, NewStage} -> {ok, NewStage};
                {stop, Reason, _, _} -> {stop, Reason};
                {stop, Reason, _} -> {stop, Reason}
            end;
        (_, {stop, Reason}) -> {stop, Reason}
    end,
    lists:foldl(Fun, {ok, Stage}, Producers).

consumer_receive({_, Ref} = From, {ProducerId, Cancel, {Demand, Min, Max}}, Events, Stage) ->
    {NewDemand, Batches} = stage_util:split_batches(Events, From, Min, Max, Demand),
    NewProducers = maps:put(Ref, {ProducerId, Cancel, {NewDemand, Min, Max}}, Stage#stage.producers),
    {Batches, Stage#stage{producers = NewProducers}};
consumer_receive(_, {_, _, manual}, Events, Stage) ->
    {[{Events, 0}], Stage}.

consumer_dispatch([{Batch, Ask} | Batches], From, Mod, State, Stage, _Hibernate) ->
    case Mod:handle_events(Batch, From, State) of
        {noreply, Events, NewState} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            ask(From, Ask, [noconnect]),
            consumer_dispatch(Batches, From, Mod, NewState, NewStage, false);
        {noreply, Events, NewState, hibernate} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            ask(From, Ask, [noconnect]),
            consumer_dispatch(Batches, From, Mod, NewState, NewStage, true);
        {stop, Reason, NewState} ->
            {stop, Reason, Stage#stage{state = NewState}};
        Other ->
            {stop, {bad_return_value, Other}, Stage#stage{state = State}}
    end;
consumer_dispatch([], _From, _Mod, State, Stage, false) ->
    {noreply, Stage#stage{state = State}};
consumer_dispatch([], _From, _Mod, State, Stage, true) ->
    {noreply, Stage#stage{state = State}, hibernate}.

consumer_subscribe({To, Opts}, Stage) when is_list(Opts) ->
    consumer_subscribe(undefined, To, Opts, Stage);
consumer_subscribe(To, Stage) ->
    consumer_subscribe(undefined, To, [], Stage).

consumer_subscribe(_Cancel, To, _Opts, #stage{type = producer} = Stage) ->
    ErrMsg = "stage producer ~tp cannot be subscribed to another stage: ~tp~n",
    error_logger:error_msg(ErrMsg, [stage_util:self_name(), To]),
    {reply, {error, not_a_consumer}, Stage};
consumer_subscribe(Current, To, Opts, Stage) ->
    case stage_util:validate_integer(Opts, max_demand, 1000, 1, infinity, false) of
        {ok, Max, _} ->
            case stage_util:validate_integer(Opts, min_demand, Max div 2, 0, Max - 1, false) of
                {ok, Min, _} ->
                    case stage_util:validate_in(Opts, cancel, permanent, [temporary, transient, permanent]) of
                        {ok, Cancel, _} ->
                            Producer = stage_util:whereis_server(To),
                            if
                                Producer =/= undefined ->
                                    Ref = monitor(process, Producer),
                                    Msg = {'$gen_producer', {self(), Ref}, {subscribe, Current, Opts}},
                                    send_noconnect(Producer, Msg),
                                    consumer_subscribe(Opts, Ref, Producer, Cancel, Min, Max, Stage);
                                Cancel =:= permanent orelse Cancel =:= transient ->
                                    {stop, noproc, {ok, make_ref()}, Stage};
                                Cancel =:= temporary ->
                                    {reply, {ok, make_ref()}, Stage}
                            end;
                        {error, Msg} ->
                            ErrMsg = "stage consumer ~tp subscribe received invalid option: ~ts~n",
                            error_logger:error_msg(ErrMsg, [stage_util:self_name(), Msg]),
                            {reply, {error, {bad_opts, Msg}}, Stage}
                    end;
                {error, Msg} ->
                    ErrMsg = "stage consumer ~tp subscribe received invalid option: ~ts~n",
                    error_logger:error_msg(ErrMsg, [stage_util:self_name(), Msg]),
                    {reply, {error, {bad_opts, Msg}}, Stage}
            end;
        {error, Msg} ->
            ErrMsg = "stage consumer ~tp subscribe received invalid option: ~ts~n",
            error_logger:error_msg(ErrMsg, [stage_util:self_name(), Msg]),
            {reply, {error, {bad_opts, Msg}}, Stage}
    end.

consumer_subscribe(Opts, Ref, Producer, Cancel, Min, Max, Stage) ->
    #stage{mod = Mod, state = State} = Stage,
    To = {Producer, Ref},
    case maybe_subscribe(Mod, producer, Opts, To, State) of
        {automatic, NewState} ->
            ask(To, Max, [noconnect]),
            Producers = maps:put(Ref, {Producer, Cancel, {Max, Min, Max}}, Stage#stage.producers),
            {reply, {ok, Ref}, Stage#stage{producers = Producers, state = NewState}};
        {manual, NewState} ->
            Producers = maps:put(Ref, {Producer, Cancel, manual}, Stage#stage.producers),
            {reply, {ok, Ref}, Stage#stage{producers = Producers, state = NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, Stage#stage{state = NewState}};
        Other ->
            {stop, {bad_return_value, Other}, Stage}
    end.

consumer_cancel(Ref, Kind, Reason, #stage{producers = Producers} = Stage) ->
    case maps:take(Ref, Producers) of
        error ->
            {noreply, Stage};
        {{Producer, Mode, _}, NewProducers} ->
            erlang:demonitor(Ref, [flush]),
            NewStage = Stage#stage{producers = NewProducers},
            schedule_cancel(Mode, {Kind, Reason}, {Producer, Ref}, NewStage)
    end.

schedule_cancel(Mode, KindReason, ProducerRef, #stage{type = producer_consumer, events = {Queue, Demand}} = Stage) ->
    case queue:is_empty(Queue) of
        true ->
            invoke_cancel(Mode, KindReason, ProducerRef, Stage);
        false ->
            NewQueue = queue:in({cancel, Mode, KindReason, ProducerRef}, Queue),
            {noreply, Stage#stage{events = {NewQueue, Demand}}}
    end;
schedule_cancel(Mode, KindReason, ProducerRef, Stage) ->
    invoke_cancel(Mode, KindReason, ProducerRef, Stage).

invoke_cancel(Mode, {_, Reason} = KindReason, {Pid, _} = ProducerRef, #stage{state = State} = Stage) ->
    case noreply_callback(handle_cancel, [KindReason, ProducerRef, State], Stage) of
        {noreply, NewStage} ->
            case Mode =:= permanent orelse (Mode =:= transient andalso (not stage_util:is_transient_shutdown(Reason))) of
                true ->
                    case Reason of
                        already_subscribed ->
                            {noreply, NewStage};
                        _ ->
                            ErrMsg = "stage consumer ~tp is stopping after receiving cancel from producer ~tp with reason: ~tp~n",
                            error_logger:info_msg(ErrMsg, [stage_util:self_name(), Pid, Reason]),
                            {stop, Reason, Stage}
                    end;
                false ->
                    {noreply, NewStage}
            end;
        Other ->
            Other
    end.


%% Producer consumer helpers
put_pc_events(Events, Ref, Queue) ->
    queue:in({Events, Ref}, Queue).

send_pc_events(Events, Ref, #stage{mod = Mod, state = State, producers = Producers} = Stage) ->
    case Producers of
        #{Ref := Entry} ->
            {ProducerId, _, _} = Entry,
            From = {ProducerId, Ref},
            {Batches, NewStage} = consumer_receive(From, Entry, Events, Stage),
            consumer_dispatch(Batches, From, Mod, State, NewStage, false);
        #{} ->
            consumer_dispatch([{Events, 0}], {pid, Ref}, Mod, State, Stage, false)
    end.


take_pc_events(Queue, Counter, Stage) when Counter > 0 ->
    case queue:out(Queue) of
        {{value, {info, Msg}}, Queue1} ->
            take_pc_events(Queue1, Counter, buffer_or_dispatch_info(Msg, Stage));
        {{value, {cancel, Mode, KindReason, Ref}}, Queue1} ->
            case invoke_cancel(Mode, KindReason, Ref, Stage) of
                {noreply, Stage1} ->
                    take_pc_events(Queue1, Counter, Stage1);
                {noreply, Stage1, hibernate} ->
                    take_pc_events(Queue1, Counter, Stage1);
                {stop, _, _} = Stop ->
                    Stop
            end;
        {{value, {Events, Ref}}, Queue1} ->
            case send_pc_events(Events, Ref, Stage#stage{events = {Queue1, Counter}}) of
                {noreply, #stage{events = {Queue2, Counter1}} = Stage1} ->
                    take_pc_events(Queue2, Counter1, Stage1);
                {noreply, #stage{events = {Queue2, Counter1}} = Stage1, hibernate} ->
                    take_pc_events(Queue2, Counter1, Stage1);
                {stop, _, _} = Stop ->
                    Stop
            end;
        {empty, Queue1} ->
            {noreply, Stage#stage{events = {Queue1, Counter}}}
    end;
take_pc_events(Queue, Counter, Stage) ->
    {noreply, Stage#stage{events = {Queue, Counter}}}.
