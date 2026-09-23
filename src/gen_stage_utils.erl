%% @doc
%% Utility functions for gen_stage validation and helper operations.
%% @end
-module(gen_stage_utils).

-export([
    validate_list/3,
    validate_in/4,
    validate_integer/6,
    validate_no_opts/1,
    is_transient_shutdown/1,
    self_name/0,
    split_batches/5
]).

%% @doc
%% Validates the argument is a list.
%% @end
-spec validate_list(proplists:proplist(), atom(), any()) -> 
    {ok, list(), proplists:proplist()} | {error, string()}.
validate_list(Opts, Key, Default) ->
    Value = proplists:get_value(Key, Opts, Default),
    NewOpts = proplists:delete(Key, Opts),
    case is_list(Value) of
        true ->
            {ok, Value, NewOpts};
        false ->
            {error, "expected " ++ atom_to_list(Key) ++ " to be a list, got: " ++ 
             lists:flatten(io_lib:format("~p", [Value]))}
    end.

%% @doc
%% Validates the given option is one of the values.
%% @end
-spec validate_in(proplists:proplist(), atom(), any(), [any()]) -> 
    {ok, any(), proplists:proplist()} | {error, string()}.
validate_in(Opts, Key, Default, Values) ->
    Value = proplists:get_value(Key, Opts, Default),
    NewOpts = proplists:delete(Key, Opts),
    case lists:member(Value, Values) of
        true ->
            {ok, Value, NewOpts};
        false ->
            {error, "expected " ++ atom_to_list(Key) ++ " to be one of " ++ 
             lists:flatten(io_lib:format("~p", [Values])) ++ ", got: " ++ 
             lists:flatten(io_lib:format("~p", [Value]))}
    end.

%% @doc
%% Validates an integer.
%% @end
-spec validate_integer(proplists:proplist(), atom(), any(), integer() | infinity, integer() | infinity, boolean()) -> 
    {ok, integer() | infinity, proplists:proplist()} | {error, string()}.
validate_integer(Opts, Key, Default, Min, Max, AllowInfinity) ->
    Value = proplists:get_value(Key, Opts, Default),
    NewOpts = proplists:delete(Key, Opts),
    KeyStr = atom_to_list(Key),
    ValueStr = lists:flatten(io_lib:format("~p", [Value])),
    
    if
        Value =:= infinity andalso AllowInfinity ->
            {ok, Value, NewOpts};
        not is_integer(Value) ->
            {error, "expected " ++ KeyStr ++ " to be an integer, got: " ++ ValueStr};
        Value < Min ->
            {error, "expected " ++ KeyStr ++ " to be equal to or greater than " ++ 
             integer_to_list(Min) ++ ", got: " ++ ValueStr};
        Value > Max ->
            {error, "expected " ++ KeyStr ++ " to be equal to or less than " ++ 
             integer_to_list(Max) ++ ", got: " ++ ValueStr};
        true ->
            {ok, Value, NewOpts}
    end.

%% @doc
%% Validates there are no options left.
%% @end
-spec validate_no_opts(proplists:proplist()) -> ok | {error, string()}.
validate_no_opts([]) ->
    ok;
validate_no_opts(Opts) ->
    {error, "unknown options " ++ lists:flatten(io_lib:format("~p", [Opts]))}.

%% @doc
%% Helper to check if a shutdown is transient.
%% @end
-spec is_transient_shutdown(any()) -> boolean().
is_transient_shutdown(normal) -> true;
is_transient_shutdown(shutdown) -> true;
is_transient_shutdown({shutdown, _}) -> true;
is_transient_shutdown(_) -> false.

%% @doc
%% Returns the name of the current process or self.
%% @end
-spec self_name() -> atom() | pid().
self_name() ->
    case erlang:process_info(self(), registered_name) of
        {registered_name, Name} when is_atom(Name) -> Name;
        _ -> self()
    end.

%% @doc
%% Splits a list of events into messages configured by min, max, and demand.
%%
%% The From argument is only used for the excess-events log message; it is
%% the subscription identifier (a plain pid in regular consumers, a nested
%% {MonitorRef, InnerRef} tuple in gen_stage_stream subscriptions).
%% @end
-spec split_batches([any()], pid() | {pid(), reference()} | {pid(), {reference(), reference()}},
                     non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
    {non_neg_integer(), [{[any()], non_neg_integer()}]}.
split_batches(Events, From, Min, Max, Demand) ->
    split_batches(Events, From, Min, Max, Demand, Demand, []).

%% @private
split_batches([], _From, _Min, _Max, _OldDemand, NewDemand, Batches) ->
    {NewDemand, lists:reverse(Batches)};
split_batches(Events, From, Min, Max, OldDemand, NewDemand, Batches) ->
    {Events1, Batch, BatchSize} = split_events(Events, Max - Min, 0, []),
    
    %% Adjust the batch size to whatever is left of the demand in case of excess.
    {OldDemand1, BatchSize1} = 
        case OldDemand - BatchSize of
            Diff when Diff < 0 ->
                error_logger:error_msg(
                    "GenStage consumer ~p has received ~p events in excess from: ~p~n",
                    [self_name(), abs(Diff), From]
                ),
                {0, OldDemand};
            _Diff ->
                {OldDemand - BatchSize, BatchSize}
        end,
    
    %% In case we've reached min, we will ask for more events.
    {NewDemand1, BatchSize2} =
        case NewDemand - BatchSize1 of
            Diff1 when Diff1 =< Min ->
                {Max, Max - Diff1};
            Diff1 ->
                {Diff1, 0}
        end,
    
    split_batches(Events1, From, Min, Max, OldDemand1, NewDemand1, 
                  [{Batch, BatchSize2} | Batches]).

%% @private
split_events(Events, Limit, Limit, Acc) -> 
    {Events, lists:reverse(Acc), Limit};
split_events([], _Limit, Counter, Acc) -> 
    {[], lists:reverse(Acc), Counter};
split_events([Event | Events], Limit, Counter, Acc) ->
    split_events(Events, Limit, Counter + 1, [Event | Acc]).