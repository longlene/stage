-module(stage_util).

-export(
   [
    validate_list/3,
    validate_in/4,
    validate_integer/6,
    validate_no_opts/1,
    is_transient_shutdown/1,
    self_name/0,
    split_batches/5,
    whereis_server/1
   ]).

validate_list(Opts, Key, Default) ->
    {Value, NewOpts} = stage_keyword:pop(Opts, Key, Default),

    if
        is_list(Value) -> {ok, Value, NewOpts};
        true -> {error, io_lib:format("expected ~p to be a list, got: ~p", [Key, Value])}
    end.

validate_in(Opts, Key, Default, Values) ->
    {Value, NewOpts} = stage_keyword:pop(Opts, Key, Default),
    case lists:member(Value, Values) of
        true -> {ok, Value, NewOpts};
        false -> {error, io_lib:format("expected ~p to be one of ~p, got: ~p", [Key, Values, Value])}
    end.

validate_integer(Opts, Key, Default, Min, Max, Infinity) ->
    {Value, NewOpts} = stage_keyword:pop(Opts, Key, Default),
    if
        Value =:= infinity andalso Infinity ->
            {ok, Value, Opts};
        not is_integer(Value) ->
            ErrorMsg = "expected ~p to be an integer, got: ~p",
            {error, io_lib:format(ErrorMsg, [Key, Value])};
        Value < Min ->
            ErrorMsg = "expected ~p to be equal to or greater than ~p, got: ~p",
            {error, io_lib:format(ErrorMsg, [Key, Min, Value])};
        Value > Max ->
            ErrorMsg = "expected ~p to be equal to or less than ~p, got: ~p",
            {error, io_lib:format(ErrorMsg, [Key, Max, Value])};
        true ->
            {ok, Value, NewOpts}
    end.

validate_no_opts([]) ->
    ok;
validate_no_opts(Opts) ->
    {error, {badarg, Opts}}.

is_transient_shutdown(normal) -> true;
is_transient_shutdown(shutdown) -> true;
is_transient_shutdown({shutdown, _}) -> true;
is_transient_shutdown(_) -> false.

self_name() ->
    case erlang:process_info(self(), registered_name) of
        {registered_name, Name} when is_atom(Name) -> Name;
        _ -> self()
    end.

split_batches(Events, From, Min, Max, Demand) ->
    split_batches(Events, From, Min, Max, Demand, Demand, []).

split_batches([], _From, _Min, _Max, _OldDemand, NewDemand, Batches) ->
    {NewDemand, lists:reverse(Batches)};
split_batches(Events, From, Min, Max, OldDemand, NewDemand, Batches) ->
    {NewEvents, Batch, BatchSize} = split_events(Events, Max - Min, 0, []),
    {OldDemand1, BatchSize1} =
    case OldDemand - BatchSize of
        Diff when Diff < 0 ->
            ErrorMsg = "gen_stage consumer ~tp has received ~tp events in excess from: ~tp~n",
            error_logger:error_msg(ErrorMsg, [gen_stage_utils:self_name(), abs(Diff), From]),
            {0, OldDemand};
        Diff ->
            {Diff, BatchSize}
    end,

    % In case we've reached min, we will ask for more events
    {NewDemand1, BatchSize2} =
    case NewDemand - BatchSize1 of
        Diff1 when Diff1 =< Min ->
            {Max, Max - Diff1};
        Diff1 ->
            {Diff1, 0}
    end,
    split_batches(NewEvents, From, Min, Max, OldDemand1, NewDemand1, [{Batch,  BatchSize2} | Batches]).

split_events(Events, Limit, Limit, Acc) ->
    {Events, lists:reverse(Acc), Limit};
split_events([], _Limit, Counter, Acc) ->
    {[], lists:reverse(Acc), Counter};
split_events([Event | Events], Limit, Counter, Acc) ->
    split_events(Events, Limit, Counter + 1, [Event | Acc]).


whereis_server(Pid) when is_pid(Pid) ->
    Pid;
whereis_server(Name) when is_atom(Name) ->
    erlang:whereis(Name);
whereis_server({global, Name}) ->
    global:whereis_name(Name);
whereis_server({via, Mod, Name}) ->
    apply(Mod, whereis_name, [Name]);
whereis_server({Name, Local}) when is_atom(Name) andalso Local =:= node() ->
    erlang:whereis(Name);
whereis_server({Name, Node} = Server) when is_atom(Name) andalso is_atom(Node) ->
    Server.
