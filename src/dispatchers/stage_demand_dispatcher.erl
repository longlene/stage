-module(stage_demand_dispatcher).

-behavior(stage_dispatcher).

-export([
         init/1,
         info/2,
         subscribe/3,
         cancel/2,
         ask/3,
         dispatch/3
        ]).

init(_Opts) ->
    {ok, {[], 0, undefined}}.

info(Msg, State) ->
    erlang:send(self(), Msg),
    {ok, State}.

subscribe(_Opts, {Pid, Ref}, {Demands, Pending, Max}) ->
    {ok, 0, {Demands ++ [{0, Pid, Ref}], Pending, Max}}.

cancel({_, Ref}, {Demands, Pending, Max}) ->
    {Current, NewDemands} = pop_demand(Ref, Demands),
    {ok, 0, {NewDemands, Current + Pending, Max}}.

ask(Counter, {Pid, Ref}, {Demands, Pending, Max}) ->
    RealMax =
    case Max of
        undefined -> Counter;
        _ -> Max
    end,
    if
        Counter > RealMax ->
            Warning = "gen_stage producer demand_dispatcher expects a maximum demand of ~tp. Using different maximum demands will overload greedy consumers. Got demand for ~tp events from ~tp~n",
            error_logger:warning_msg(Warning, [Max, Counter, Pid]);
        true ->
            ok
    end,
    {Current, Demands1} = pop_demand(Ref, Demands),
    Demands2 = add_demand(Current + Counter, Pid, Ref, Demands1),
    AlreadySent = min(Pending, Counter),
    {ok, Counter - AlreadySent, {Demands2, Pending - AlreadySent, Max}}.

dispatch(Events, Length, {Demands, Pending, Max}) ->
    {NewEvents, NewDemands} = dispatch_demand(Events, Length, Demands),
    {ok, NewEvents, {NewDemands, Pending, Max}}.

dispatch_demand([], _Length, Demands) ->
    {[], Demands};
dispatch_demand(Events, _Length, [{0, _, _} | _] = Demands) ->
    {Events, Demands};
dispatch_demand(Events, Length, [{Counter, Pid, Ref} | Demands]) ->
    {DeliverNow, DeliverLater, NewLength, NewCounter} = split_events(Events, Length, Counter),
    erlang:send(Pid, {'$gen_consumer', {self(), Ref}, DeliverNow}, [noconnect]),
    NewDemands = add_demand(NewCounter, Pid, Ref, Demands),
    dispatch_demand(DeliverLater, NewLength, NewDemands).

split_events(Events, Length, Counter) when Length =< Counter ->
    {Events, [], 0, Counter - Length};
split_events(Events, Length, Counter) ->
    {Now, Later} = lists:split(Counter, Events),
    {Now, Later, Length - Counter, 0}.

add_demand(Counter, Pid, Ref, [{C, _, _} | _] = Demands) when Counter > C ->
    [{Counter, Pid, Ref} | Demands];
add_demand(Counter, Pid, Ref, [Demand | Demands]) ->
    [Demand | add_demand(Counter, Pid, Ref, Demands)];
add_demand(Counter, Pid, Ref, []) when is_integer(Counter) ->
    [{Counter, Pid, Ref}].

pop_demand(Ref, Demands) ->
    case lists:keytake(Ref, 3, Demands) of
        {value, {Current, _Pid, Ref}, Rest} ->
            {Current, Rest};
        false ->
            {undefined, Demands}
    end.

