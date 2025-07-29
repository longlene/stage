-module(gen_stage_demand_dispatcher).
%% @doc
%% A dispatcher that sends batches to the highest demand.
%%
%% This is the default dispatcher used by gen_stage. In order
%% to avoid greedy consumers, it is recommended that all consumers
%% have exactly the same maximum demand.
%%
%% Options:
%%   - shuffle_demands_on_first_dispatch: when true, shuffle the initial demands list
%%     which is constructed on subscription before first dispatch. It prevents overloading
%%     the first consumer on first dispatch. Defaults to false.
%%   - max_demand: the maximum demand expected on gen_stage:ask/3.
%%     Defaults to the first demand asked.
%%
%% Examples
%% 
%% To start a producer with demands shuffled on first dispatch:
%%
%% {producer, State, [{dispatcher, {gen_stage_demand_dispatcher, 
%%                                 [{shuffle_demands_on_first_dispatch, true}]}}]}
%% @end

-behavior(gen_stage_dispatcher).

-export([
         init/1,
         info/2,
         subscribe/3,
         cancel/2,
         ask/3,
         dispatch/3
        ]).

%% State: {Demands, Pending, MaxDemand, ShuffleDemand}
%% where:
%%   - Demands: list of {Demand, Pid, Ref}
%%   - Pending: integer() - pending events count
%%   - MaxDemand: integer() | undefined - maximum demand configured
%%   - ShuffleDemand: boolean() - whether to shuffle on first dispatch

init(Opts) ->
    ShuffleDemand = proplists:get_value(shuffle_demands_on_first_dispatch, Opts, false),
    MaxDemand = proplists:get_value(max_demand, Opts, undefined),
    {ok, {[], 0, MaxDemand, ShuffleDemand}}.

info(Msg, State) ->
    erlang:send(self(), Msg),
    {ok, State}.

subscribe(_Opts, {Pid, Ref}, {Demands, Pending, Max, ShuffleDemand}) ->
    {ok, 0, {Demands ++ [{0, Pid, Ref}], Pending, Max, ShuffleDemand}}.

cancel({_, Ref}, {Demands, Pending, Max, ShuffleDemand}) ->
    {Current, NewDemands} = pop_demand(Ref, Demands),
    {ok, 0, {NewDemands, Current + Pending, Max, ShuffleDemand}}.

ask(Counter, {Pid, Ref}, {Demands, Pending, Max, ShuffleDemand}) ->
    %% Set max demand to first demand if not configured
    RealMax = case Max of
        undefined -> Counter;
        _ -> Max
    end,
    
    %% Warn if counter exceeds max demand  
    case Counter > RealMax of
        true ->
            error_logger:warning_msg(
                "gen_stage producer demand_dispatcher expects a maximum demand of ~p. "
                "Using different maximum demands will overload greedy consumers. "
                "Got demand for ~p events from ~p~n",
                [RealMax, Counter, Pid]);
        false ->
            ok
    end,
    
    {Current, Demands1} = pop_demand(Ref, Demands),
    Demands2 = add_demand(Current + Counter, Pid, Ref, Demands1),
    AlreadySent = min(Pending, Counter),
    {ok, Counter - AlreadySent, {Demands2, Pending - AlreadySent, RealMax, ShuffleDemand}}.

%% Handle first dispatch with shuffle
dispatch(Events, Length, {Demands, Pending, Max, true}) ->
    ShuffledDemands = shuffle_list(Demands),
    dispatch(Events, Length, {ShuffledDemands, Pending, Max, false});
dispatch(Events, Length, {Demands, Pending, Max, false}) ->
    {NewEvents, ToBuffer, NewDemands} = dispatch_demand(Events, Length, Demands),
    {ok, NewEvents, {NewDemands, max(Pending - ToBuffer, 0), Max, false}}.

dispatch_demand([], Length, Demands) ->
    {[], Length, Demands};
dispatch_demand(Events, Length, []) ->
    {Events, Length, []};
dispatch_demand(Events, Length, [{0, _, _} | _] = Demands) ->
    {Events, Length, Demands};
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
            {0, Demands}
    end.

%% @private
%% Simple shuffle implementation using Erlang random
shuffle_list([]) ->
    [];
shuffle_list(List) ->
    %% Tag each element with a random number, sort, and extract elements
    Tagged = [{rand:uniform(), Item} || Item <- List],
    Sorted = lists:sort(Tagged),
    [Item || {_Rand, Item} <- Sorted].

