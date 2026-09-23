-module(gen_stage_broadcast_dispatcher).

%% @doc
%% A dispatcher that accumulates demand from all consumers
%% before broadcasting events to all of them.
%%
%% This dispatcher guarantees that events are dispatched to all
%% consumers without exceeding the demand of any given consumer.
%%
%% The `selector' option
%%
%% Subscribers can specify an optional `selector' function in their
%% subscription options. The function receives an event and returns a
%% boolean; when it returns false the event is not delivered to that
%% subscriber (the dispatcher asks the producer for the discarded
%% events so that other subscribers can receive them).
%%
%% For example, to only receive events whose `key' field starts with
%% `foo-':
%%
%%     gen_stage:sync_subscribe(Consumer, [{to, Producer},
%%                                        {selector, fun(Event) ->
%%                                             case Event of
%%                                                 #{key := Key} ->
%%                                                     string:prefix(Key, "foo-") =/= nomatch;
%%                                                 _ -> false
%%                                             end
%%                                         end}])
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

%% State: {Demands, Waiting, Requested, SubscribedProcesses}
%% where:
%%   - Demands: list of {Demand, Pid, Ref, Selector}
%%   - Waiting: total demand that has been sent upstream but not yet dispatched
%%   - Requested: total demand that has been sent upstream (may exceed Waiting
%%     because dispatches reduce Waiting without reducing Requested directly)
%%   - SubscribedProcesses: map of pids already subscribed (per process)

init(_Opts) ->
    {ok, {[], 0, 0, #{}}}.

info(Msg, State) ->
    erlang:send(self(), Msg),
    {ok, State}.

subscribe(Opts, {Pid, Ref}, {Demands, Waiting, Requested, SubscribedProcesses}) ->
    Selector = validate_selector(Opts),
    case subscribed(SubscribedProcesses, Pid) of
        true ->
            logger:error("~p is already registered with ~p. This subscription has been discarded.",
                        [Pid, self()]),
            {error, already_subscribed};
        false ->
            NewSubscribedProcesses = add_subscriber(SubscribedProcesses, Pid),
            NewDemands = adjust_demand(-Waiting, Demands),
            {ok, 0, {add_demand(0, Pid, Ref, Selector, NewDemands), 0, Requested, NewSubscribedProcesses}}
    end.

cancel({Pid, Ref}, {Demands, Waiting, Requested, SubscribedProcesses}) ->
    NewSubscribedProcesses = delete_subscriber(SubscribedProcesses, Pid),
    case delete_demand(Ref, Demands) of
        [] ->
            {ok, 0, {[], 0, Requested, NewSubscribedProcesses}};
        NewDemands ->
            %% Since we may have removed the process we were waiting on,
            %% cancellation may actually generate demand!
            {Demands1, UpstreamDemand, Waiting1, Requested1} =
                sync_demands(NewDemands, Waiting, Requested),
            {ok, UpstreamDemand, {Demands1, Waiting1, Requested1, NewSubscribedProcesses}}
    end.

ask(Counter, {Pid, Ref}, {Demands, Waiting, Requested, SubscribedProcesses}) ->
    {Current, Selector, Demands1} = pop_demand(Ref, Demands),
    Demands2 = add_demand(Current + Counter, Pid, Ref, Selector, Demands1),
    {Demands3, UpstreamDemand, Waiting1, Requested1} =
        sync_demands(Demands2, Waiting, Requested),
    {ok, UpstreamDemand, {Demands3, Waiting1, Requested1, SubscribedProcesses}}.

dispatch(Events, _Length, {Demands, 0, Requested, SubscribedProcesses}) ->
    {ok, Events, {Demands, 0, Requested, SubscribedProcesses}};
dispatch(Events, Length, {Demands, Waiting, Requested, SubscribedProcesses}) ->
    {DeliverNow, DeliverLater, NewWaiting, DeliverNowCount} =
        split_events(Events, Length, Waiting),
    [begin
         Selected =
         case filter_and_count(DeliverNow, Selector) of
             {Selected0, 0} ->
                 Selected0;
             {Selected0, Discarded} ->
                 erlang:send(self(), {'$gen_producer', {Pid, Ref}, {ask, Discarded}}),
                 Selected0
         end,
         erlang:send(Pid, {'$gen_consumer', {self(), Ref}, Selected}, [noconnect]),
         ok
     end || {_, Pid, Ref, Selector} <- Demands],
    {ok, DeliverLater, {Demands, NewWaiting, Requested - DeliverNowCount, SubscribedProcesses}}.

%% @private
sync_demands(Demands, Waiting, Requested) ->
    NewMin = get_min(Demands),
    Demands1 = adjust_demand(NewMin, Demands),
    Waiting1 = Waiting + NewMin,
    Request = max(0, Waiting1 - Requested),
    Requested1 = Requested + Request,
    {Demands1, Request, Waiting1, Requested1}.

%% @private
filter_and_count(Messages, undefined) ->
    {Messages, 0};
filter_and_count(Messages, Selector) ->
    filter_and_count(Messages, Selector, [], 0).

%% @private
filter_and_count([Message | Messages], Selector, Acc, Count) ->
    case Selector(Message) of
        true ->
            filter_and_count(Messages, Selector, [Message | Acc], Count);
        false ->
            filter_and_count(Messages, Selector, Acc, Count + 1)
    end;
filter_and_count([], _Selector, Acc, Count) ->
    {lists:reverse(Acc), Count}.

%% @private
validate_selector(Opts) ->
    case proplists:get_value(selector, Opts) of
        undefined -> undefined;
        Selector when is_function(Selector, 1) ->
            Selector;
        _Other ->
            throw("selector option must be passed a unary function")
    end.

%% @private
get_min([]) ->
    0;
get_min([{Acc, _, _, _} | Demands]) ->
    Min =
    lists:foldl(
      fun({Val, _, _, _}, Acc0) ->
              erlang:min(Val, Acc0)
      end, Acc, Demands),
    erlang:max(Min, 0).

%% @private
split_events(Events, Length, Counter) when Length =< Counter ->
    {Events, [], Counter - Length, Length};
split_events(Events, _Length, Counter) ->
    {Now, Later} = lists:split(Counter, Events),
    {Now, Later, 0, Counter}.

%% @private
adjust_demand(0, Demands) ->
    Demands;
adjust_demand(Min, Demands) ->
    lists:map(
      fun({Counter, Pid, Key, Selector}) ->
              {Counter - Min, Pid, Key, Selector}
      end, Demands).

%% @private
add_demand(Counter, Pid, Ref, Selector, Demands)
  when is_integer(Counter) andalso is_pid(Pid) andalso
       (Selector =:= undefined orelse is_function(Selector, 1)) ->
    [{Counter, Pid, Ref, Selector} | Demands].

%% @private
pop_demand(Ref, Demands) ->
    case lists:keytake(Ref, 3, Demands) of
        {value, {Current, _Pid, Ref, Selector}, Rest} -> {Current, Selector, Rest};
        false -> {0, undefined, Demands}
    end.

%% @private
delete_demand(Ref, Demands) ->
    lists:keydelete(Ref, 3, Demands).

%% @private
add_subscriber(SubscribedProcesses, Pid) ->
    maps:put(Pid, [], SubscribedProcesses).

%% @private
delete_subscriber(SubscribedProcesses, Pid) ->
    maps:remove(Pid, SubscribedProcesses).

%% @private
subscribed(SubscribedProcesses, Pid) ->
    erlang:is_map_key(Pid, SubscribedProcesses).
