-module(stage_broadcast_dispatcher).

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
    {ok, {[], 0, maps:new()}}.

info(Msg, State) ->
    erlang:send(self(), Msg),
    {ok, State}.

subscribe(Opts, {Pid, Ref}, {Demands, Waiting, SubscribedProcesses}) ->
    Selector = validate_selector(Opts),
    case subscribed(SubscribedProcesses, Pid) of
        true ->
            logger:error("~p is already registered with ~p, ignore", [Pid, self()]),
            {error, already_subscribed};
        false ->
            NewSubscribedProcesses = add_subscriber(SubscribedProcesses, Pid),
            {ok, 0, {add_demand(-Waiting, Pid, Ref, Selector, Demands), Waiting, NewSubscribedProcesses}}
    end.

cancel({Pid, Ref}, {Demands, Waiting, SubscribedProcesses}) ->
    Demands1 = delete_demand(Ref, Demands),
    NewMin = get_min(Demands1),
    Demands2 = adjust_demand(NewMin, Demands1),
    NewSubscribedProcesses = delete_subscriber(SubscribedProcesses, Pid),
    {ok, NewMin, {Demands2, Waiting + NewMin, NewSubscribedProcesses}}.


ask(Counter, {Pid, Ref}, {Demands, Waiting, SubscribedProcesses}) ->
    {Current, Selector, Demands1} = pop_demand(Ref, Demands),
    Demands2 = add_demand(Current + Counter, Pid, Ref, Selector, Demands1),
    NewMin = get_min(Demands2),
    Demands3 = adjust_demand(NewMin, Demands2),
    {ok, NewMin, {Demands3, Waiting + NewMin, SubscribedProcesses}}.

dispatch(Events, _Length, {Demands, 0, SubscribedProcesses}) ->
    {ok, Events, {Demands, 0, SubscribedProcesses}};
dispatch(Events, Length, {Demands, Waiting, SubscribedProcesses}) ->
    {DeliverNow, DeliverLater, NewWaiting} = split_events(Events, Length, Waiting),
    [begin
         Selected =
         case filter_and_count(DeliverNow, Selector) of
             {Selector0, 0} ->
                 Selector0;
             {Selector0, Discarded} ->
                 erlang:send(self(), {'$gen_producer', {Pid, Ref}, {ask, Discarded}}),
                 Selector0
         end,
         erlang:send(Pid, {'$gen_consumer', {self(), Ref}, Selected}, [noconnect]),
         ok
     end || {_, Pid, Ref, Selector} <- Demands],

    {ok, DeliverLater, {Demands, NewWaiting, SubscribedProcesses}}.

filter_and_count(Messages, undefined) ->
    {Messages, 0};
filter_and_count(Messages, Selector) ->
    filter_and_count(Messages, Selector, [], 0).

filter_and_count([Message | Messages], Selector, Acc, Count) ->
    case Selector(Message) of
        true ->
            filter_and_count(Messages, Selector, [Message | Acc], Count);
        false ->
            filter_and_count(Messages, Selector, Acc, Count + 1)
    end;
filter_and_count([], _Selector, Acc, Count) ->
    {lists:reverse(Acc), Count}.

validate_selector(Opts) ->
    case proplists:get_value(selector, Opts) of
        undefined -> undefined;
        Selector when is_function(Selector, 1) ->
            Selector;
        _Other ->
            throw("selector option must be passed a unary function")
    end.

get_min([]) ->
    0;
get_min([{Acc, _, _, _} | Demands]) ->
    Min = 
    lists:foldl(
      fun({Val, _, _, _}, Acc0) ->
              erlang:min(Val, Acc0)
      end, Acc, Demands),
    erlang:max(Min, 0).

split_events(Events, Length, Counter) when Length =< Counter ->
    {Events, [], Counter - Length};
split_events(Events, _Length, Counter) ->
    {Now, Later} = lists:split(Counter, Events),
    {Now, Later, 0}.

adjust_demand(0, Demands) ->
    Demands;
adjust_demand(Min, Demands) ->
    lists:map(
      fun({Counter, Pid, Key, Selector}) ->
              {Counter - Min, Pid, Key, Selector}
      end, Demands).

add_demand(Counter, Pid, Ref, Selector, Demands) when is_integer(Counter) andalso is_pid(Pid) andalso (Selector =:= undefined orelse is_function(Selector, 1)) ->
    [{Counter, Pid, Ref, Selector} | Demands].

pop_demand(Ref, Demands) ->
    case lists:keytake(Ref, 3, Demands) of
        {value, {Current, _Pid, Ref, Selector}, Rest} -> {Current, Selector, Rest};
        false -> {0, undefined, Demands}
    end.

delete_demand(Ref, Demands) ->
    lists:keydelete(Ref, 3, Demands).

add_subscriber(SubscribedProcesses, Pid) ->
    maps:put(Pid, [], SubscribedProcesses).

delete_subscriber(SubscribedProcesses, Pid) ->
    maps:remove(Pid, SubscribedProcesses).

subscribed(SubscribedProcesses, Pid) ->
    erlang:is_map_key(Pid, SubscribedProcesses).

