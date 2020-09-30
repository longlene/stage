-module(stage_partition_dispatcher).

-behavior(stage_dispatcher).

-export([
         init/1,
         info/2,
         subscribe/3,
         cancel/2,
         ask/3,
         dispatch/3
        ]).

-define(INIT, {undefined, undefined, 0}).

init(Opts) ->
    Partitions =
    case proplists:get_value(partitions, Opts) of
        undefined ->
            throw("partitions option is required when using partition dispatcher");
        Partition when is_integer(Partition) ->
            lists:seq(0, Partition - 1);
        Partitions0 ->
            Partitions0
    end,
    HashPresent = stage_keyword:has_key(Opts, hash),
    PartitionMap =
    lists:foldl(
      fun(Partition, Acc) ->
              if
                  not HashPresent andalso not is_integer(Partition) ->
                      throw({badarg, partitions});
                  true ->
                      ok
              end,
              erlang:put(Partition, []),
              maps:put(Partition, ?INIT, Acc)
      end, #{}, Partitions),
    Size = map_size(PartitionMap),
    Hash = stage_keyword:get(Opts, hash, fun(Key) -> hash(Key, Size) end),
    {ok, {make_ref(), Hash, 0, 0, PartitionMap, #{}, #{}}}.

hash(Event, Range) ->
    {Event, erlang:phash2(Event, Range)}.

info(Msg, {Tag, Hash, Waiting, Pending, Partitions, References, Infos}) ->
    Info = make_ref(),
    {Partitions1, Queued} =
    lists:foldl(
      fun
          ({Partition, {Pid, Ref, Queue}}, {PartitionsAcc, QueuedAcc}) when not is_integer(Queue) ->
              {maps:put(Partition, {Pid, Ref, queue:in({Tag, Info}, Queue)}, PartitionsAcc), [Partition | QueuedAcc]};
         (_, {PartitionsAcc, QueuedAcc}) ->
              {PartitionsAcc, QueuedAcc}
      end, {Partitions, []}, Partitions),
    Infos1 =
    case Queued of
        [] ->
            erlang:send(self(), Msg),
            Infos;
        _ ->
            maps:put(Info, {Msg, Queued}, Infos)
    end,
    {ok, {Tag, Hash, Waiting, Pending, Partitions1, References, Infos1}}.

subscribe(Opts, {Pid, Ref}, {Tag, Hash, Waiting, Pending, Partitions, References, Infos}) ->
    Partition = proplists:get_value(partition, Opts),
    case Partitions of
        #{Partition := {undefined, undefined, DemandOrQueue}} ->
            Partitions1 = maps:put(Partition, {Pid, Ref, DemandOrQueue}, Partitions),
            References1 = maps:put(Ref, Partition, References),
            {ok, 0, {Tag, Hash, Waiting, Pending, Partitions1, References1, Infos}};
        #{Partition := {Pid, _, _}} ->
            throw({badarg, partition_dup});
        _ when Partition =:= undefined ->
            throw({badarg, partition_not_found})
    end.

cancel({_, Ref}, {Tag, Hash, Waiting, Pending, Partitions, References, Infos}) ->
    {Partition, References1} =
    case maps:take(Ref, References) of
        {_, _} = Tuple -> Tuple;
        error -> {undefined, References}
    end,
    {_Pid, _Ref, DemandOrQueue} = maps:get(Partition, Partitions),
    Partitions1 = maps:put(Partition, ?INIT, Partitions),
    case DemandOrQueue of
        Demand when is_integer(Demand) ->
            {ok, 0, {Tag, Hash, Waiting, Pending + Demand, Partitions1, References1, Infos}};
        Queue ->
            {Length, Infos1} = clear_queue(Queue, Tag, Partition, 0, Infos),
            {ok, Length, {Tag, Hash, Waiting + Length, Pending, Partitions1, References1, Infos1}}
    end.

ask(Counter, {_, Ref}, {Tag, Hash, Waiting, Pending, Partitions, References, Infos}) ->
    Partition = maps:get(Ref, References),
    {Pid, Ref1, DemandOrQueue} = maps:get(Partition, Partitions),
    {DemandOrQueue1, Infos1} =
    case DemandOrQueue of
        Demand when is_integer(Demand) ->
            {Demand + Counter, Infos};
        Queue ->
            send_from_queue(Queue, Tag, Pid, Ref1, Partition, Counter, [], Infos)
    end,
    Partitions1 = maps:put(Partition, {Pid, Ref1, DemandOrQueue1}, Partitions),
    AlreadySent = min(Pending, Counter),
    Demand1 = Counter - AlreadySent,
    Pending1 = Pending - AlreadySent,
    {ok, Demand1, {Tag, Hash, Waiting + Demand1, Pending1, Partitions1, References, Infos1}}.

send_from_queue(Queue, _Tag, Pid, Ref, _Partition, 0, Acc, Infos) ->
    maybe_send(Acc, Pid, Ref),
    {Queue, Infos};
send_from_queue(Queue, Tag, Pid, Ref, Partition, Counter, Acc, Infos) ->
    case queue:out(Queue) of
        {{value, {Tag, Info}}, Queue1} ->
            maybe_send(Acc, Pid, Ref),
            Infos1 = maybe_info(Infos, Info, Partition),
            send_from_queue(Queue1, Tag, Pid, Ref, Partition, Counter, [], Infos1);
        {{value, Event}, Queue1} ->
            send_from_queue(Queue1, Tag, Pid, Ref, Partition, Counter - 1, [Event | Acc], Infos);
        {empty, _Queue1} ->
            maybe_send(Acc, Pid, Ref),
            {Counter, Infos}
    end.

clear_queue(Queue, Tag, Partition, Counter, Infos) ->
    case queue:out(Queue) of
        {{value, {Tag, Info}}, Queue1} ->
            clear_queue(Queue1, Tag, Partition, Counter, maybe_info(Infos, Info, Partition));
        {{value, _}, Queue1} ->
            clear_queue(Queue1, Tag, Partition, Counter + 1, Infos);
        {empty, _Queue} ->
            {Counter, Infos}
    end.

maybe_send([], _Pid, _Ref) ->
    ok;
maybe_send(Events, Pid, Ref) ->
    erlang:send(Pid, {'$gen_consumer', {self(), Ref}, lists:reverse(Events)}, [noconnect]).

maybe_info(Infos, Info, Partition) ->
    case Infos of
        #{Info := {Msg, [Partition]}} ->
            erlang:send(self(), Msg),
            maps:remove(Info, Infos);
        #{Info := {Msg, Partitions}} ->
            maps:put(Info, {Msg, lists:delete(Partition, Partitions)}, Infos)
    end.

dispatch(Events, _Length, {Tag, Hash, Waiting, Pending, Partitions, References, Infos}) ->
    {DeliverLater, Waiting1} = split_events(Events, Waiting, Hash, Partitions),
    Partitions1 = maps:from_list(dispatch_per_partition(maps:to_list(Partitions))),
    {ok, DeliverLater, {Tag, Hash, Waiting1, Pending, Partitions1, References, Infos}}.

split_events(Events, 0, _Hash, _Partitions) ->
    {Events, 0};
split_events([], Counter, _Hash, _Partitions) ->
    {[], Counter};
split_events([Event | Events], Counter, Hash, Partitions) ->
    case Hash(Event) of
        {Event1, Partition} ->
            case erlang:get(Partition) of
                undefined ->
                    throw(iolist_to_binary(io_lib:format("unknown partition ~p for event ~p, ignore", [Partition, Event1])));
                Current ->
                    erlang:put(Partition, [Event1 | Current]),
                    split_events(Events, Counter - 1, Hash, Partitions)
            end;
        undefined ->
            split_events(Events, Counter, Hash, Partitions);
        Other ->
            throw(iolist_to_binary(io_lib:format("hash function should return {Event, Partition}, got ~p", [Other])))
    end.

dispatch_per_partition([{Partition, {Pid, Ref, DemandOrQueue} = Value} | Rest]) ->
    case erlang:put(Partition, []) of
        [] ->
            [{Partition, Value} | dispatch_per_partition(Rest)];
        Events ->
            Events1 = lists:reverse(Events),
            {Events2, DemandOrQueue1} =
            case DemandOrQueue of
                Demand when is_integer(Demand) ->
                    split_into_queue(Events1, Demand, []);
                Queue ->
                    {[], put_into_queue(Events1, Queue)}
            end,
            maybe_send(Events2, Pid, Ref),
            [{Partition, {Pid, Ref, DemandOrQueue1}} | dispatch_per_partition(Rest)]
    end;
dispatch_per_partition([]) ->
    [].



split_into_queue(Events, 0, Acc) ->
    {Acc, put_into_queue(Events, queue:new())};
split_into_queue([], Counter, Acc) ->
    {Acc, Counter};
split_into_queue([Event | Events], Counter, Acc) ->
    split_into_queue(Events, Counter - 1, [Event | Acc]).

put_into_queue(Events, Queue) ->
    lists:foldl(
      fun(Event, Acc) ->
              queue:in(Event, Acc)
      end, Queue, Events).

