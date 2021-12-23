-module(gen_stage_partition_dispatcher_SUITE).

-include("test_helper.hrl").

-export([all/0]).
-export([
         subscribes_asks_and_cancels/1,
         subscribes_asks_and_dispatches/1,
         subscribes_asks_and_dispatches_to_custom_partitions/1,
         subscribes_asks_and_dispatches_to_partitions_or_none/1
        ]).

all() ->
    [
     subscribes_asks_and_cancels,
     subscribes_asks_and_dispatches,
     subscribes_asks_and_dispatches_to_custom_partitions,
     subscribes_asks_and_dispatches_to_partitions_or_none
    ].

dispatcher(Opts) ->
    {ok, State} = gen_stage_partition_dispatcher:init(Opts),
    State.

waiting_and_pending({_, _, Waiting, Pending, _, _, _}) ->
    {Waiting, Pending}.

subscribes_asks_and_cancels(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    Disp = dispatcher([{partitions, 2}]),

    {ok, 0, Disp1} = gen_stage_partition_dispatcher:subscribe([{partition, 0}], {Pid, Ref}, Disp),

    {ok, 10, Disp2} = gen_stage_partition_dispatcher:ask(10, {Pid, Ref}, Disp1),
    ?assertEqual({10, 0}, waiting_and_pending(Disp2)),
    {ok, 0, Disp3} = gen_stage_partition_dispatcher:cancel({Pid, Ref}, Disp2),
    ?assertEqual({10, 10}, waiting_and_pending(Disp3)),

    {ok, 0, Disp4} = gen_stage_partition_dispatcher:subscribe([{partition, 1}], {Pid, Ref}, Disp3),

    {ok, 0, Disp5} = gen_stage_partition_dispatcher:ask(5, {Pid, Ref}, Disp4),
    ?assertEqual({10, 5}, waiting_and_pending(Disp5)),
    {ok, 0, Disp6} = gen_stage_partition_dispatcher:cancel({Pid, Ref}, Disp5),
    ?assertEqual({10, 10}, waiting_and_pending(Disp6)).


subscribes_asks_and_dispatches(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    Disp = dispatcher([{partitions, 1}]),
    {ok, 0, Disp1} = gen_stage_partition_dispatcher:subscribe([{partition, 0}], {Pid, Ref}, Disp),

    {ok, 3, Disp2} = gen_stage_partition_dispatcher:ask(3, {Pid, Ref}, Disp1),
    {ok, [], Disp3} = gen_stage_partition_dispatcher:dispatch([1], 1, Disp2),
    ?assert({2, 0} =:= waiting_and_pending(Disp3)),
    ?assertReceived({'$gen_consumer', {_, Ref}, [1]}),

    {ok, 3, Disp4} = gen_stage_partition_dispatcher:ask(3, {Pid, Ref}, Disp3),
    ?assert({5, 0} =:= waiting_and_pending(Disp4)),

    {ok, [9, 11], Disp5} = gen_stage_partition_dispatcher:dispatch([2, 5, 6, 7, 8, 9, 11], 7, Disp4),
    ?assert({0, 0} =:= waiting_and_pending(Disp5)),
    ?assertReceived({'$gen_consumer', {_, Ref}, [2, 5, 6, 7, 8]}).

subscribes_asks_and_dispatches_to_custom_partitions(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    Hash =
        fun(Event) ->
                {Event, case Event rem 2 of 0 -> even; _ -> odd end}
        end,
    Disp = dispatcher([{partitions, [odd, even]}, {hash, Hash}]),
    {ok, 0, Disp1} = gen_stage_partition_dispatcher:subscribe([{partition, odd}], {Pid, Ref}, Disp),
    {ok, 3, Disp2} = gen_stage_partition_dispatcher:ask(3, {Pid, Ref}, Disp1),
    {ok, [], Disp3} = gen_stage_partition_dispatcher:dispatch([1], 1, Disp2),
    ?assert({2, 0} =:= waiting_and_pending(Disp3)),
    ?assertReceived({'$gen_consumer', {_, Ref}, [1]}),

    {ok, 3, Disp4} = gen_stage_partition_dispatcher:ask(3, {Pid, Ref}, Disp3),
    ?assert({5, 0} =:= waiting_and_pending(Disp4)),

    {ok, [15, 17], Disp5} = gen_stage_partition_dispatcher:dispatch([5, 7, 9, 11, 13, 15, 17], 7, Disp4),
    ?assert({0, 0} =:= waiting_and_pending(Disp5)),
    ?assertReceived({'$gen_consumer', {_, Ref}, [5, 7, 9, 11, 13]}).

subscribes_asks_and_dispatches_to_partitions_or_none(_Config) ->
    Pid = self(),
    EvenRef = make_ref(),
    OddRef = make_ref(),
    HashFun =
        fun
            (Event) when Event rem 3 =:= 0 -> none;
            (Event) when Event rem 2 =:= 0 -> {Event, even};
            (Event) -> {Event, odd}
        end,
    Disp = dispatcher([{partitions, [odd, even]}, {hash, HashFun}]),
    {ok, 0, Disp1} = gen_stage_partition_dispatcher:subscribe([{partition, even}], {Pid, EvenRef}, Disp),
    {ok, 0, Disp2} = gen_stage_partition_dispatcher:subscribe([{partition, odd}], {Pid, OddRef}, Disp1),

    {ok, 4, Disp3} = gen_stage_partition_dispatcher:ask(4, {Pid, EvenRef}, Disp2),
    {ok, 4, Disp4} = gen_stage_partition_dispatcher:ask(4, {Pid, OddRef}, Disp3),
    {ok, [12], Disp5} = gen_stage_partition_dispatcher:dispatch([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 12, Disp4),

    ?assertReceived({'$gen_consumer', {_, EvenRef}, [2, 4, 8, 10]}),
    ?assertReceived({'$gen_consumer', {_, OddRef}, [1, 5, 7, 11]}),
    ?assert({0, 0} =:= waiting_and_pending(Disp5)).


