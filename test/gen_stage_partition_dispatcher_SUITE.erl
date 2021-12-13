-module(gen_stage_partition_dispatcher_SUITE).

-include("test_helper.hrl").

-export([all/0]).
-export([
         subscribes_asks_and_cancels/1,
         subscribes_asks_and_dispatches/1
        ]).

all() ->
    [
     subscribes_asks_and_cancels,
     subscribes_asks_and_dispatches
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

