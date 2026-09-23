-module(gen_stage_broadcast_dispatcher_SUITE).

-include("test_helper.hrl").

-export([all/0]).
-export([
         subscribes_and_cancels/1,
         subscribes_asks_and_cancels/1,
         multiple_subscriptions_with_early_demand/1,
         multiple_subscriptions_with_late_demand/1,
         subscribes_asks_and_dispatches_to_multiple_consumers/1,
         subscribes_asks_dispatches_and_repeats/1,
         subscribes_asks_cancels_and_reuses_events/1,
         cancels_blocking_subscriber_with_requested_demand/1,
         subscribes_with_a_selector/1,
         delivers_info_to_current_process/1,
         subscribing_is_idempotent/1
        ]).

all() ->
    [
     subscribes_and_cancels,
     subscribes_asks_and_cancels,
     multiple_subscriptions_with_early_demand,
     multiple_subscriptions_with_late_demand,
     subscribes_asks_and_dispatches_to_multiple_consumers,
     subscribes_asks_dispatches_and_repeats,
     subscribes_asks_cancels_and_reuses_events,
     cancels_blocking_subscriber_with_requested_demand,
     subscribes_with_a_selector,
     delivers_info_to_current_process,
     subscribing_is_idempotent
    ].

%% @private
dispatcher(Opts) ->
    {ok, {[], 0, 0, _Subscribers} = State} = gen_stage_broadcast_dispatcher:init(Opts),
    State.

subscribes_and_cancels(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    Disp = dispatcher([]),
    ExpectedSubscribers = #{Pid => []},

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([], {Pid, Ref}, Disp),
    ?assertEqual(Disp1, {[{0, Pid, Ref, undefined}], 0, 0, ExpectedSubscribers}),

    {ok, 0, Disp2} = gen_stage_broadcast_dispatcher:cancel({Pid, Ref}, Disp1),
    ?assertEqual(Disp2, {[], 0, 0, #{}}).

subscribes_asks_and_cancels(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    Disp = dispatcher([]),
    ExpectedSubscribers = #{Pid => []},

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([], {Pid, Ref}, Disp),
    ?assertEqual(Disp1, {[{0, Pid, Ref, undefined}], 0, 0, ExpectedSubscribers}),

    {ok, 10, Disp2} = gen_stage_broadcast_dispatcher:ask(10, {Pid, Ref}, Disp1),
    ?assertEqual(Disp2, {[{0, Pid, Ref, undefined}], 10, 10, ExpectedSubscribers}),

    {ok, 0, Disp3} = gen_stage_broadcast_dispatcher:cancel({Pid, Ref}, Disp2),
    ?assertEqual(Disp3, {[], 0, 10, #{}}),

    %% Now attempt to dispatch with no consumers
    {ok, [1, 2, 3], Disp4} = gen_stage_broadcast_dispatcher:dispatch([1, 2, 3], 3, Disp3),
    ?assertEqual(Disp4, {[], 0, 10, #{}}).

multiple_subscriptions_with_early_demand(_Config) ->
    Pid1 = self(),
    Pid2 = spawn(fun() -> ok end),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),

    ExpectedSubscribers = #{Pid1 => []},

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
    ?assertEqual(Disp1, {[{0, Pid1, Ref1, undefined}], 0, 0, ExpectedSubscribers}),

    {ok, 10, Disp2} = gen_stage_broadcast_dispatcher:ask(10, {Pid1, Ref1}, Disp1),
    ?assertEqual(Disp2, {[{0, Pid1, Ref1, undefined}], 10, 10, ExpectedSubscribers}),

    ExpectedSubscribers1 = ExpectedSubscribers#{Pid2 => []},

    {ok, 0, Disp3} = gen_stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp2),
    ?assertEqual(Disp3, {[{0, Pid2, Ref2, undefined}, {10, Pid1, Ref1, undefined}], 0, 10, ExpectedSubscribers1}),

    ExpectedSubscribers2 = maps:remove(Pid1, ExpectedSubscribers1),

    {ok, 0, Disp4} = gen_stage_broadcast_dispatcher:cancel({Pid1, Ref1}, Disp3),
    ?assertEqual(Disp4, {[{0, Pid2, Ref2, undefined}], 0, 10, ExpectedSubscribers2}),

    {ok, 0, Disp5} = gen_stage_broadcast_dispatcher:ask(10, {Pid2, Ref2}, Disp4),
    ?assertEqual(Disp5, {[{0, Pid2, Ref2, undefined}], 10, 10, ExpectedSubscribers2}).

multiple_subscriptions_with_late_demand(_Config) ->
    Pid1 = self(),
    Pid2 = spawn(fun() -> ok end),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),

    ExpectedSubscribers = #{Pid1 => []},

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
    ?assertEqual(Disp1, {[{0, Pid1, Ref1, undefined}], 0, 0, ExpectedSubscribers}),

    ExpectedSubscribers1 = ExpectedSubscribers#{Pid2 => []},

    {ok, 0, Disp2} = gen_stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp1),
    ?assertEqual(Disp2, {[{0, Pid2, Ref2, undefined}, {0, Pid1, Ref1, undefined}], 0, 0, ExpectedSubscribers1}),

    {ok, 0, Disp3} = gen_stage_broadcast_dispatcher:ask(10, {Pid1, Ref1}, Disp2),
    ?assertEqual(Disp3, {[{10, Pid1, Ref1, undefined}, {0, Pid2, Ref2, undefined}], 0, 0, ExpectedSubscribers1}),

    ExpectedSubscribers2 = maps:remove(Pid2, ExpectedSubscribers1),

    {ok, 10, Disp4} = gen_stage_broadcast_dispatcher:cancel({Pid2, Ref2}, Disp3),
    ?assertEqual(Disp4, {[{0, Pid1, Ref1, undefined}], 10, 10, ExpectedSubscribers2}),

    {ok, 10, Disp5} = gen_stage_broadcast_dispatcher:ask(10, {Pid1, Ref1}, Disp4),
    ?assertEqual(Disp5, {[{0, Pid1, Ref1, undefined}], 20, 20, ExpectedSubscribers2}).

subscribes_asks_and_dispatches_to_multiple_consumers(_Config) ->
    {Pid1, Pid2, Pid3} = {spawn_forwarder(), spawn_forwarder(), spawn_forwarder()},
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Ref3 = make_ref(),
    Disp = dispatcher([]),

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
    {ok, 0, Disp2} = gen_stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp1),

    {ok, 0, Disp3} = gen_stage_broadcast_dispatcher:ask(3, {Pid1, Ref1}, Disp2),
    {ok, 2, Disp4} = gen_stage_broadcast_dispatcher:ask(2, {Pid2, Ref2}, Disp3),

    ExpectedSubscribers = #{Pid1 => [], Pid2 => []},

    ?assertEqual(Disp4, {[{0, Pid2, Ref2, undefined}, {1, Pid1, Ref1, undefined}], 2, 2, ExpectedSubscribers}),

    %% One batch fits all
    {ok, [], Disp5} = gen_stage_broadcast_dispatcher:dispatch([a, b], 2, Disp4),
    ?assertEqual(Disp5, {[{0, Pid2, Ref2, undefined}, {1, Pid1, Ref1, undefined}], 0, 0, ExpectedSubscribers}),
    ?assertReceive({'$gen_consumer', {_, Ref1}, [a, b]}),
    ?assertReceive({'$gen_consumer', {_, Ref2}, [a, b]}),

    %% A batch with left-over
    {ok, 1, Disp6} = gen_stage_broadcast_dispatcher:ask(2, {Pid2, Ref2}, Disp5),

    {ok, [d], Disp7} = gen_stage_broadcast_dispatcher:dispatch([c, d], 2, Disp6),
    ?assertEqual(Disp7, {[{1, Pid2, Ref2, undefined}, {0, Pid1, Ref1, undefined}], 0, 0, ExpectedSubscribers}),
    ?assertReceive({'$gen_consumer', {_, Ref1}, [c]}),
    ?assertReceive({'$gen_consumer', {_, Ref2}, [c]}),

    %% A batch with no demand
    {ok, [d], Disp8} = gen_stage_broadcast_dispatcher:dispatch([d], 1, Disp7),
    ?assertEqual(Disp8, {[{1, Pid2, Ref2, undefined}, {0, Pid1, Ref1, undefined}], 0, 0, ExpectedSubscribers}),
    ?refuteReceive({'$gen_consumer', {_, _}, _}),

    %% Add a late subscriber
    {ok, 1, Disp9} = gen_stage_broadcast_dispatcher:ask(1, {Pid1, Ref1}, Disp8),
    {ok, 0, Disp10} = gen_stage_broadcast_dispatcher:subscribe([], {Pid3, Ref3}, Disp9),
    {ok, [d, e], Disp11} = gen_stage_broadcast_dispatcher:dispatch([d, e], 2, Disp10),

    ExpectedSubscribers1 = maps:put(Pid3, [], ExpectedSubscribers),

    ?assertEqual(Disp11,
                 {[{0, Pid3, Ref3, undefined}, {1, Pid1, Ref1, undefined}, {1, Pid2, Ref2, undefined}],
                  0, 1, ExpectedSubscribers1}),

    %% Even out
    {ok, 0, Disp12} = gen_stage_broadcast_dispatcher:ask(2, {Pid1, Ref1}, Disp11),
    {ok, 0, Disp13} = gen_stage_broadcast_dispatcher:ask(2, {Pid2, Ref2}, Disp12),
    {ok, 2, Disp14} = gen_stage_broadcast_dispatcher:ask(3, {Pid3, Ref3}, Disp13),
    {ok, [], Disp15} = gen_stage_broadcast_dispatcher:dispatch([d, e, f], 3, Disp14),

    ?assertEqual(Disp15,
                 {[{0, Pid3, Ref3, undefined}, {0, Pid2, Ref2, undefined}, {0, Pid1, Ref1, undefined}],
                  0, 0, ExpectedSubscribers1}),
    ?assertReceive({'$gen_consumer', {_, Ref1}, [d, e, f]}),
    ?assertReceive({'$gen_consumer', {_, Ref2}, [d, e, f]}),
    ?assertReceive({'$gen_consumer', {_, Ref3}, [d, e, f]}).

subscribes_asks_dispatches_and_repeats(_Config) ->
    {Pid1, Pid2} = {spawn_forwarder(), spawn_forwarder()},
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
    ExpectedSubscribers = #{Pid1 => []},
    ?assertEqual(Disp1, {[{0, Pid1, Ref1, undefined}], 0, 0, ExpectedSubscribers}),

    {ok, 10, Disp2} = gen_stage_broadcast_dispatcher:ask(10, {Pid1, Ref1}, Disp1),
    ?assertEqual(Disp2, {[{0, Pid1, Ref1, undefined}], 10, 10, ExpectedSubscribers}),

    {ok, [], Disp3} = gen_stage_broadcast_dispatcher:dispatch([a, b, c], 3, Disp2),
    ?assertEqual(Disp3, {[{0, Pid1, Ref1, undefined}], 7, 7, ExpectedSubscribers}),
    ?assertReceive({'$gen_consumer', {_, Ref1}, [a, b, c]}),

    {ok, 0, Disp4} = gen_stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp3),
    ExpectedSubscribers1 = maps:put(Pid2, [], ExpectedSubscribers),
    ?assertEqual(Disp4, {[{0, Pid2, Ref2, undefined}, {7, Pid1, Ref1, undefined}], 0, 7, ExpectedSubscribers1}),

    {ok, 0, Disp5} = gen_stage_broadcast_dispatcher:ask(20, {Pid2, Ref2}, Disp4),
    ?assertEqual(Disp5, {[{13, Pid2, Ref2, undefined}, {0, Pid1, Ref1, undefined}], 7, 7, ExpectedSubscribers1}),

    {ok, [], Disp6} = gen_stage_broadcast_dispatcher:dispatch([d, e, f, g, h, i, j], 7, Disp5),
    ?assertEqual(Disp6, {[{13, Pid2, Ref2, undefined}, {0, Pid1, Ref1, undefined}], 0, 0, ExpectedSubscribers1}),
    ?assertReceive({'$gen_consumer', {_, Ref1}, [d, e, f, g, h, i, j]}),
    ?assertReceive({'$gen_consumer', {_, Ref2}, [d, e, f, g, h, i, j]}).

subscribes_asks_cancels_and_reuses_events(_Config) ->
    {Pid1, Pid2} = {spawn_forwarder(), spawn_forwarder()},
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
    ExpectedSubscribers = #{Pid1 => []},
    ?assertEqual(Disp1, {[{0, Pid1, Ref1, undefined}], 0, 0, ExpectedSubscribers}),

    {ok, 10, Disp2} = gen_stage_broadcast_dispatcher:ask(10, {Pid1, Ref1}, Disp1),
    ?assertEqual(Disp2, {[{0, Pid1, Ref1, undefined}], 10, 10, ExpectedSubscribers}),

    {ok, 0, Disp3} = gen_stage_broadcast_dispatcher:cancel({Pid1, Ref1}, Disp2),
    ExpectedSubscribers1 = maps:remove(Pid1, ExpectedSubscribers),
    ?assertEqual(Disp3, {[], 0, 10, ExpectedSubscribers1}),

    {ok, 0, Disp4} = gen_stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp3),
    ExpectedSubscribers2 = maps:put(Pid2, [], ExpectedSubscribers1),
    ?assertEqual(Disp4, {[{0, Pid2, Ref2, undefined}], 0, 10, ExpectedSubscribers2}),

    {ok, 0, Disp5} = gen_stage_broadcast_dispatcher:ask(5, {Pid2, Ref2}, Disp4),
    ?assertEqual(Disp5, {[{0, Pid2, Ref2, undefined}], 5, 10, ExpectedSubscribers2}),

    {ok, [], Disp6} = gen_stage_broadcast_dispatcher:dispatch([a, b, c], 3, Disp5),
    ?assertEqual(Disp6, {[{0, Pid2, Ref2, undefined}], 2, 7, ExpectedSubscribers2}),
    ?assertReceive({'$gen_consumer', {_, Ref2}, [a, b, c]}).

cancels_blocking_subscriber_with_requested_demand(_Config) ->
    {Pid1, Pid2} = {spawn_forwarder(), spawn_forwarder()},
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
    ExpectedSubscribers = #{Pid1 => []},
    ?assertEqual(Disp1, {[{0, Pid1, Ref1, undefined}], 0, 0, ExpectedSubscribers}),

    {ok, 10, Disp2} = gen_stage_broadcast_dispatcher:ask(10, {Pid1, Ref1}, Disp1),
    ?assertEqual(Disp2, {[{0, Pid1, Ref1, undefined}], 10, 10, ExpectedSubscribers}),

    {ok, 0, Disp3} = gen_stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp2),
    ExpectedSubscribers1 = maps:put(Pid2, [], ExpectedSubscribers),
    ?assertEqual(Disp3, {[{0, Pid2, Ref2, undefined}, {10, Pid1, Ref1, undefined}], 0, 10, ExpectedSubscribers1}),

    {ok, 0, Disp4} = gen_stage_broadcast_dispatcher:ask(5, {Pid1, Ref1}, Disp3),
    ?assertEqual(Disp4, {[{15, Pid1, Ref1, undefined}, {0, Pid2, Ref2, undefined}], 0, 10, ExpectedSubscribers1}),

    {ok, 5, Disp5} = gen_stage_broadcast_dispatcher:cancel({Pid2, Ref2}, Disp4),
    ExpectedSubscribers2 = maps:remove(Pid2, ExpectedSubscribers1),
    ?assertEqual(Disp5, {[{0, Pid1, Ref1, undefined}], 15, 15, ExpectedSubscribers2}).

subscribes_with_a_selector(_Config) ->
    {Pid1, Pid2} = {spawn_forwarder(), spawn_forwarder()},
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),
    Selector1 = fun(Event) ->
                        case Event of
                            %% OTP 27+ string:prefix/2 returns the remainder or
                            %% nomatch, not true/false
                            #{key := Key} -> string:prefix(Key, "pre") =/= nomatch;
                            _ -> false
                        end
                end,
    Selector2 = fun(Event) ->
                        case Event of
                            #{key := Key} -> string:prefix(Key, "pref") =/= nomatch;
                            _ -> false
                        end
                end,

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([{selector, Selector1}], {Pid1, Ref1}, Disp),
    {ok, 0, Disp2} = gen_stage_broadcast_dispatcher:subscribe([{selector, Selector2}], {Pid2, Ref2}, Disp1),
    %% (no ?assertMatch here: the macro rebinds pattern variables inside a fun)
    {[{0, P2, R2, _Sel2}, {0, P1, R1, _Sel1}], 0, 0, Subs2} = Disp2,
    ?assertEqual(Pid2, P2),
    ?assertEqual(Ref2, R2),
    ?assertEqual(Pid1, P1),
    ?assertEqual(Ref1, R1),
    ?assertEqual(2, map_size(Subs2)),

    {ok, 0, Disp3} = gen_stage_broadcast_dispatcher:ask(4, {Pid2, Ref2}, Disp2),
    {ok, 4, Disp4} = gen_stage_broadcast_dispatcher:ask(4, {Pid1, Ref1}, Disp3),

    Events = [#{key => "pref-1234"}, #{key => "pref-5678"}, #{key => "pre0000"}, #{key => "foo0000"}],
    {ok, [], _Disp5} = gen_stage_broadcast_dispatcher:dispatch(Events, 4, Disp4),

    ?assertReceive({'$gen_producer', {_, Ref1}, {ask, 1}}),
    ?assertReceive({'$gen_producer', {_, Ref2}, {ask, 2}}),
    ?assertReceive({'$gen_consumer', {_, Ref1},
                    [#{key := "pref-1234"}, #{key := "pref-5678"}, #{key := "pre0000"}]}),
    ?assertReceive({'$gen_consumer', {_, Ref2}, [#{key := "pref-1234"}, #{key := "pref-5678"}]}).

delivers_info_to_current_process(_Config) ->
    {Pid1, Pid2} = {spawn_forwarder(), spawn_forwarder()},
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
    {ok, 0, Disp2} = gen_stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp1),
    {ok, 0, Disp3} = gen_stage_broadcast_dispatcher:ask(3, {Pid1, Ref1}, Disp2),

    {ok, NotifyDisp} = gen_stage_broadcast_dispatcher:info(hello, Disp3),
    ?assertEqual(Disp3, NotifyDisp),
    ?assertReceive(hello).

subscribing_is_idempotent(_Config) ->
    Pid = self(),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),
    ExpectedSubscribers = #{Pid => []},

    {ok, 0, Disp1} = gen_stage_broadcast_dispatcher:subscribe([], {Pid, Ref1}, Disp),

    {error, already_subscribed} = gen_stage_broadcast_dispatcher:subscribe([], {Pid, Ref2}, Disp1),
    ?assertEqual(Disp1, {[{0, Pid, Ref1, undefined}], 0, 0, ExpectedSubscribers}).

%% @private
spawn_forwarder() ->
    Parent = self(),
    spawn_link(fun() -> forwarder_loop(Parent) end).

%% @private
forwarder_loop(Parent) ->
    receive
        Msg ->
            erlang:send(Parent, Msg),
            forwarder_loop(Parent)
    end.
