-module(stage_broadcast_dispatcher_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(assertReceive(Guard), receive Guard -> true end).

-export([all/0]).
-export([
         subscribes_and_cancels/1,
         multiple_subscriptions_with_early_demand/1,
         multiple_subscriptions_with_late_demand/1,
         delivers_info_to_current_process/1
        ]).

all() ->
    [
     subscribes_and_cancels,
     multiple_subscriptions_with_early_demand,
     multiple_subscriptions_with_late_demand,
     delivers_info_to_current_process
    ].

dispatcher(Opts) ->
    {ok, {[], 0, _Subscribers} = State} = stage_broadcast_dispatcher:init(Opts),
    State.

subscribes_and_cancels(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    Disp = dispatcher([]),
    ExpectedSubscribers = #{Pid => []},

    {ok, 0, Disp1} = stage_broadcast_dispatcher:subscribe([], {Pid, Ref}, Disp),
    ?assertEqual(Disp1, {[{0, Pid, Ref, undefined}], 0, ExpectedSubscribers}),

    {ok, 0, Disp2} = stage_broadcast_dispatcher:cancel({Pid, Ref}, Disp1),
    ?assertEqual(Disp2, {[], 0, #{}}).

multiple_subscriptions_with_early_demand(_Config) ->
    Pid1 = self(),
    Pid2 = spawn(fun() -> ok end),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),

    ExpectedSubscribers = #{Pid1 => []},
    {ok, 0, Disp1} = stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
    ?assertEqual(Disp1, {[{0, Pid1, Ref1, undefined}], 0, ExpectedSubscribers}),

    {ok, 10, Disp2} = stage_broadcast_dispatcher:ask(10, {Pid1, Ref1}, Disp1),
    ?assertEqual(Disp2, {[{0, Pid1, Ref1, undefined}], 10, ExpectedSubscribers}),

    ExpectedSubscribers1 = ExpectedSubscribers#{Pid2 => []},

    {ok, 0, Disp3} = stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp2),
    ?assertEqual(Disp3, {[{-10, Pid2, Ref2, undefined}, {0, Pid1, Ref1, undefined}], 10, ExpectedSubscribers1}),

    ExpectedSubscribers2 = maps:remove(Pid1, ExpectedSubscribers1),

    {ok, 0, Disp4} = stage_broadcast_dispatcher:cancel({Pid1, Ref1}, Disp3),
    ?assertEqual(Disp4, {[{-10, Pid2, Ref2, undefined}], 10, ExpectedSubscribers2}),

    {ok, 0, Disp5} = stage_broadcast_dispatcher:ask(10, {Pid2, Ref2}, Disp4),
    ?assertEqual(Disp5, {[{0, Pid2, Ref2, undefined}], 10, ExpectedSubscribers2}).

multiple_subscriptions_with_late_demand(_Config) ->
    Pid1 = self(),
    Pid2 = spawn_forwarder(),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),

    ExpectedSubscribers = #{Pid1 => []},

    {ok, 0, Disp1} = stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
    ?assertEqual(Disp1, {[{0, Pid1, Ref1, undefined}], 0, ExpectedSubscribers}),

    ExpectedSubscribers1 = ExpectedSubscribers#{Pid2 => []},

    {ok, 0, Disp2} = stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp1),
    ?assertEqual(Disp2, {[{0, Pid2, Ref2, undefined}, {0, Pid1, Ref1, undefined}], 0, ExpectedSubscribers1}),

    {ok, 0, Disp3} = stage_broadcast_dispatcher:ask(10, {Pid1, Ref1}, Disp2),
    ?assertEqual(Disp3, {[{10, Pid1, Ref1, undefined}, {0, Pid2, Ref2, undefined}], 0, ExpectedSubscribers1}),

    ExpectedSubscribers2 = maps:remove(Pid2, ExpectedSubscribers1),

    {ok, 10, Disp4} = stage_broadcast_dispatcher:cancel({Pid2, Ref2}, Disp3),
    ?assertEqual(Disp4, {[{0, Pid1, Ref1, undefined}], 10, ExpectedSubscribers2}),

    {ok, 10, Disp5} = stage_broadcast_dispatcher:ask(10, {Pid1, Ref1}, Disp4),
    ?assertEqual(Disp5, {[{0, Pid1, Ref1, undefined}], 20, ExpectedSubscribers2}).

%subscribes_asks_and_dispatches_to_multiple_consumers(_Config) ->
%    Pid1 = spawn_forwarder(),
%    Pid2 = spawn_forwarder(),
%    Pid3 = spawn_forwarder(),
%    Ref1 = make_ref(),
%    Ref2 = make_ref(),
%    Ref3 = make_ref(),
%    Disp = dispatcher([]),
%
%    {ok, 0, Disp1} = stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
%    {ok, 0, Disp2} = stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp1),
%
%    {ok, 0, Disp3} = stage_broadcast_dispatcher:ask(3, {Pid1, Ref1}, Disp2),
%    {ok, 2, Disp4} = stage_broadcast_dispatcher:ask(2, {Pid2, Ref2}, Disp3),
%
%    ExpectedSubscribers = #{Pid1 => [], Pid2 => []},
%
%    ?assertEqual(Disp4, {[{0, Pid2, Ref2, undefined}, {1, Pid1, Ref1, undefined}], 2, ExpectedSubscribers}),
%
%    % One batch fits all
%    {ok, [], Disp5} = stage_broadcast_dispatcher:dispatch([a, b], 2, Disp4),
%    ?assertEqual(Disp5, {[{0, Pid2, Ref2, undefined}, {1, Pid1, Ref1, undefined}], 0, ExpectedSubscribers}),
%
%    ?assertReceive({'$gen_consumer', {_, Ref1}, [a, b]),
%    ?assertReceive({'$gen_consumer', {_, Ref2}, [a, b]),
%
%    % A batch with left-over
%    {ok, 1, Disp6} = stage_broadcast_dispatcher:ask(2, {Pid2, Ref2}, Disp5),
%
%    {ok, [d], Disp7} = stage_broadcast_dispatcher:dispatch([c, d], 2, Disp6),
%    ?assertEqual(Disp7, {[{1, Pid2, Ref2, undefined}, {0, Pid1, Ref1, undefined}], 0, ExpectedSubscribers),
%    ?assertReceive({'$gen_consumer', {_, Ref1}, [c]),
%    ?assertReceive({'$gen_consumer', {_, Ref2}, [c]),
%
%    % A batch with no demand
%    {ok, [d], Disp8} = stage_broadcast_dispatcher:dispatch([d], 1, Disp7),
%    ?assertEqual(Disp8, {[{1, Pid2, Ref2, undefined}, {0, Pid1, Ref1, undefined}], 0, ExpectedSubscribers}),
%    refute_receive({'$gen_consumer', {_, _}, _}),
%
%    % Add a late subscriber
%    {ok, 1, Disp9} = stage_broadcast_dispatcher:ask(1, {Pid1, Ref1}, Disp8),
%    {ok, 0, Disp10} = stage_broadcast_dispatcher:subscribe([], {Pid3, Ref3}, Disp9),
%    {ok, [e], Disp11} = stage_broadcast_dispatcher:dispatch([d, e], 2, Disp10)

% TODO

delivers_info_to_current_process(_Config) ->
    Pid1 = spawn_forwarder(),
    Pid2 = spawn_forwarder(),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Disp = dispatcher([]),

    {ok, 0, Disp1} = stage_broadcast_dispatcher:subscribe([], {Pid1, Ref1}, Disp),
    {ok, 0, Disp2} = stage_broadcast_dispatcher:subscribe([], {Pid2, Ref2}, Disp1),
    {ok, 0, Disp3} = stage_broadcast_dispatcher:ask(3, {Pid1, Ref1}, Disp2),

    {ok, NotifyDisp} = stage_broadcast_dispatcher:info(hello, Disp3),
    ?assertEqual(Disp3, NotifyDisp),
    ?assertReceive(hello).

% TODO subscribing is idempotent

spawn_forwarder() ->
    Parent = self(),
    spawn_link(fun() -> forwarder_loop(Parent) end).

forwarder_loop(Parent) ->
    receive
        Msg ->
            erlang:send(Parent, Msg),
            forwarder_loop(Parent)
    end.
