-module(stage_demand_dispatcher_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(assertReceive(Guard), receive Guard -> true end).

-export([all/0]).
-export([
         subscribes_and_cancels/1
        ]).

all() ->
    [
     subscribes_and_cancels
    ].

dispatcher(Opts) ->
    {ok, {[], 0, undefined} = State} = stage_demand_dispatcher:init(Opts),
    State.

subscribes_and_cancels(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    Disp = dispatcher([]),

    {ok, 0, Disp1} = stage_demand_dispatcher:subscribe([], {Pid, Ref}, Disp),
    ?assertEqual(Disp1, {[{0, Pid, Ref}], 0, undefined}),

    {ok, 0, Disp2} = stage_demand_dispatcher:cancel({Pid, Ref}, Disp1),
    ?assertEqual(Disp2, {[], 0, undefined}).

