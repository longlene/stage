-module(gen_stage_demand_dispatcher_SUITE).

-include("test_helper.hrl").

-export([all/0]).
-export([
         subscribes_and_cancels/1
        ]).

all() ->
    [
     subscribes_and_cancels
    ].

dispatcher(Opts) ->
    {ok, {[], 0, undefined} = State} = gen_stage_demand_dispatcher:init(Opts),
    State.

subscribes_and_cancels(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    Disp = dispatcher([]),

    {ok, 0, Disp1} = gen_stage_demand_dispatcher:subscribe([], {Pid, Ref}, Disp),
    ?assertEqual(Disp1, {[{0, Pid, Ref}], 0, undefined}),

    {ok, 0, Disp2} = gen_stage_demand_dispatcher:cancel({Pid, Ref}, Disp1),
    ?assertEqual(Disp2, {[], 0, undefined}).

