-module(gen_stage_SUITE).

-include("test_helper.hrl").

-export([all/0]).
-export([
         with_default_max_and_min_demand/1,
         with_80_percent_min_demand/1,
         with_20_percent_min_demand/1,
         with_1_max_and_0_min_demand/1,
         with_shared_broadcast_demand/1,
         with_shared_broadcast_demand_and_synchronizer_subscriber/1,
         with_80_percent_min_demand_with_init_subscription/1,
         with_20_percent_min_demand_with_init_subscription/1,
         with_80_percent_min_demand_with_late_subscription/1,

         handle_info/1,
         terminate/1
        ]).

all() ->
    [
     with_default_max_and_min_demand,
     with_80_percent_min_demand,
     with_20_percent_min_demand,
     with_1_max_and_0_min_demand,
     with_shared_broadcast_demand,
     with_shared_broadcast_demand_and_synchronizer_subscriber,
     with_80_percent_min_demand_with_init_subscription,
     with_20_percent_min_demand_with_init_subscription,
     with_80_percent_min_demand_with_late_subscription,

     handle_info,
     terminate
    ].

with_default_max_and_min_demand(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, _} = forwarder:start_link({consumer, self(), [{subscribe_to, [Producer]}]}),
    Data = lists:seq(0, 499),
    ?assertReceive({consumed, Data}),
    Data1 = lists:seq(500, 999),
    ?assertReceive({consumed, Data1}).

with_80_percent_min_demand(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, _} = forwarder:start_link({consumer, self(), [{subscribe_to, [{Producer, [{min_demand, 80}, {max_demand, 100}]}]}]}),
    Data = lists:seq(0, 19),
    ?assertReceive({consumed, Data}),
    Data1 = lists:seq(20, 39),
    ?assertReceive({consumed, Data1}).


with_20_percent_min_demand(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, _} = forwarder:start_link({consumer, self(), [{subscribe_to, [{Producer, [{min_demand, 20}, {max_demand, 100}]}]}]}),
    Data = lists:seq(0, 79),
    ?assertReceive({consumed, Data}),
    Data1 = lists:seq(80, 99),
    ?assertReceive({consumed, Data1}),
    Data2 = lists:seq(100, 179),
    ?assertReceive({consumed, Data2}),
    Data3 = lists:seq(180, 259),
    ?assertReceive({consumed, Data3}),
    Data4 = lists:seq(260, 279),
    ?assertReceive({consumed, Data4}).

with_1_max_and_0_min_demand(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, Consumer} = forwarder:start_link({consumer, self()}),
    ok = gen_stage:async_subscribe(Consumer, [{to, Producer}, {max_demand, 1}, {min_demand, 0}]),
    ?assertReceive({consumed, [0]}),
    ?assertReceive({consumed, [1]}),
    ?assertReceive({consumed, [2]}).

with_shared_broadcast_demand(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0, [{dispatcher, gen_stage_broadcast_dispatcher}]}),
    {ok, Consumer1} = forwarder:start_link({consumer, self()}),
    {ok, Consumer2} = forwarder:start_link({consumer, self()}),
    ok = gen_stage:async_subscribe(Consumer1, [{to, Producer}, {max_demand, 10}, {min_demand, 0}]),
    ok = gen_stage:async_subscribe(Consumer2, [{to, Producer}, {max_demand, 20}, {min_demand, 0}]),
    ?assertReceive({consumed, [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009]}),
    ?assertReceive({consumed, [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009]}).

with_shared_broadcast_demand_and_synchronizer_subscriber(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0, [{dispatcher, gen_stage_broadcast_dispatcher}]}),
    {ok, Consumer1} = forwarder:start_link({consumer, self()}),
    {ok, Consumer2} = forwarder:start_link({consumer, self()}),

    % Subscribe but not demand
    StageRef = make_ref(),
    Producer ! {'$gen_producer', {self(), StageRef}, {subscribe, undefined, []}},

    % Further subscriptions will block
    gen_stage:sync_subscribe(Consumer1, [{to, Producer}, {max_demand, 10}, {min_demand, 0}]),
    gen_stage:sync_subscribe(Consumer2, [{to, Producer}, {max_demand, 20}, {min_demand, 0}]),
    ?refuteReceived({consumed, _}),

    % Cancel the stale one
    Producer ! {'$gen_producer', {self(), StageRef}, {cancel, killed}},

    ?assertReceive({consumed, [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009]}),
    ?assertReceive({consumed, [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009]}).

with_80_percent_min_demand_with_init_subscription(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, Doubler} =
        doubler:start_link(
          {producer_consumer, self(),
          [{subscribe_to, [{Producer, [{max_demand, 100}, {min_demand, 80}]}]}]}),
    {ok, _} = forwarder:start_link({consumer, self(), [{subscribe_to, [{Doubler, [{max_demand, 100}, {min_demand, 50}]}]}]}),
    Batch = lists:seq(0, 19),
    ?assertReceive({producer_consumed, Batch}),
    Batch1 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(0, 19)),
    ?assertReceive({consumed, Batch1}),
    Batch2 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(20, 39)),
    ?assertReceive({consumed, Batch2}),
    Batch3 = lists:seq(100, 119),
    ?assertReceive({producer_consumed, Batch3}),
    Batch4 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(120, 124)),
    ?assertReceive({consumed, Batch4}),
    Batch5 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(125, 139)),
    ?assertReceive({consumed, Batch5}).

with_20_percent_min_demand_with_init_subscription(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, Doubler} =
        doubler:start_link(
          {producer_consumer, self(),
          [{subscribe_to, [{Producer, [{max_demand, 100}, {min_demand, 20}]}]}]}),
    {ok, _} = forwarder:start_link({consumer, self(), [{subscribe_to, [{Doubler, [{max_demand, 100}, {min_demand, 50}]}]}]}),
    Batch = lists:seq(0, 79),
    ?assertReceive({producer_consumed, Batch}),
    Batch1 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(0, 24)),
    ?assertReceive({consumed, Batch1}),
    Batch2 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(25, 49)),
    ?assertReceive({consumed, Batch2}),
    Batch3 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(50, 74)),
    ?assertReceive({consumed, Batch3}),
    Batch4 = lists:seq(100, 179),
    ?assertReceive({producer_consumed, Batch4}).

with_80_percent_min_demand_with_late_subscription(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, Doubler} = doubler:start_link({producer_consumer, self()}),
    {ok, Consumer} = forwarder:start_link({consumer, self()}),

    gen_stage:sync_subscribe(Consumer, [{to, Doubler}, {min_demand, 50}, {max_demand, 100}]),
    gen_stage:sync_subscribe(Doubler, [{to, Producer}, {min_demand, 80}, {max_demand, 100}]),

    Batch = lists:seq(0, 19),
    ?assertReceive({producer_consumed, Batch}),
    Batch1 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(0, 19)),
    ?assertReceive({consumed, Batch1}),
    Batch2 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(20, 39)),
    ?assertReceive({consumed, Batch2}),

    Batch3 = lists:seq(100, 119),
    ?assertReceive({producer_consumed, Batch3}),
    Batch4 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(120, 124)),
    ?assertReceive({consumed, Batch4}),
    Batch5 = lists:flatmap(fun(I) -> [I, I] end, lists:seq(125, 139)),
    ?assertReceive({consumed, Batch5}).

% TODO

handle_info(_Config) ->
    {ok,Consumer} = forwarder:start_link({consumer, self()}),
    Ref = make_ref(),
    erlang:send(Consumer, {'DOWN', Ref, process, self(), oops}),
    ?assertReceive({'DOWN', Ref, process, Pid, oops} when Pid =:= self()).


terminate(_Config) ->
    {ok, Pid} = forwarder:start_link({consumer, self()}),
    ok = gen_stage:stop(Pid),
    ?assertReceive({terminated, normal}).

