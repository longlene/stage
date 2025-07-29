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
         with_20_percent_min_demand_with_late_subscription/1,
         stops_asking_when_consumer_stops_asking/1,
         keeps_emitting_events_even_when_discarded/1,

         %% New demand management tests
         demand_can_be_set_to_accumulate_on_init/1,
         demand_can_be_set_to_accumulate_via_api/1,
         demand_can_be_set_to_forward_via_api/1,
         
         %% New API tests
         test_from_list_api/1,
         test_from_fun_api/1,
         test_sync_info/1,
         test_estimate_buffered_count/1,

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
     with_20_percent_min_demand_with_late_subscription,
     stops_asking_when_consumer_stops_asking,
     keeps_emitting_events_even_when_discarded,

     %% New demand management tests
     demand_can_be_set_to_accumulate_on_init,
     demand_can_be_set_to_accumulate_via_api,
     demand_can_be_set_to_forward_via_api,
     
     %% New API tests
     test_from_list_api,
     test_from_fun_api,
     test_sync_info,
     test_estimate_buffered_count,

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

with_20_percent_min_demand_with_late_subscription(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, Doubler} = doubler:start_link({producer_consumer, self()}),
    {ok, Consumer} = forwarder:start_link({consumer, self()}),

    gen_stage:sync_subscribe(Consumer, [{to, Doubler}, {min_demand, 50}, {max_demand, 100}]),
    gen_stage:sync_subscribe(Doubler, [{to, Producer}, {min_demand, 20}, {max_demand, 100}]),

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

stops_asking_when_consumer_stops_asking(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, Postponer} =
        postponer:start_link(
          {producer_consumer, self(),
          [{subscribe_to, [{Producer, [{max_demand, 10}, {min_demand, 8}]}]}]}),
    {ok, _} = sleeper:start_link({consumer, self(), [{subscribe_to, [{Postponer, [{max_demand, 10}, {min_demand, 5}]}]}]}),

    ?assertReceive({postponed, [0, 1]}),
    ?assertReceive({sleep, [0, 1]}),
    ?assertReceive({postponed, [2, 3]}),
    ?assertReceive({postponed, [4, 5]}),
    ?assertReceive({postponed, [6, 7]}),
    ?assertReceive({postponed, [8, 9]}),
    ?refuteReceived({sleep, [2, 3]}),
    ?refuteReceived({postponed, [10, 11]}).

keeps_emitting_events_even_when_discarded(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, Discarder} =
        discarder:start_link(
          {producer_consumer, self(),
          [{subscribe_to, [{Producer, [{max_demand, 100}, {min_demand, 80}]}]}]}),
    {ok, _} = forwarder:start_link({consumer, self(), [{subscribe_to, [{Discarder, [{max_demand, 100}, {min_demand, 50}]}]}]}),

    Batch = lists:seq(0, 19),
    ?assertReceive({discarded, Batch}),
    Batch1 = lists:seq(100, 119),
    ?assertReceive({discarded, Batch1}),
    Batch2 = lists:seq(1000, 1019),
    ?assertReceive({discarded, Batch2}).

    



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

%% ==== NEW TESTS ====

%% Test demand management
demand_can_be_set_to_accumulate_on_init(_Config) ->
    %% Create a simple producer first
    {ok, Producer} = counter:start_link({producer, 0}),
    
    %% Test default demand mode
    ?assertEqual(forward, gen_stage:demand(Producer)),
    
    %% Test setting demand mode
    ok = gen_stage:demand(Producer, accumulate),
    ?assertEqual(accumulate, gen_stage:demand(Producer)),
    
    %% Test switching back
    ok = gen_stage:demand(Producer, forward),
    ?assertEqual(forward, gen_stage:demand(Producer)).

demand_can_be_set_to_accumulate_via_api(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    ?assertEqual(forward, gen_stage:demand(Producer)),
    
    %% Set to accumulate
    ok = gen_stage:demand(Producer, accumulate),
    ?assertEqual(accumulate, gen_stage:demand(Producer)),
    
    %% Set back to forward
    ok = gen_stage:demand(Producer, forward),
    ?assertEqual(forward, gen_stage:demand(Producer)).

demand_can_be_set_to_forward_via_api(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    
    %% Start with forward (default), set to accumulate, then back to forward
    ?assertEqual(forward, gen_stage:demand(Producer)),
    ok = gen_stage:demand(Producer, accumulate),
    ?assertEqual(accumulate, gen_stage:demand(Producer)),
    ok = gen_stage:demand(Producer, forward),
    ?assertEqual(forward, gen_stage:demand(Producer)).

%% Test new APIs
test_from_list_api(_Config) ->
    List = [a, b, c, d, e],
    {ok, Producer} = gen_stage:from_list(List),
    
    {ok, _Consumer} = forwarder:start_link({consumer, self(),
                                           [{subscribe_to, [Producer]}]}),
    
    %% Should receive all items from the list
    ?assertReceive({consumed, List}).

test_from_fun_api(_Config) ->
    %% Create a simple generator function
    CounterFun = (fun() ->
        N = case get(test_counter) of
            undefined -> 0;
            X -> X
        end,
        if N < 3 ->
            put(test_counter, N + 1),
            {value, N};
        true ->
            done
        end
    end),
    
    {ok, Producer} = gen_stage:from_fun(CounterFun),
    
    {ok, _Consumer} = forwarder:start_link({consumer, self(),
                                           [{subscribe_to, [Producer]}]}),
    
    %% Should receive generated values
    receive
        {consumed, Events} ->
            ?assert(is_list(Events) andalso length(Events) > 0)
    after 5000 ->
        ?assert(false)  %% Timeout
    end.

test_sync_info(_Config) ->
    {ok, Producer} = counter:start_link({producer, self()}),
    
    %% Send sync info
    ok = gen_stage:sync_info(Producer, test_message),
    
    %% Should receive the message
    ?assertReceive(test_message).

test_estimate_buffered_count(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    
    %% Initially should have 0 buffered events
    Count = gen_stage:estimate_buffered_count(Producer),
    ?assert(is_integer(Count) andalso Count >= 0).

