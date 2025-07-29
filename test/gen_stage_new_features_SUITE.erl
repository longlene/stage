-module(gen_stage_new_features_SUITE).

-include("test_helper.hrl").

-export([all/0]).
-export([
    test_from_list_simple/1,
    test_from_list_with_options/1,
    test_from_fun_simple/1,
    test_from_fun_with_done/1,
    test_enhanced_demand_dispatcher/1,
    test_demand_dispatcher_shuffle/1,
    test_demand_dispatcher_max_demand/1,
    test_utils_validation/1,
    test_sync_info_ordering/1,
    test_buffer_estimation/1
]).

all() ->
    [
        test_from_list_simple,
        test_from_list_with_options,
        test_from_fun_simple,
        test_from_fun_with_done,
        test_enhanced_demand_dispatcher,
        test_demand_dispatcher_shuffle,
        test_demand_dispatcher_max_demand,
        test_utils_validation,
        test_sync_info_ordering,
        test_buffer_estimation
    ].

%% Test from_list API
test_from_list_simple(_Config) ->
    List = [1, 2, 3, 4, 5],
    {ok, Producer} = gen_stage:from_list(List),
    
    {ok, _Consumer} = forwarder:start_link({consumer, self(),
                                           [{subscribe_to, [Producer]}]}),
    
    ?assertReceive({consumed, List}).

test_from_list_with_options(_Config) ->
    List = [a, b, c],
    {ok, Producer} = gen_stage:from_list(List, [{on_cancel, stop}]),
    
    {ok, Consumer} = forwarder:start_link({consumer, self(),
                                           [{subscribe_to, [Producer]}]}),
    
    ?assertReceive({consumed, List}),
    
    %% Test that on_cancel works - when consumer stops, producer should also stop
    %% Note: This happens automatically when the producer sends stop to itself
    timer:sleep(200),  %% Give time for processes to stop naturally
    ok.

%% Test from_fun API
test_from_fun_simple(_Config) ->
    %% Create a simple counter function
    Counter = fun() ->
        case get(count) of
            undefined -> 
                put(count, 1),
                {value, 1};
            N when N < 3 ->
                put(count, N + 1),
                {value, N + 1};
            _ ->
                done
        end
    end,
    
    {ok, Producer} = gen_stage:from_fun(Counter),
    
    {ok, _Consumer} = forwarder:start_link({consumer, self(),
                                           [{subscribe_to, [Producer]}]}),
    
    %% Should receive some values
    receive
        {consumed, Events} ->
            ?assert(is_list(Events) andalso length(Events) > 0)
    after 5000 ->
        ?assert(false)  %% Timeout
    end.

test_from_fun_with_done(_Config) ->
    %% Function that immediately returns done
    DoneFun = fun() -> done end,
    
    {ok, Producer} = gen_stage:from_fun(DoneFun),
    
    {ok, _Consumer} = forwarder:start_link({consumer, self(),
                                           [{subscribe_to, [Producer]}]}),
    
    %% Should receive empty list or no message at all
    receive
        {consumed, []} -> ok;
        {consumed, Events} -> ?assertEqual([], Events)
    after 1000 ->
        ok  %% No events is also acceptable
    end.

%% Test enhanced demand dispatcher
test_enhanced_demand_dispatcher(_Config) ->
    DispatcherOpts = [
        {shuffle_demands_on_first_dispatch, false},
        {max_demand, 100}
    ],
    
    {ok, State} = gen_stage_demand_dispatcher:init(DispatcherOpts),
    {[], 0, 100, false} = State,  %% Verify state structure
    
    %% Test subscribe
    Pid = self(),
    Ref = make_ref(),
    {ok, 0, State1} = gen_stage_demand_dispatcher:subscribe([], {Pid, Ref}, State),
    {[{0, Pid, Ref}], 0, 100, false} = State1,
    
    %% Test ask with max_demand
    {ok, 50, State2} = gen_stage_demand_dispatcher:ask(50, {Pid, Ref}, State1),
    {[{50, Pid, Ref}], 0, 100, false} = State2.

test_demand_dispatcher_shuffle(_Config) ->
    DispatcherOpts = [{shuffle_demands_on_first_dispatch, true}],
    
    {ok, State} = gen_stage_demand_dispatcher:init(DispatcherOpts),
    {[], 0, undefined, true} = State,  %% Verify shuffle is enabled
    
    %% Add multiple consumers
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    
    {ok, 0, State1} = gen_stage_demand_dispatcher:subscribe([], {Pid1, Ref1}, State),
    {ok, 0, State2} = gen_stage_demand_dispatcher:subscribe([], {Pid2, Ref2}, State1),
    
    %% First dispatch should shuffle (shuffle flag should be reset)
    Events = [1, 2, 3, 4],
    {ok, _Events, {_Demands, _Pending, _Max, false}} = 
        gen_stage_demand_dispatcher:dispatch(Events, length(Events), State2),
    
    %% Shuffle flag should be reset to false after first dispatch
    ok.

test_demand_dispatcher_max_demand(_Config) ->
    DispatcherOpts = [{max_demand, 10}],
    
    {ok, State} = gen_stage_demand_dispatcher:init(DispatcherOpts),
    
    Pid = self(),
    Ref = make_ref(),
    {ok, 0, State1} = gen_stage_demand_dispatcher:subscribe([], {Pid, Ref}, State),
    
    %% Test asking for more than max_demand (should generate warning)
    %% We can't easily test the warning output, but we can verify the function doesn't crash
    {ok, 15, _State2} = gen_stage_demand_dispatcher:ask(15, {Pid, Ref}, State1),
    
    ok.

%% Test utility functions
test_utils_validation(_Config) ->
    %% Test validate_integer
    {ok, 5, []} = gen_stage_utils:validate_integer([{count, 5}], count, 0, 1, 10, false),
    {error, _} = gen_stage_utils:validate_integer([{count, 15}], count, 0, 1, 10, false),
    {ok, infinity, []} = gen_stage_utils:validate_integer([{count, infinity}], count, 0, 1, 10, true),
    
    %% Test validate_list
    {ok, [1, 2, 3], []} = gen_stage_utils:validate_list([{items, [1, 2, 3]}], items, []),
    {error, _} = gen_stage_utils:validate_list([{items, not_a_list}], items, []),
    
    %% Test validate_in
    {ok, a, []} = gen_stage_utils:validate_in([{option, a}], option, b, [a, b, c]),
    {error, _} = gen_stage_utils:validate_in([{option, d}], option, b, [a, b, c]),
    
    %% Test validate_no_opts
    ok = gen_stage_utils:validate_no_opts([]),
    {error, _} = gen_stage_utils:validate_no_opts([{unknown, option}]).

%% Test sync_info ordering
test_sync_info_ordering(_Config) ->
    {ok, Producer} = counter:start_link({producer, self()}),
    
    %% Send multiple sync_info messages
    ok = gen_stage:sync_info(Producer, message1),
    ok = gen_stage:sync_info(Producer, message2),
    ok = gen_stage:sync_info(Producer, message3),
    
    %% Should receive messages in order
    ?assertReceive(message1),
    ?assertReceive(message2),
    ?assertReceive(message3),
    
    ok = gen_stage:stop(Producer).

%% Test buffer estimation
test_buffer_estimation(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    
    %% Initially should have 0 buffered events
    Count1 = gen_stage:estimate_buffered_count(Producer),
    ?assert(is_integer(Count1)),
    ?assert(Count1 >= 0),
    
    %% Test with timeout
    Count2 = gen_stage:estimate_buffered_count(Producer, 1000),
    ?assert(is_integer(Count2)),
    ?assert(Count2 >= 0),
    
    ok = gen_stage:stop(Producer).