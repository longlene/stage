%% @doc
%% Test script to verify new features work correctly.
%% 
%% This tests the new from_list/2 and from_fun/2 APIs
%% @end
-module(test_new_features).

-export([test_all/0, test_from_list/0, test_from_fun/0, test_enhanced_dispatcher/0]).

test_all() ->
    io:format("Testing new features...~n"),
    
    io:format("1. Testing from_list API...~n"),
    test_from_list(),
    
    io:format("2. Testing from_fun API...~n"),
    test_from_fun(),
    
    io:format("3. Testing enhanced dispatcher...~n"),
    test_enhanced_dispatcher(),
    
    io:format("All tests completed successfully!~n").

%% Test from_list functionality
test_from_list() ->
    List = [a, b, c, d, e],
    {ok, Producer} = gen_stage:from_list(List),
    
    %% Create a simple consumer to receive events
    Consumer = spawn(fun() -> consumer_loop([]) end),
    
    %% Subscribe consumer to producer
    {ok, _Tag} = gen_stage:sync_subscribe(Consumer, [{to, Producer}]),
    
    %% Wait a bit for events to flow
    timer:sleep(100),
    
    %% Clean up
    gen_stage:stop(Producer),
    Consumer ! stop,
    
    io:format("   from_list test completed~n").

%% Test from_fun functionality  
test_from_fun() ->
    %% Create a simple generator function
    Counter = fun() ->
        case get(counter) of
            undefined -> 
                put(counter, 1),
                {value, 1};
            N when N < 5 ->
                put(counter, N + 1),
                {value, N + 1};
            _ ->
                done
        end
    end,
    
    {ok, Producer} = gen_stage:from_fun(Counter),
    
    %% Create a simple consumer
    Consumer = spawn(fun() -> consumer_loop([]) end),
    
    %% Subscribe consumer to producer
    {ok, _Tag} = gen_stage:sync_subscribe(Consumer, [{to, Producer}]),
    
    %% Wait a bit for events to flow
    timer:sleep(100),
    
    %% Clean up
    gen_stage:stop(Producer),
    Consumer ! stop,
    
    io:format("   from_fun test completed~n").

%% Test enhanced dispatcher features
test_enhanced_dispatcher() ->
    %% Test shuffle_demands_on_first_dispatch option
    DispatcherOpts = [
        {shuffle_demands_on_first_dispatch, true},
        {max_demand, 100}
    ],
    
    {ok, State} = gen_stage_demand_dispatcher:init(DispatcherOpts),
    {[], 0, 100, true} = State,  %% Verify state structure
    
    io:format("   enhanced dispatcher test completed~n").

%% Simple consumer loop for testing
consumer_loop(Events) ->
    receive
        {'$gen_consumer', {_Producer, _Ref}, NewEvents} ->
            AllEvents = Events ++ NewEvents,
            io:format("   Consumer received: ~p (total: ~p)~n", [NewEvents, AllEvents]),
            consumer_loop(AllEvents);
        stop ->
            io:format("   Consumer stopping with ~p events~n", [length(Events)]);
        Other ->
            io:format("   Consumer received unexpected: ~p~n", [Other]),
            consumer_loop(Events)
    after 1000 ->
        io:format("   Consumer timeout with ~p events~n", [length(Events)])
    end.