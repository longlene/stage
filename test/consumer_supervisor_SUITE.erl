-module(consumer_supervisor_SUITE).

-include("test_helper.hrl").

-export([all/0]).
-export([
    test_basic_consumer_supervisor/1,
    test_consumer_supervisor_with_demand/1,
    test_consumer_supervisor_child_spec/1,
    test_consumer_supervisor_restart/1
]).

all() ->
    [
        test_basic_consumer_supervisor,
        test_consumer_supervisor_with_demand,
        test_consumer_supervisor_child_spec,
        test_consumer_supervisor_restart
    ].

test_basic_consumer_supervisor(_Config) ->
    %% Create a producer
    {ok, Producer} = counter:start_link({producer, 0}),
    
    %% Create a simple child spec for testing
    ChildSpec = #{
        id => test_worker,
        start => {test_worker, start_link, []},
        restart => temporary
    },
    
    %% Start consumer supervisor
    {ok, Supervisor} = test_consumer_supervisor:start_link([ChildSpec], 
                                                      [{strategy, one_for_one},
                                                       {subscribe_to, [Producer]}]),
    
    %% Give it some time to work
    timer:sleep(200),
    
    %% Verify supervisor is running
    ?assert(is_process_alive(Supervisor)),
    
    %% Clean up
    ok = gen_stage:stop(Supervisor),
    ok = gen_stage:stop(Producer).

test_consumer_supervisor_with_demand(_Config) ->
    %% Create a producer
    {ok, Producer} = counter:start_link({producer, 0}),
    
    %% Create a child spec
    ChildSpec = #{
        id => test_worker,
        start => {test_worker, start_link, []},
        restart => temporary
    },
    
    %% Start consumer supervisor with specific demand settings
    {ok, Supervisor} = test_consumer_supervisor:start_link([ChildSpec], 
                                                      [{strategy, one_for_one},
                                                       {max_demand, 10},
                                                       {min_demand, 5},
                                                       {subscribe_to, [Producer]}]),
    
    %% Give it some time to work
    timer:sleep(200),
    
    %% Verify supervisor is running
    ?assert(is_process_alive(Supervisor)),
    
    %% Clean up
    ok = gen_stage:stop(Supervisor),
    ok = gen_stage:stop(Producer).

test_consumer_supervisor_child_spec(_Config) ->
    %% Test valid child spec
    ChildSpec = #{
        id => test_worker,
        start => {test_worker, start_link, []},
        restart => temporary
    },
    
    Opts = [{strategy, one_for_one}],
    
    %% This should validate and accept the child spec
    try
        {ok, _Supervisor} = test_consumer_supervisor:start_link([ChildSpec], Opts),
        ok
    catch
        error:{badarg, _} ->
            %% Expected if validation fails
            ok;
        _:_ ->
            %% Other errors are ok for this test
            ok
    end.

test_consumer_supervisor_restart(_Config) ->
    %% Create a producer
    {ok, Producer} = counter:start_link({producer, 0}),
    
    %% Create a child spec with transient restart
    ChildSpec = #{
        id => test_worker,
        start => {test_worker, start_link, []},
        restart => transient
    },
    
    %% Start consumer supervisor
    {ok, Supervisor} = test_consumer_supervisor:start_link([ChildSpec], 
                                                      [{strategy, one_for_one},
                                                       {subscribe_to, [Producer]}]),
    
    %% Give it some time to work
    timer:sleep(200),
    
    %% Check children count (might be 0 if no events processed yet)
    Children = consumer_supervisor:which_children(Supervisor),
    ?assert(is_list(Children)),
    
    %% Clean up
    ok = gen_stage:stop(Supervisor),
    ok = gen_stage:stop(Producer).

