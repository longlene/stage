%% @doc
%% Simple test worker for consumer_supervisor tests
%% @end
-module(test_worker).

-export([start_link/1]).

start_link(Event) ->
    %% Simple worker that just starts and does nothing
    Pid = spawn_link(fun() -> 
        %% Worker process that handles the event
        worker_loop(Event)
    end),
    {ok, Pid}.

worker_loop(Event) ->
    %% Simulate some work
    timer:sleep(10),
    %% Send a message that work is done (for testing)
    case whereis(test_supervisor_monitor) of
        undefined -> ok;
        Pid -> Pid ! {worker_completed, Event}
    end,
    %% Worker terminates normally
    ok.