%% @doc
%% Basic producer-consumer example demonstrating the three stage types.
%% 
%% This example creates a pipeline: Producer A -> Producer-Consumer B -> Consumer C
%% where A emits sequential numbers, B amplifies them, and C prints the results.
%%
%% To run: 
%%   1. Compile: c(producer_consumer).
%%   2. Run: producer_consumer:start_example().
%% @end
-module(producer_consumer).

-behaviour(gen_stage).

-export([start_example/0]).
-export([start_producer/1, start_amplifier/1, start_printer/0]).
-export([init/1, handle_demand/2, handle_events/3]).

%% Example entry point
start_example() ->
    io:format("Starting gen_stage pipeline example...~n"),
    
    %% Start the stages
    {ok, A} = start_producer(0),      %% starting from zero
    {ok, B} = start_amplifier(2),     %% expand by 2  
    {ok, C} = start_printer(),        %% prints events
    
    %% Connect the pipeline
    {ok, _Tag1} = gen_stage:sync_subscribe(B, [{to, A}]),
    {ok, _Tag2} = gen_stage:sync_subscribe(C, [{to, B}]),
    
    io:format("Pipeline started: Producer -> Amplifier -> Printer~n"),
    io:format("Producer emits: [0,1,2,3,...]~n"),
    io:format("Amplifier expands: [0,1,2] -> [0,1,2,1,2,3,2,3,4]~n"),
    io:format("Printer displays results every second~n"),
    io:format("Press Ctrl+C to stop.~n"),
    
    %% Keep the example running
    receive
        stop -> ok
    after 30000 ->
        ok
    end.

%% Start functions
start_producer(Counter) ->
    gen_stage:start_link(?MODULE, {producer, Counter}, []).

start_amplifier(Number) ->
    gen_stage:start_link(?MODULE, {amplifier, Number}, []).

start_printer() ->
    gen_stage:start_link(?MODULE, {printer, ok}, []).

%% GenStage callbacks
init({producer, Counter}) ->
    {producer, Counter};
init({amplifier, Number}) ->
    {producer_consumer, Number};
init({printer, State}) ->
    {consumer, State}.

%% Producer callback
handle_demand(Demand, Counter) when Demand > 0 ->
    %% Generate sequential numbers
    Events = lists:seq(Counter, Counter + Demand - 1),
    io:format("Producer generating: ~p~n", [Events]),
    {noreply, Events, Counter + Demand}.

%% Consumer and Producer-Consumer callback  
handle_events(Events, _From, Number) when is_integer(Number) ->
    %% Amplifier: expand each event into a sequence
    AmplifiedEvents = lists:flatmap(fun(Event) ->
        Seq = lists:seq(Event, Event + Number),
        io:format("Amplifying ~p -> ~p~n", [Event, Seq]),
        Seq
    end, Events),
    {noreply, AmplifiedEvents, Number};

handle_events(Events, _From, State) ->
    %% Printer: just display the events
    timer:sleep(1000),
    io:format("*** PRINTER: Received ~p events: ~p~n", [length(Events), Events]),
    {noreply, [], State}.