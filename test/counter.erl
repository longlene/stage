-module(counter).
%% @doc
%% A producer that works as a counter in batches.
%% It also supports events to be queued via sync
%% and async calls. A negative counter disables
%% the counting behaviour.
%% @end

-behavior(gen_stage).

-export([start_link/1,
         start_link/2,
         sync_queue/2,
         async_queue/2,
         stop/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         handle_subscribe/4,
         handle_cancel/3,
         handle_demand/2,
         terminate/2]).

start_link(Init) ->
    start_link(Init, []).

start_link(Init, Opts) ->
    gen_stage:start_link(?MODULE, Init, Opts).

sync_queue(Stage, Events) ->
    gen_stage:call(Stage, {queue, Events}).

async_queue(Stage, Events) ->
    gen_stage:cast(Stage, {queue, Events}).

stop(Stage) ->
    gen_stage:call(Stage, stop).

% callbacks
init(Init) ->
    Init.

handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State};
handle_call({early_reply_queue, Events}, From, State) ->
    gen_stage:reply(From, State),
    {noreply, Events, State};
handle_call({queue, Events}, _From, State) ->
    {reply, State, Events, State}.

handle_cast({queue, Events}, State) ->
    {noreply, Events, State}.

handle_info({queue, Events}, State) ->
    {noreply, Events, State};
handle_info(Other, State) ->
    case is_pid(State) of
        true -> erlang:send(State, Other);
        false -> ok
    end,
    {noreply, [], State}.

handle_subscribe(consumer, Opts, From, State) ->
    case is_pid(State) of
        true -> erlang:send(State, {producer_subscribed, From});
        false -> ok
    end,
    {proplists:get_value(producer_demand, Opts, automatic), State}.

handle_cancel(Reason, From, State) ->
    case is_pid(State) of
        true -> erlang:send(State, {producer_canclled, From, Reason});
        false -> ok
    end,
    {noreply, [], State}.

handle_demand(Demand, Pid) when is_pid(Pid) andalso Demand > 0 ->
    {noreply, [], Pid};
handle_demand(Demand, Counter) when Demand > 0 ->
    Events = lists:seq(Counter, Counter + Demand - 1),
    {noreply, Events, Counter + Demand}.

terminate(Reason, State) ->
    io:format("terminate, reason ~p, state ~p~n", [Reason, State]),
    ok.
