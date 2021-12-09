-module(forwarder).
%% @doc
%% Forwards messages to the given process.

-behaviour(gen_stage).

-export([start_link/1,
         start_link/2,
         ask/3]).

-export([
         init/1,
         handle_call/3,
         handle_subscribe/4,
         handle_info/2,
         handle_events/3,
         handle_cancel/3,
         terminate/2
        ]).


start_link(Init) ->
    start_link(Init, []).

start_link(Init, Opts) ->
    gen_stage:start_link(?MODULE, Init, Opts).

ask(Forwarder, To, N) ->
    gen_stage:call(Forwarder, {ask, To, N}).

init(Init) ->
    Init.

handle_call({ask, To, N}, _, State) ->
    gen_stage:ask(To, N),
    {reply, ok, [], State}.

handle_subscribe(producer, Opts, From, Recipient) ->
    Recipient ! {consumer_subscribed, From},
    {proplists:get_value(consumer_demand, Opts, automatic), Recipient}.

handle_info(Other, Recipient) ->
    erlang:send(Recipient, Other),
    {noreply, [], Recipient}.

handle_events(Events, _From, Recipient) ->
    Recipient ! {consumed, Events},
    {noreply, [], Recipient}.

handle_cancel(Reason, From, Recipient) ->
    Recipient ! {consumer_cancelled, From, Reason},
    {noreply, [], Recipient}.

terminate(Reason, State) ->
    State ! {terminated, Reason}.
