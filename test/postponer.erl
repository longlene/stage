-module(postponer).

-behavior(gen_stage).

-export([start_link/1,
         start_link/2]).

-export([init/1,
         handle_events/3,
         handle_info/2]).

start_link(Init) ->
    start_link(Init, []).

start_link(Init, Opts) ->
    gen_stage:start_link(?MODULE, Init, Opts).

% callbacks
init(Init) ->
    Init.

handle_events(Events, _From, Recipient) ->
    erlang:send(self(), {postponed, Events}),
    {noreply, [], Recipient}.

handle_info({postponed, Events}, Recipient) ->
    Recipient ! {postponed, Events},
    {noreply, Events, Recipient}.

