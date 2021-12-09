-module(sleeper).
%% @doc
%% Sleeps after the first batch.
%% @end

-behavior(gen_stage).

-export([start_link/1,
         start_link/2]).

-export([init/1,
         handle_events/3]).

start_link(Init) ->
    start_link(Init, []).

start_link(Init, Opts) ->
    gen_stage:start_link(?MODULE, Init, Opts).

% callbacks
init(Init) ->
    Init.

handle_events(Events, _From, Recipient) ->
    Recipient ! {sleep, Events},
    timer:sleep(infinity),
    {noreply, [], Recipient}.

