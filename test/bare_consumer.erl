%% @doc
%% A bare consumer that only implements the required callbacks.
%% Used to test the default (undefined) handle_call/handle_cast
%% behaviour: a stage that does not export handle_cast must stop with
%% {bad_cast, Request} (like Elixir's `use GenStage' default).
%% @end
-module(bare_consumer).

-behaviour(gen_stage).

-export([start_link/1, init/1, handle_events/3, terminate/2]).

start_link(Recipient) ->
    gen_stage:start_link(?MODULE, Recipient).

init(Recipient) ->
    {consumer, Recipient}.

handle_events(Events, _From, Recipient) ->
    Recipient ! {consumed, Events},
    {noreply, [], Recipient}.

terminate(Reason, Recipient) ->
    Recipient ! {terminated, Reason}.
