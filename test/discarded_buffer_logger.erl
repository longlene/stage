-module(discarded_buffer_logger).
%% @doc
%% Logs about any discarded items
%% @end

-behavior(gen_stage).

-export([start_link/1,
         start_link/2,
         sync_queue/2]).

-export([init/1,
         handle_call/3,
         handle_demand/2,
         format_discarded/2]).

start_link(Init) ->
    start_link(Init, []).

start_link(Init, Opts) ->
    gen_stage:start_link(?MODULE, Init, Opts).

sync_queue(Stage, Events) ->
    gen_stage:call(Stage, {queue, Events}).

% callbacks
init(Init) ->
    Init.

handle_call({queue, Events}, _From, State) ->
    {reply, ok, Events, State}.

handle_demand(_Demand, State) ->
    {noreply, [], State}.

format_discarded(Discarded, #{log_discarded := LogDiscarded}) ->
    error_logger:info_msg("discarded_buffer_logger has discarded ~tp events from buffer", [Discarded]),
    LogDiscarded.

