-module(demand_producer).
%% @doc
%% A producer that tracks demand requests.
%% @end

-behavior(gen_stage).

-export([start_link/1,
         start_link/2]).

-export([init/1,
         handle_demand/2 ]).

start_link(Init) ->
    start_link(Init, []).

start_link(Init, Opts) ->
    gen_stage:start_link(?MODULE, Init, Opts).

% callbacks
init(Init) ->
    Init.

handle_demand(Demand, PastDemands) ->
    {noreply, [Demand], [Demand | PastDemands]}.

