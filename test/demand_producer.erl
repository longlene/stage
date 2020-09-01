-module(demand_producer).

-behavior(gen_stage).

-export([start_link/1]).

-export([init/1,
         handle_demand/2 ]).

start_link(Init) ->
    gen_stage:start_link(?MODULE, Init,[]).

% callbacks
init(Args) ->
    Args.

handle_demand(Demand, PastDemands) ->
    {noreply, [Demand], [Demand | PastDemands]}.

