-module(doubler).

-behavior(gen_stage).

-export([start_link/1]).

-export([init/1,
         handle_info/2,
         handle_subscribe/4,
         handle_cancel/3,
         handle_events/3
        ]).

start_link(Init) ->
    gen_stage:start_link(?MODULE, Init,[]).

% callbacks
init(Args) ->
    Args.

handle_subscribe(Kind, Opts, From, Recipient) ->
    erlang:send(Recipient, {producer_consumer_subscribed, Kind, From}),
    {proplists:get_value(producer_consumer_demand, Opts, automatic),Recipient}.

handle_cancel(Reason, From, Recipient) ->
    erlang:send(Recipient, {producer_consumer_cancelled, From, Reason}),
    {noreply, [], Recipient}.

handle_events(Events, _From, Recipient) ->
    erlang:send(Recipient, {producer_consumed, Events}),
    {noreply, lists:flatmap(fun(X) -> [X, X] end, Events), Recipient}.

handle_info(Other, State) ->
    case is_pid(State) of
        true ->
            erlang:send(State, Other);
        false ->
            ok
    end,
    {noreply, [], State}.

