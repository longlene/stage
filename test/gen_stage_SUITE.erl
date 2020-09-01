-module(gen_stage_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(assertReceive(Guard), receive Guard -> true end).

-export([all/0]).
-export([
         default_max_and_min_demand/1,
         min_80_demand/1,
         min_20_demand/1,
         max_1_and_min_0_demand/1,
         shared_demand/1,

         handle_info/1,
         terminate/1
        ]).

all() ->
    [
     default_max_and_min_demand,
     min_80_demand,
     min_20_demand,
     max_1_and_min_0_demand,
     shared_demand,

     handle_info,
     terminate
    ].

default_max_and_min_demand(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, _} = forwarder:start_link({consumer, self(), [{subscribe_to, [Producer]}]}),
    Data = lists:seq(0, 499),
    ?assertReceive({consumed, Data}),
    Data1 = lists:seq(500, 999),
    ?assertReceive({consumed, Data1}).

min_80_demand(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, _} = forwarder:start_link({consumer, self(), [{subscribe_to, [{Producer, [{min_demand, 80}, {max_demand, 100}]}]}]}),
    Data = lists:seq(0, 19),
    ?assertReceive({consumed, Data}),
    Data1 = lists:seq(20, 39),
    ?assertReceive({consumed, Data1}).


min_20_demand(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, _} = forwarder:start_link({consumer, self(), [{subscribe_to, [{Producer, [{min_demand, 20}, {max_demand, 100}]}]}]}),
    Data = lists:seq(0, 79),
    ?assertReceive({consumed, Data}),
    Data1 = lists:seq(80, 99),
    ?assertReceive({consumed, Data1}).

max_1_and_min_0_demand(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, Consumer} = forwarder:start_link({consumer, self()}),
    ok = gen_stage:async_subscribe(Consumer, [{to, Producer}, {max_demand, 1}, {min_demand, 0}]),
    ?assertReceive({consumed, [0]}),
    ?assertReceive({consumed, [1]}),
    ?assertReceive({consumed, [2]}).

shared_demand(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0, [{dispatcher, stage_broadcast_dispatcher}]}),
    {ok, Consumer1} = forwarder:start_link({consumer, self()}),
    {ok, Consumer2} = forwarder:start_link({consumer, self()}),
    ok = gen_stage:async_subscribe(Consumer1, [{to, Producer}, {max_demand, 10}, {min_demand, 0}]),
    ok = gen_stage:async_subscribe(Consumer2, [{to, Producer}, {max_demand, 20}, {min_demand, 0}]),
    ?assertReceive({consumer_subscribed,{Producer, _}}),
    ?assertReceive({consumer_subscribed,{Producer, _}}).

% TODO

handle_info(_Config) ->
    {ok,Consumer} = forwarder:start_link({consumer, self()}),
    Ref = make_ref(),
    erlang:send(Consumer, {'DOWN', Ref, process, self(), oops}),
    ?assertReceive({'DOWN', Ref, process, Pid, oops} when Pid =:= self()).


terminate(_Config) ->
    {ok, Pid} = forwarder:start_link({consumer, self()}),
    ok = gen_stage:stop(Pid),
    ?assertReceive({terminated, normal}).

