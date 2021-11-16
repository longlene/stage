-module(consumer_supervisor_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(assertReceived(Guard),
        ?assertMatch(Guard, receive Msg -> Msg end)).

-export([all/0]).
-export([
        ]).

all() ->
    [
    ].

