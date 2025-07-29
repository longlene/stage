%% @doc
%% Simple test supervisor module for consumer_supervisor tests
%% @end
-module(test_consumer_supervisor).

-export([start_link/2, init/1]).

%% @doc
%% Start a consumer supervisor with given children and options
%% @end
start_link(Children, Opts) ->
    consumer_supervisor:start_link(?MODULE, {Children, Opts}).

%% @doc
%% Initialize the supervisor with the given children and options
%% @end
init({Children, Opts}) ->
    %% Return the standard consumer supervisor init format
    {ok, Children, Opts}.