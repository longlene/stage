%% @doc
%% Default callback module for starting a consumer_supervisor with a
%% plain list of children (see `consumer_supervisor:start_link/2').
%%
%% `init/1' simply returns the given `{Children, Opts}' as a successful
%% init result: ` {ok, Children, Opts}'.
%% @end
-module(consumer_supervisor_default).

-export([init/1]).

init({Children, Opts}) when is_list(Children), is_list(Opts) ->
    {ok, Children, Opts}.
