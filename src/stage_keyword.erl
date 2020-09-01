-module(stage_keyword).

-export([
         get/3,
         has_key/2,
         pop/3,
         pop_lazy/3
        ]).

get(Keywords, Key, Default) ->
    case lists:keyfind(Key, 1, Keywords) of
        {Key, Value} -> Value;
        false -> Default
    end.

has_key(Keywords, Key) when is_list(Keywords) andalso is_atom(Key) ->
    lists:keymember(Key, 1, Keywords).

pop(Keywords, Key, Default) ->
    case fetch(Keywords, Key) of
        {ok, Value} -> {Value, delete(Keywords, Key)};
        error -> {Default, Keywords}
    end.

pop_lazy(Keywords, Key, Fun) when is_list(Keywords), is_atom(Key), is_function(Fun, 0) ->
    case fetch(Keywords, Key) of
        {ok, Value} ->
            {Value, delete(Keywords, Key)};
        error ->
            {Fun(), Keywords}
    end.


fetch(Keywords, Key) when is_list(Keywords), is_atom(Key) ->
    case lists:keyfind(Key, 1, Keywords) of
        {Key, Value} -> {ok, Value};
        false -> error
    end.

delete(Keywords, Key) when is_list(Keywords), is_atom(Key) ->
    case lists:keymember(Key, 1, Keywords) of
        true -> delete_key(Keywords, Key);
        _ -> Keywords
    end.

delete_key([{Key, _} | Tail], Key) -> delete_key(Tail, Key);
delete_key([{_, _} = Pair | Tail], Key) -> [Pair | delete_key(Tail, Key)];
delete_key([], _Key) -> [].

