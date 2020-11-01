-module(stage_buffer).

-export(
   [
    new/1,
    estimate_size/1,
    take_count_or_until_permanent/2,
    store_temporary/3,
    store_permanent_unless_empty/2
   ]).

new(Size) when Size > 0 ->
    {queue:new(), 0, init_wheel(Size)}.

estimate_size({_, Count, _}) -> Count.

store_temporary({Queue, Counter, Infos}, Temps, Keep) when is_list(Temps) ->
    {{Excess, Queue1, Counter1}, Perms, Infos1} =
    store_temporary(Keep, Temps, Queue, Counter, capacity_wheel(Infos), Infos),
    {{Queue1, Counter1, Infos1}, Excess, Perms}.

store_temporary(_Keep, Temps, _Queue, 0, infinity, Infos) ->
    {{0, queue:from_list(Temps), length(Temps)}, [], Infos};
store_temporary(_Keep, Temps, Queue, Counter, infinity, Infos) ->
    {queue_infinity(Temps, Queue, Counter), [], Infos};
store_temporary(first, Temps, Queue, Counter, Max, Infos) ->
    {queue_first(Temps, Queue, Counter, Max), [], Infos};
store_temporary(last, Temps, Queue, Counter, Max, Infos) ->
    queue_last(Temps, Queue, 0, Counter, Max, [], Infos).

%% Infinity
queue_infinity([], Queue, Counter) ->
    {0, Queue, Counter};
queue_infinity([Temp | R], Queue, Counter) ->
    queue_infinity(R, queue:in(Temp, Queue), Counter + 1).

%% First
queue_first([], Queue, Counter, _Max) ->
    {0, Queue, Counter};
queue_first(Temps, Queue, Max, Max) ->
    {length(Temps), Queue, Max};
queue_first([Temp | R], Queue, Counter, Max) ->
    queue_first(R, queue:in(Temp, Queue), Counter + 1, Max).

%% Last
queue_last([], Queue, Excess, Counter, _Max, Perms, Wheel) ->
    {{Excess, Queue, Counter}, Perms, Wheel};
queue_last([Temp | R], Queue, Excess, Max, Max, Perms, Wheel) ->
    NewQueue = queue:in(Temp, queue:drop(Queue)),
    case pop_and_increment_wheel(Wheel) of
        {ok, NewPerms, NewWheel} ->
            queue_last(R, NewQueue, Excess + 1, Max, Max, Perms ++ NewPerms, NewWheel);
        {error, NewWheel} ->
            queue_last(R, NewQueue, Excess + 1, Max, Max, Perms, NewWheel)
    end;
queue_last([Temp | R], Queue, Excess, Counter, Max, Perms, Wheel) ->
    queue_last(R, queue:in(Temp, Queue), Excess, Counter + 1, Max, Perms, Wheel).

store_permanent_unless_empty(Buffer, Perm) ->
    case Buffer of
        {_Queue, 0, _Infos} ->
            empty;
        {Queue, Count, Infos} when is_reference(Infos) ->
            {ok, {queue:in({Infos, Perm}, Queue), Count + 1, Infos}};
        {Queue, Count, Infos} ->
            {ok, {Queue, Count, put_wheel(Infos, Count, Perm)}}
    end.

take_count_or_until_permanent({_Queue, Buffer, _Infos}, Counter) when Buffer =:= 0 orelse Counter =:= 0 ->
    empty;
take_count_or_until_permanent({Queue, Buffer, Infos}, Counter) ->
    take_count_or_until_permanent(Counter, [], Queue, Buffer, Infos).

take_count_or_until_permanent(0, Temps, Queue, Buffer, Infos) ->
    {ok, {Queue, Buffer, Infos}, 0, lists:reverse(Temps), []};
take_count_or_until_permanent(Counter, Temps, Queue, 0, Infos) ->
    {ok, {Queue, 0, Infos}, Counter, lists:reverse(Temps), []};
take_count_or_until_permanent(Counter, Temps, Queue, Buffer, Infos) when is_reference(Infos) ->
    {{value, Value}, NewQueue} = queue:out(Queue),
    case Value of
        {Infos, Perm} ->
            {ok, {NewQueue, Buffer - 1, Infos}, Counter, lists:reverse(Temps), [Perm]};
        Temp ->
            take_count_or_until_permanent(Counter - 1, [Temp | Temps], NewQueue, Buffer - 1, Infos)
    end;
take_count_or_until_permanent(Counter, Temps, Queue, Buffer, Infos) ->
    {{value, Temp}, NewQueue} = queue:out(Queue),
    case pop_and_increment_wheel(Infos) of
        {ok, Perms, NewInfos} ->
            {ok, {NewQueue, Buffer - 1, NewInfos}, Counter - 1, lists:reverse([Temp | Temps]), Perms};
        {error, NewInfos} ->
            take_count_or_until_permanent(Counter - 1, [Temp | Temps], NewQueue, Buffer - 1, NewInfos)
    end.


%% Wheel helpers
init_wheel(infinity) -> make_ref();
init_wheel(Size) -> Size.

capacity_wheel(Ref) when is_reference(Ref) -> infinity;
capacity_wheel({_, Max, _}) -> Max;
capacity_wheel(Max) -> Max.

put_wheel({Pos, Max, Wheel}, Count, Perm) ->
    Key = (Pos + Count - 1) rem Max, 
    Fun = fun(V) -> [Perm | V] end,
    {Pos, Max, maps:update_with(Key, Fun, [Perm], Wheel)};
put_wheel(Max, Count, Perm) ->
    {0, Max, #{((Count - 1) rem Max) => [Perm]}}.

pop_and_increment_wheel({Pos, Max, Wheel}) ->
    NewPos = (Pos + 1) rem Max,
    case maps:take(Pos, Wheel) of
        {Perms, NewWheel} ->
            MaybeTriplet = if NewWheel =:= #{} -> Max; true -> {NewPos, Max, NewWheel} end,
            {ok, Perms, MaybeTriplet};
        error ->
            {error, {NewPos, Max, Wheel}}
    end;
pop_and_increment_wheel(Max) ->
    {error, Max}.
