%% Port of Elixir GenStage's test/gen_stage/buffer_test.exs
-module(gen_stage_buffer_SUITE).

-include("test_helper.hrl").

-export([all/0]).
-export([
    test_estimate_size_ignores_permanents/1,
    test_first_discards_excess/1,
    test_first_infinity/1,
    test_last_keeps_last/1,
    test_last_emits_displaced_permanents/1,
    test_store_permanent_empty/1,
    test_take_empty/1,
    test_take_stops_at_permanent/1,
    test_take_permanents_same_position/1,
    test_take_interleaved_wheel_positions/1,
    test_take_infinity_permanents_fifo/1,
    test_take_infinity_stops_before_next_temporary/1,
    test_take_infinity_permanents_after_last_temporary/1
]).

all() ->
    [
        test_estimate_size_ignores_permanents,
        test_first_discards_excess,
        test_first_infinity,
        test_last_keeps_last,
        test_last_emits_displaced_permanents,
        test_store_permanent_empty,
        test_take_empty,
        test_take_stops_at_permanent,
        test_take_permanents_same_position,
        test_take_interleaved_wheel_positions,
        test_take_infinity_permanents_fifo,
        test_take_infinity_stops_before_next_temporary,
        test_take_infinity_permanents_after_last_temporary
    ].

test_estimate_size_ignores_permanents(_Config) ->
    B0 = gen_stage_buffer:new(10),
    {B1, _, _} = gen_stage_buffer:store_temporary(B0, [temp1, temp2], first),
    {ok, B2} = gen_stage_buffer:store_permanent_unless_empty(B1, perm3),
    {ok, B3} = gen_stage_buffer:store_permanent_unless_empty(B2, perm4),
    ?assertEqual(2, gen_stage_buffer:estimate_size(B3)).

test_first_discards_excess(_Config) ->
    B0 = gen_stage_buffer:new(3),
    {B1, 0, _} = gen_stage_buffer:store_temporary(B0, [a, b, c], first),
    {B2, 3, _} = gen_stage_buffer:store_temporary(B1, [d, e, f], first),
    ?assertEqual(3, gen_stage_buffer:estimate_size(B2)),
    ?assertMatch({ok, _, _, [a, b, c], []},
                 gen_stage_buffer:take_count_or_until_permanent(B2, 3)).

test_first_infinity(_Config) ->
    B0 = gen_stage_buffer:new(infinity),
    {B1, 0, []} = gen_stage_buffer:store_temporary(B0, lists:seq(1, 1000), first),
    ?assertEqual(1000, gen_stage_buffer:estimate_size(B1)).

test_last_keeps_last(_Config) ->
    B0 = gen_stage_buffer:new(3),
    {B1, 0, _} = gen_stage_buffer:store_temporary(B0, [a, b, c], last),
    {B2, 3, _} = gen_stage_buffer:store_temporary(B1, [d, e, f], last),
    ?assertMatch({ok, _, _, [d, e, f], []},
                 gen_stage_buffer:take_count_or_until_permanent(B2, 3)).

test_last_emits_displaced_permanents(_Config) ->
    B0 = gen_stage_buffer:new(3),
    {B1, _, _} = gen_stage_buffer:store_temporary(B0, [temp1, temp2], last),
    {ok, B2} = gen_stage_buffer:store_permanent_unless_empty(B1, perm3),
    {ok, B3} = gen_stage_buffer:store_permanent_unless_empty(B2, perm4),
    {_, Excess, Perms} = gen_stage_buffer:store_temporary(B3, [temp5, temp6, temp7], last),
    ?assertEqual(2, Excess),
    ?assert(length(Perms) > 0).

test_store_permanent_empty(_Config) ->
    ?assertEqual(empty, gen_stage_buffer:store_permanent_unless_empty(gen_stage_buffer:new(10), perm1)).

test_take_empty(_Config) ->
    ?assertEqual(empty, gen_stage_buffer:take_count_or_until_permanent(gen_stage_buffer:new(10), 5)).

test_take_stops_at_permanent(_Config) ->
    B0 = gen_stage_buffer:new(10),
    {B1, _, _} = gen_stage_buffer:store_temporary(B0, [temp1, temp2], first),
    {ok, B2} = gen_stage_buffer:store_permanent_unless_empty(B1, perm3),
    {B3, _, _} = gen_stage_buffer:store_temporary(B2, [temp4], first),
    ?assertMatch({ok, _, 3, [temp1, temp2], [perm3]},
                 gen_stage_buffer:take_count_or_until_permanent(B3, 5)).

test_take_permanents_same_position(_Config) ->
    B0 = gen_stage_buffer:new(10),
    {B1, _, _} = gen_stage_buffer:store_temporary(B0, [temp1, temp2], first),
    B2 = store_perms(B1, [perm3, perm4, perm5]),
    ?assertMatch({ok, _, _, [temp1, temp2], [perm3, perm4, perm5]},
                 gen_stage_buffer:take_count_or_until_permanent(B2, 5)).

test_take_interleaved_wheel_positions(_Config) ->
    B0 = gen_stage_buffer:new(10),
    {B1, _, _} = gen_stage_buffer:store_temporary(B0, [temp1, temp2], first),
    B2 = store_perms(B1, [perm3, perm4]),
    {B3, _, _} = gen_stage_buffer:store_temporary(B2, [temp5, temp6, temp7], first),
    B4 = store_perms(B3, [perm8, perm9]),
    {B5, _, _} = gen_stage_buffer:store_temporary(B4, [temp10], first),
    B6 = store_perms(B5, [perm11]),
    {ok, B7, _, [temp1, temp2], [perm3, perm4]} =
        gen_stage_buffer:take_count_or_until_permanent(B6, 3),
    {ok, B8, _, [temp5, temp6, temp7], [perm8, perm9]} =
        gen_stage_buffer:take_count_or_until_permanent(B7, 4),
    ?assertMatch({ok, _, _, [temp10], [perm11]},
                 gen_stage_buffer:take_count_or_until_permanent(B8, 2)).

test_take_infinity_permanents_fifo(_Config) ->
    B0 = gen_stage_buffer:new(infinity),
    {B1, _, _} = gen_stage_buffer:store_temporary(B0, [temp1, temp2], first),
    B2 = store_perms(B1, [perm3, perm4, perm5]),
    {ok, B3, _, Temps, Perms} = gen_stage_buffer:take_count_or_until_permanent(B2, 5),
    ?assertEqual([temp1, temp2], Temps),
    ?assertEqual([perm3, perm4, perm5], Perms),
    ?assertEqual(0, gen_stage_buffer:estimate_size(B3)).

test_take_infinity_stops_before_next_temporary(_Config) ->
    B0 = gen_stage_buffer:new(infinity),
    {B1, _, _} = gen_stage_buffer:store_temporary(B0, [temp1], first),
    B2 = store_perms(B1, [perm2, perm3]),
    {B3, _, _} = gen_stage_buffer:store_temporary(B2, [temp4], first),
    B4 = store_perms(B3, [perm5]),
    {ok, B5, 4, [temp1], [perm2, perm3]} = gen_stage_buffer:take_count_or_until_permanent(B4, 5),
    ?assertMatch({ok, _, 3, [temp4], [perm5]},
                 gen_stage_buffer:take_count_or_until_permanent(B5, 4)).

test_take_infinity_permanents_after_last_temporary(_Config) ->
    B0 = gen_stage_buffer:new(infinity),
    {B1, _, _} = gen_stage_buffer:store_temporary(B0, [temp], first),
    B2 = store_perms(B1, [perm1, perm2]),
    ?assertMatch({ok, _, 0, [temp], [perm1, perm2]},
                 gen_stage_buffer:take_count_or_until_permanent(B2, 1)).

store_perms(Buffer, Perms) ->
    lists:foldl(fun(Perm, B) ->
                        {ok, NewB} = gen_stage_buffer:store_permanent_unless_empty(B, Perm),
                        NewB
                end, Buffer, Perms).
