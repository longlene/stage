-module(gen_stage_parity_SUITE).

-include("test_helper.hrl").

-export([all/0]).
-export([
    test_accumulate_demand_mode/1,
    test_demand_mode_get/1,
    test_default_handle_cast_stops/1,
    test_default_handle_call_stops/1,
    test_from_fun_without_link/1,
    test_stream/1,
    test_stream_producer_down/1,
    test_whereis/1,
    test_consumer_supervisor_children_start_link/1,
    test_consumer_supervisor_rejects_permanent_template/1,
    test_consumer_supervisor_requires_strategy/1,
    test_consumer_supervisor_which_children_shape/1,
    test_accumulate_holds_dispatched_events/1,
    test_from_fun_multiple_batches/1,
    test_partition_dispatcher_sync_info/1,
    test_consumer_supervisor_restart_keeps_event/1,
    test_producer_consumer_info_after_last_event/1
]).
-export([start_crashing_worker/2]).

all() ->
    [
        test_accumulate_demand_mode,
        test_demand_mode_get,
        test_default_handle_cast_stops,
        test_default_handle_call_stops,
        test_from_fun_without_link,
        test_stream,
        test_stream_producer_down,
        test_whereis,
        test_consumer_supervisor_children_start_link,
        test_consumer_supervisor_rejects_permanent_template,
        test_consumer_supervisor_requires_strategy,
        test_consumer_supervisor_which_children_shape,
        test_accumulate_holds_dispatched_events,
        test_from_fun_multiple_batches,
        test_partition_dispatcher_sync_info,
        test_consumer_supervisor_restart_keeps_event,
        test_producer_consumer_info_after_last_event
    ].

%% ===== Demand accumulate mode =====

%% Mirrors Elixir GenStage's "can be set to :accumulate on init" test
%% (test/gen_stage_test.exs). See also test_accumulate_holds_dispatched_events
%% for events returned from other callbacks.
test_accumulate_demand_mode(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0, [{demand, accumulate}]}, []),
    ?assertEqual(accumulate, gen_stage:demand(Producer)),
    {ok, Consumer} = forwarder:start_link({consumer, self()}),

    {ok, _Ref} = gen_stage:sync_subscribe(Consumer, [{to, Producer}, {max_demand, 4}, {min_demand, 0}]),
    ?refuteReceive({consumed, [0, 1, 2, 3]}),

    ok = gen_stage:demand(Producer, forward),
    ?assertEqual(forward, gen_stage:demand(Producer)),
    ?assertReceive({consumed, [0, 1, 2, 3]}),

    ok = gen_stage:stop(Consumer),
    ok = gen_stage:stop(Producer).

test_demand_mode_get(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}, []),
    ?assertEqual(forward, gen_stage:demand(Producer)),
    ok = gen_stage:stop(Producer).

%% ===== Default callbacks (Elixir `use GenStage' defaults) =====

test_default_handle_cast_stops(_Config) ->
    {ok, Consumer} = bare_consumer:start_link(self()),
    unlink(Consumer),
    Ref = monitor(process, Consumer),

    ok = gen_stage:cast(Consumer, {unknown_thing, 1}),

    %% Elixir's default handle_cast stops the stage with {bad_cast, Request}
    ?assertReceive({terminated, {bad_cast, {unknown_thing, 1}}}),
    ?assertReceive({'DOWN', Ref, process, Consumer, _}).

test_default_handle_call_stops(_Config) ->
    {ok, Consumer} = bare_consumer:start_link(self()),
    unlink(Consumer),
    Ref = monitor(process, Consumer),

    try gen_stage:call(Consumer, {unknown_call, 2}) of
        _Reply -> ?assert(false)
    catch
        %% Elixir's default handle_call stops the stage with
        %% {bad_call, Request}; OTP 29 propagates {Reason, Stack}
        exit:{{bad_call, {unknown_call, 2}}, _Stack} ->
            ok
    end,
    ?assertReceive({'DOWN', Ref, process, Consumer, _}).

%% ===== from_fun =====

test_from_fun_without_link(_Config) ->
    Fun = fun() -> done end,
    {ok, Producer} = gen_stage:from_fun(Fun, [{link, false}]),
    {links, Links} = process_info(self(), links),
    ?assertNot(lists:member(Producer, Links)),
    unlink(Producer),
    exit(Producer, kill).

%% ===== gen_stage_stream =====

test_stream(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, {MonitorPid, MonitorRef, Subscriptions}} = gen_stage_stream:subscribe([Producer]),
    ?assertEqual(1, map_size(Subscriptions)),
    ?assert(is_process_alive(MonitorPid)),

    %% Receive a first batch of events
    {Pid1, From1, Events} = receive_first_events(Producer, MonitorRef, 5000),
    ?assertEqual(Producer, Pid1),
    %% (no ?assertMatch here: the macro rebinds pattern variables inside a fun)
    {MRef1, _InnerRef} = From1,
    ?assertEqual(MonitorRef, MRef1),
    ?assert(length(Events) > 0),

    %% Ask for more events on the same subscription. The From used with
    %% ask/2 must be the full {ProducerPid, {MonitorRef, InnerRef}} tuple
    %% (the second element of the '$gen_consumer' message), not the bare
    %% inner ref pair - sending to a reference silently drops the message.
    ok = gen_stage_stream:ask({Pid1, From1}, 5),
    {Pid2, _From2, Events2} = receive_first_events(Producer, MonitorRef, 5000),
    ?assertEqual(Producer, Pid2),
    ?assert(length(Events2) > 0),

    %% Close the stream: subscriptions are cancelled and the monitor dies
    ok = gen_stage_stream:close({MonitorPid, MonitorRef, Subscriptions}),
    ?assertNot(is_process_alive(MonitorPid)),
    ok = gen_stage:stop(Producer).

%% @private
%% Receive the first events message for this producer/stream; returns
%% {Producer, {MonitorRef, InnerRef}, Events}.
receive_first_events(Producer, MonitorRef, Timeout) ->
    receive
        {'$gen_consumer', {Pid, {MonitorRef, _InnerRef}}, Events} when is_list(Events) ->
            ?assertEqual(Producer, Pid),
            {Pid, {MonitorRef, _InnerRef}, Events}
    after
        Timeout ->
            erlang:exit({timeout_receiving_events, Producer})
    end.

test_stream_producer_down(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    {ok, {MonitorPid, MonitorRef, _Subscriptions}} =
        gen_stage_stream:subscribe([{Producer, [{cancel, temporary}]}]),

    %% A first batch should arrive
    ?assertReceive({'$gen_consumer', {_, {MonitorRef, _}}, _}),

    %% The producer dying is forwarded to the caller
    unlink(Producer),
    exit(Producer, kill),
    ?assertReceive({MonitorRef, {'DOWN', _InnerRef, _Reason}}),

    ok = gen_stage_stream:close({MonitorPid, MonitorRef, #{}}).

%% ===== gen_stage:whereis/1 =====

test_whereis(_Config) ->
    ?assertEqual(undefined, gen_stage:whereis(no_such_stage_xyz)),

    {ok, Producer} = counter:start_link({producer, 0}),
    ?assertEqual(Producer, gen_stage:whereis(Producer)),

    Name = parity_whereis_test,
    {ok, Named} = gen_stage:start_link({local, Name}, counter, {producer, 0}, []),
    ?assertEqual(Named, gen_stage:whereis(Name)),
    ok = gen_stage:stop(Producer),
    ok = gen_stage:stop(Named).

%% ===== consumer_supervisor =====

test_consumer_supervisor_children_start_link(_Config) ->
    ChildSpec = #{
        id => test_worker,
        start => {test_worker, start_link, []},
        restart => temporary
    },
    {ok, Supervisor} = consumer_supervisor:start_link([ChildSpec], [{strategy, one_for_one}]),
    ?assert(is_process_alive(Supervisor)),
    ok = gen_stage:stop(Supervisor).

test_consumer_supervisor_rejects_permanent_template(_Config) ->
    ChildSpec = #{
        id => test_worker,
        start => {test_worker, start_link, []},
        restart => permanent
    },
    Msg = run_unlinked_start_link(fun() ->
                                      consumer_supervisor:start_link(
                                          [ChildSpec], [{strategy, one_for_one}])
                              end),
    {error, {bad_opts, M}} = Msg,
    ?assert(string:find(M, "permanent") =/= nomatch).

test_consumer_supervisor_requires_strategy(_Config) ->
    ChildSpec = #{
        id => test_worker,
        start => {test_worker, start_link, []},
        restart => temporary
    },
    Msg = run_unlinked_start_link(fun() ->
                                      consumer_supervisor:start_link([ChildSpec], [])
                              end),
    {error, {bad_opts, M}} = Msg,
    ?assert(string:find(M, "strategy") =/= nomatch).

%% @private
%% Run a failing start_link in an unlinked spawned process so that the
%% pending exit signal of the aborted server cannot leak into the
%% (exit-trapping) test process.
run_unlinked_start_link(Fun) ->
    Self = self(),
    spawn(fun() ->
                %% trap exits like the (trapping) test process: the aborted
                %% server's exit signal is flushed by proc_lib instead of
                %% killing this helper process
                process_flag(trap_exit, true),
                Result = try Fun() catch C:R -> {crashed, C, R} end,
                receive {'EXIT', _, _} -> ok after 0 -> ok end,
                Self ! {unlinked_start_link, Result}
        end),
    receive
        {unlinked_start_link, Result} -> Result
    after
        10000 -> erlang:exit(run_unlinked_start_link_timeout)
    end.

test_consumer_supervisor_which_children_shape(_Config) ->
    {ok, Producer} = counter:start_link({producer, 0}),
    ChildSpec = #{
        id => test_worker,
        start => {test_worker, start_link, []},
        restart => temporary
    },
    {ok, Supervisor} = test_consumer_supervisor:start_link(
        [ChildSpec],
        [{strategy, one_for_one}, {subscribe_to, [Producer]}, {max_demand, 10}]),

    timer:sleep(300),

    %% which_children returns a list of {undefined, PidOrRestarting, Type, Modules}
    Children = consumer_supervisor:which_children(Supervisor),
    ?assert(is_list(Children)),
    [begin
         ?assertMatch({undefined, _, worker, [_]}, Child)
     end || Child <- Children],

    ok = gen_stage:stop(Supervisor),
    ok = gen_stage:stop(Producer).

%% ===== Regression tests for porting bugs =====

%% Events returned from callbacks other than handle_demand/2 (here
%% handle_call/3) must also be held back while in accumulate mode.
test_accumulate_holds_dispatched_events(_Config) ->
    {ok, Producer} = counter:start_link({producer, self(), [{demand, accumulate}]}),
    {ok, Consumer} = forwarder:start_link({consumer, self()}),
    {ok, _} = gen_stage:sync_subscribe(Consumer, [{to, Producer}, {max_demand, 10}]),

    _ = counter:sync_queue(Producer, [a, b, c]),
    ?refuteReceive({consumed, _}),

    ok = gen_stage:demand(Producer, forward),
    ?assertReceive({consumed, [a, b, c]}),

    ok = gen_stage:stop(Consumer),
    ok = gen_stage:stop(Producer).

%% from_fun/1 must keep producing across several demand batches and
%% stop normally once the generator returns `done'.
test_from_fun_multiple_batches(_Config) ->
    Counter = counters:new(1, []),
    Fun = fun() ->
                  N = counters:get(Counter, 1) + 1,
                  counters:add(Counter, 1, 1),
                  case N =< 25 of
                      true -> {value, N};
                      false -> done
                  end
          end,
    {ok, Producer} = gen_stage:from_fun(Fun, [{link, false}]),
    ProducerRef = monitor(process, Producer),
    {ok, Consumer} = forwarder:start_link({consumer, self()}),
    {ok, _} = gen_stage:sync_subscribe(Consumer, [{to, Producer}, {max_demand, 10},
                                                  {min_demand, 5}, {cancel, transient}]),

    ?assertReceive({'DOWN', ProducerRef, process, Producer, normal}),
    ?assertEqual(lists:seq(1, 25), collect_consumed()),
    ok = gen_stage:stop(Consumer).

collect_consumed() ->
    receive
        {consumed, Events} -> Events ++ collect_consumed()
    after 200 -> []
    end.

%% sync_info/2 on a producer using the partition dispatcher must not crash
%% and the info must be delivered.
test_partition_dispatcher_sync_info(_Config) ->
    Dispatcher = {gen_stage_partition_dispatcher, [{partitions, 2},
                                                   {hash, fun(E) -> {E, E rem 2} end}]},
    {ok, Producer} = counter:start_link({producer, self(), [{dispatcher, Dispatcher}]}),
    {ok, C0} = forwarder:start_link({consumer, self()}),
    {ok, C1} = forwarder:start_link({consumer, self()}),
    {ok, _} = gen_stage:sync_subscribe(C0, [{to, Producer}, {partition, 0}, {max_demand, 10}]),
    {ok, _} = gen_stage:sync_subscribe(C1, [{to, Producer}, {partition, 1}, {max_demand, 10}]),

    _ = counter:sync_queue(Producer, [0, 1, 2, 3]),
    ?assertReceive({consumed, [0, 2]}),
    ?assertReceive({consumed, [1, 3]}),

    ok = gen_stage:sync_info(Producer, hello),
    ?assertReceive(hello),
    ?assert(is_process_alive(Producer)),

    ok = gen_stage:stop(C0),
    ok = gen_stage:stop(C1),
    ok = gen_stage:stop(Producer).

%% A transient child restarted by consumer_supervisor must receive the
%% same event it was originally started with.
test_consumer_supervisor_restart_keeps_event(_Config) ->
    {ok, Producer} = counter:start_link({producer, self()}),
    ChildSpec = #{id => crashing_worker,
                  start => {?MODULE, start_crashing_worker, [self()]},
                  restart => transient},
    {ok, Supervisor} = consumer_supervisor:start_link(
                         [ChildSpec],
                         [{strategy, one_for_one}, {max_restarts, 10},
                          {subscribe_to, [{Producer, [{max_demand, 1}]}]}]),

    _ = counter:sync_queue(Producer, [ev1]),
    ?assertReceive({worker_started, ev1, first}),
    ?assertReceive({worker_started, ev1, restarted}),

    ok = gen_stage:stop(Supervisor),
    ok = gen_stage:stop(Producer).

%% Crashes on its first run for a given event, stays alive when restarted.
start_crashing_worker(Parent, Event) ->
    Key = {?MODULE, crashed, Event},
    Pid = spawn_link(
            fun() ->
                    case persistent_term:get(Key, false) of
                        false ->
                            persistent_term:put(Key, true),
                            Parent ! {worker_started, Event, first},
                            exit(crash);
                        true ->
                            persistent_term:erase(Key),
                            Parent ! {worker_started, Event, restarted},
                            receive stop -> ok end
                    end
            end),
    {ok, Pid}.

%% In a producer_consumer (infinite buffer) an info queued right after the
%% last buffered event must be delivered together with that event, not
%% on the next demand.
test_producer_consumer_info_after_last_event(_Config) ->
    {ok, Producer} = counter:start_link({producer, self()}),
    {ok, PC} = doubler:start_link({producer_consumer, self()}),
    {ok, Consumer} = forwarder:start_link({consumer, self()}),
    {ok, _} = gen_stage:sync_subscribe(Consumer, [{to, PC}, {consumer_demand, manual}]),
    From = receive {consumer_subscribed, F} -> F after 5000 -> error(timeout) end,
    {ok, _} = gen_stage:sync_subscribe(PC, [{to, Producer}, {max_demand, 10}]),

    ok = forwarder:ask(Consumer, From, 2),
    _ = counter:sync_queue(Producer, [1, 2]),
    %% doubler emits [1,1,2,2]: two are delivered, two stay buffered
    ?assertReceive({consumed, [1, 1]}),

    %% queued as a permanent right after the buffered [2,2]
    ok = gen_stage:sync_info(PC, hello),
    ?refuteReceive(hello),

    %% asking exactly for the buffered events must also flush the info
    ok = forwarder:ask(Consumer, From, 2),
    ?assertReceive({consumed, [2, 2]}),
    ?assertReceive(hello),

    ok = gen_stage:stop(Consumer),
    ok = gen_stage:stop(PC),
    ok = gen_stage:stop(Producer).
