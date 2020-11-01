-module(consumer_supervisor).

-behaviour(gen_stage).

-export([
         start_link/2,
         start_link/4,
         start_child/2,
         terminate_child/2,
         wait_children/6,
         which_children/1,
         count_children/1,
         start_child/3,
         save_child/5
        ]).

-export([init/1, handle_subscribe/4, handle_cancel/3, handle_events/3, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type option() ::
{registry, atom()}
| {name, atom() | {global, term()} | {via, module(), term()}}
| {strategy, supervisor:strategy()}
| {max_restarts, non_neg_integer()}
| {max_records, non_neg_integer()}
| {subscribe_to, [gen_stage:stage() | {gen_stage:stage(), [{atom(), any()}]}]}.

-callback init(Args :: term()) ->
    {ok, [supervisor:child_spec()], Options :: [{atom(), any()}]}
    | ignore.

-record(state, {
          name,
          mod,
          args,
          template,
          max_restarts,
          max_seconds,
          strategy,
          children = #{},
          producers = #{},
          restarts = [],
          restarting = 0
         }).

-spec start_link(module(), any()) -> supervisor:on_start().
start_link(Mod, Args) ->
    gen_stage:start_link(?MODULE, {Mod, Args}, []).

-spec start_link(term(), module(), any(), [option()]) -> supervisor:on_start().
start_link(Name, Mod, Args, Opts) ->
    gen_stage:start_link(Name, ?MODULE, {Mod, Args}, Opts).

start_child(Supervisor, Args) when is_list(Args) ->
    call(Supervisor, {start_child, Args}).

terminate_child(Supervisor, Pid) when is_pid(Pid) ->
    call(Supervisor, {terminate_child, Pid}).

which_children(Supervisor) ->
    call(Supervisor, which_children).

count_children(Supervisor) ->
    call(Supervisor, count_children).

call(Supervisor, Req) ->
    gen_stage:call(Supervisor, Req, infinity).


%% Callbacks
init({Mod, Args, Name}) ->
    erlang:put('$initial_call', {supervisor, Mod, 1}),
    process_flag(trap_exit, true),

    case Mod:init(Args) of
        {ok, Children, Opts} ->
            RealName =
            case Name of
                undefined -> {self(), Mod};
                _ -> Name
            end,
            State = #state{mod = Mod, args = Args, name = RealName},
            case init(State, Children, Opts) of
                {ok, NewState, NewOpts} ->
                    {consumer, NewState, NewOpts};
                {error, Msg} ->
                    {stop, {bad_opts, Msg}}
            end;
        ignore ->
            ignore;
        Other ->
            {stop, {bad_return_value, Other}}
    end.

init(State, [Child], Opts) when is_list(Opts) ->
    {Strategy, Opts1} = stage_keyword:pop(Opts, strategy, undefined),
    {MaxRestarts, Opts2} = stage_keyword:pop(Opts1, max_restarts, 3),
    {MaxSeconds, Opts3} = stage_keyword:pop(Opts2, max_seconds, 5),
    Template = normalize_template(Child),
    NewState = State#state{
                 template = Template,
                 strategy = Strategy,
                 max_restarts = MaxRestarts,
                 max_seconds = MaxSeconds
                },
    {ok, NewState, Opts3};
init(_State, [_], _Opts) ->
    {error, "supervisor's init expects a keywords list as options"}.

handle_subscribe(producer, Opts, {_, Ref} = From, #state{producers = Producers} = State) ->
    Max = stage_keyword:get(Opts, max_demand, 1000),
    Min = stage_keyword:get(Opts, min_demand, Max div 2),
    gen_stage:ask(From, Max),
    NewProducers = maps:put(Ref, {From, 0, 0, Min, Max}, Producers),
    {manual, State#state{producers = NewProducers}}.

handle_cancel(_, {_, Ref}, #state{producers = Producers} = State) ->
    NewProducers = maps:remove(Ref, Producers),
    {noreply, [], State#state{producers = NewProducers}}.

handle_events(Events, {Pid, Ref} = From, State) ->
    #state{template = Child, children = Children} = State,
    {New, Errors} = start_events(Events, From, Child, 0, #{}, State),
    NewChildren = maps:merge(New, Children),
    Started = map_size(NewChildren) - map_size(Children),
    {noreply, [], maybe_ask(Ref, Pid, Started + Errors, Errors, NewChildren, State)}.

start_events([Extra | Extras], From, Child, Errors, Acc, State) ->
    {_, Ref} = From,
    {_, {M, F, Args}, Restart, _, _, _} = Child,
    NewArgs = Args ++ [Extra],
    case start_child(M, F, NewArgs) of
        {ok, Pid, _} when Restart =:= temporary ->
            NewAcc = maps:put(Pid, [Ref], Acc),
            start_events(Extras, From, Child, Errors, NewAcc, State);
        {ok, Pid, _} ->
            NewAcc = maps:put(Pid, [Ref | Args], Acc),
            start_events(Extras, From, Child, Errors, NewAcc, State);
        {ok, Pid} when Restart =:= temporary ->
            NewAcc = maps:put(Pid, [Ref], Acc),
            start_events(Extras, From, Child, Errors, NewAcc, State);
        {ok, Pid} ->
            NewAcc = maps:put(Pid, [Ref | Args], Acc),
            start_events(Extras, From, Child, Errors, NewAcc, State);
        ignore ->
            start_events(Extras, From, Child, Errors + 1, Acc, State);
        {error, Reason} ->
            error_logger:error_msg("consumer_supervisor failed to start child from: ~tp with reason: ~tp~n", [From, Reason]),
            report_error(start_error, Reason, undefined, Args, Child, State),
            start_events(Extras, From, Child, Errors + 1, Acc, State)
    end;
start_events([], _, _, Errors, Acc, _) ->
    {Acc, Errors}.

maybe_ask(Ref, Pid, Events, Down, Children, State) ->
    #state{producers = Producers} = State,
    case Producers of
        #{Ref := {To, Count, Pending, Min, Max}} ->
            if
                Count + Events > Max ->
                    error_logger:error_msg("consumer supervisor has received ~tp events in excess from: ~tp~n", [Count + Events - Max, {Pid, Ref}]);
                true -> ok
            end,
            NewPending =
            case Pending + Down of
                Ask when Ask >= Min ->
                    gen_stage:ask(To, Ask),
                    0;
                Ask ->
                    Ask
            end,
            NewCount = Count + Events - Down,
            NewProducers = maps:put(Ref, {To, NewCount, NewPending, Min, Max}, Producers),
            State#state{children = Children, producers = NewProducers};
        #{} ->
            State#state{children = Children}
    end.

handle_call(which_children, _From, State) ->
    #state{children = Children, template = Child} = State,
    {_, _, _, _, Type, Mods} = Child,
    Reply =
    lists:foreach(
      fun({Pid, Args}) ->
              MaybePid =
              case Args of
                  {restarting, _} -> restarting;
                  _ -> Pid
              end,
              {undefined, MaybePid, Type, Mods}
      end, Children),
    {reply, Reply, [], State};
handle_call(count_children, _From, State) ->
    #state{children = Children, template = Child, restarting = Restarting} = State,
    {_, _, _, _, Type, _} = Child,
    Specs = map_size(Children),
    Active = Specs - Restarting,
    Reply =
    case Type of
        supervisor ->
            #{specs => 1, active => Active, workers => 0, supervisrs => Specs};
        worker ->
            #{specs => 1, active => Active, workers => Specs, supervisors => 0}
    end,
    {reply, Reply, [], State};
handle_call({terminate_child, Pid}, _From, #state{children = Children} = State) ->
    case Children of
        #{Pid := [Producer | _] = Info} ->
            ok = terminate_children(#{Pid => Info}, State),
            {reply, ok, [], delete_child_and_maybe_ask(Producer, Pid, State)};
        #{Pid := {restarting, [Producer | _]} =Info} ->
            ok = terminate_children(#{Pid => Info}, State),
            {reply, ok, [], delete_child_and_maybe_ask(Producer, Pid, State)};
        #{} ->
            {reply, {error, not_found}, [], State}
    end;
handle_call({start_child, Extra}, _From, #state{template = Child} = State) ->
    handle_start_child(Child, Extra, State).

handle_start_child({_, {M, F, Args}, Restart, _, _, _}, Extra, State) ->
    NewArgs = Args ++ Extra,
    case start_child(M, F, NewArgs) of
        {ok, Pid, _} = Reply ->
            {reply, Reply, [], save_child(Restart, dynamic, Pid, NewArgs, State)};
        {ok, Pid} = Reply ->
            {reply, Reply, [], save_child(Restart, dynamic, Pid, NewArgs, State)};
        Reply ->
            {reply, Reply, [], State}
    end.

start_child(M, F, A) ->
    try apply(M, F, A) of
        {ok, Pid, Extra} when is_pid(Pid) -> {ok, Pid, Extra};
        {ok, Pid} when is_pid(Pid) -> {ok, Pid};
        ignore -> ignore;
        {error, _} = Err -> Err;
        Other -> {error, Other}
    catch
        Class:Exception:Stacktrace ->
            {error, exit_reason(Class, Exception, Stacktrace)}
    end.

save_child(temporary, Producer, Pid, _, #state{children = Children} = State) ->
    State#state{children = maps:put(Pid, [Producer], Children)};
save_child(_, Producer, Pid, Args, #state{children = Children} = State) ->
    State#state{children = maps:put(Pid, [Producer | Args], Children)}.

exit_reason(exit, Reason, _) -> Reason;
exit_reason(error, Reason, Stack) -> {Reason, Stack};
exit_reason(throw, Value, Stack) -> {{nocatch, Value}, Stack}.

handle_cast(_Msg, State) ->
    {noreply, [], State}.

handle_info({'EXIT', Pid, Reason}, State) ->
    case maybe_restart_child(Pid, Reason, State) of
        {ok, NewState} -> {noreply, [], NewState};
        {shutdown, NewState} -> {stop, shutdown, NewState}
    end;
handle_info({'$gen_restart', Pid}, State) ->
    #state{children = Children, template = Child, restarting = Restarting} = State,
    State1 = State#state{restarting = Restarting - 1},
    case Children of
        #{Pid := RestartingArgs} ->
            {restarting, [Producer | Args]} = RestartingArgs,
            case restart_child(Producer, Pid, Args, Child, State1) of
                {ok, State2} ->
                    {noreply, [], State2};
                {shutdown, State2} ->
                    {stop, shutdown, State2}
            end;
        #{} ->
            {noreply, [], State1}
    end;
handle_info(Msg, State) ->
    error_logger:error_msg("consumer supervisor received unexpected message: ~tp~n", [Msg]),
    {noreply, [], State}.

code_change(_, #state{mod = Mod, args = Args} = State, _) ->
    case Mod:init(Args) of
        {ok, Children, Opts} ->
            case init(State, Children, Opts) of
                {ok, NewState, _} -> {ok, NewState};
                {error, Reason} -> {error, {bad_opts, Reason}}
            end;
        ignore ->
            {ok, State};
        Err ->
            Err
    end.

terminate(_, #state{children = Children} = State) ->
    ok = terminate_children(Children, State).

terminate_children(Children, #state{template = Template} = State) ->
    {_, _, Restart, Shutdown, _, _} = Template,
    {Pids, Stacks} = monitor_children(Children, Restart),
    Size = map_size(Pids),
    Stacks =
    case Shutdown of
        brutal_kill ->
            maps:map(fun(Pid, _) -> exit(Pid, kill) end, Pids),
            wait_children(Restart, Shutdown, Pids, Size, undefined, Stacks);
        infinity ->
            maps:map(fun(Pid, _) -> exit(Pid, shutdown) end, Pids),
            wait_children(Restart, Shutdown, Pids, Size, undefined, Stacks);
        Time ->
            maps:map(fun(Pid, _) -> exit(Pid, shutdown) end, Pids),
            Timer = erlang:start_timer(Time, self(), kill),
            wait_children(Restart, Shutdown, Pids, Size, Timer, Stacks)
    end,
    lists:foreach(
      fun({Pid, Reason}) ->
              report_error(shutdown_error, Reason, Pid, undefined, Template, State)
      end, Stacks).

monitor_children(Children, Restart) ->
    lists:foldl(
      fun
          ({_, {restarting, _}}, {Pids, Stacks}) ->
              {Pids, Stacks};
      ({Pid, _}, {Pids, Stacks}) ->
              case monitor_child(Pid) of
                  ok ->
                      {maps:put(Pid, true, Pids), Stacks};
                  {error, normal} when Restart =/= permanent ->
                      {Pids, Stacks};
                  {error, Reason} ->
                      {Pids, maps:put(Pid, Reason, Stacks)}
              end
      end, {#{}, #{}}, Children).

monitor_child(Pid) ->
    Ref = erlang:monitor(process, Pid),
    unlink(Pid),
    receive
        {'EXIT', Pid, Reason} ->
            receive
                {'DOWN', Ref, process, Pid, _} -> {error, Reason}
            end
    after
        0 -> ok
    end.

wait_children(_Restart, _Shutdown, _Pids, 0, undefined, Stacks) ->
    Stacks;
wait_children(_Restart, _Shutdown, _Pids, 0, Timer, Stacks) ->
    erlang:cancel_timer(Timer),
    receive
        {timeout, Timer, kill} -> ok
    after
        0 -> ok
    end,
    Stacks;
wait_children(Restart, brutal_kill, Pids, Size, Timer, Stacks) ->
    receive
        {'DOWN', _Ref, process, Pid, killed} ->
            wait_children(Restart, brutal_kill, maps:remove(Pid, Pids), Size - 1, Timer, Stacks);
        {'DOWN', _Ref, process, Pid, Reason} ->
            wait_children(Restart, brutal_kill, maps:remove(Pid, Pids), Size - 1, Timer, maps:put(Pid, Reason, Stacks))
    end;
wait_children(Restart, Shutdown, Pids, Size, Timer, Stacks) ->
    receive
        {'DOWN', _Ref, process, Pid, {shutdown, _}} ->
            wait_children(Restart, Shutdown, maps:remove(Pid, Pids), Size - 1, Timer, Stacks);
        {'DOWN', _Ref, process, Pid, shutdown} ->
            wait_children(Restart, Shutdown, maps:remove(Pid, Pids), Size - 1, Timer, Stacks);
        {'DOWN', _Ref, process, Pid, normal} when Restart =/= permanent ->
            wait_children(Restart, Shutdown, maps:remove(Pid, Pids), Size - 1, Timer, Stacks);
        {'DOWN', _Ref, process, Pid, Reason} ->
            NewStacks = maps:put(Pid, Reason, Stacks),
            wait_children(Restart, Shutdown, maps:remove(Pid, Pids), Size - 1, Timer, NewStacks);
        {timeout, Timer, kill} ->
            [exit(Pid, kill) || {Pid, _} <- Pids],
            wait_children(Restart, Shutdown, Pids, Size, undefined, Stacks)
    end.

maybe_restart_child(Pid, Reason, State) ->
    #state{children = Children, template = Child} = State,
    {_, _, Restart, _, _, _} = Child,
    case Children of
        #{Pid := [Producer | Args]} ->
            maybe_restart_child(Restart, Reason, Producer, Pid, Args, Child, State);
        #{} ->
            {ok, State}
    end.

maybe_restart_child(permanent, Reason, Producer, Pid, Args, Child, State) ->
    report_error(child_terminated, Reason, Pid, Args, Child, State),
    restart_child(Producer, Pid, Args, Child, State);
maybe_restart_child(_, normal, Producer, Pid, _Args, _Child, State) ->
    {ok, delete_child_and_maybe_ask(Producer, Pid, State)};
maybe_restart_child(_, shutdown, Producer, Pid, _Args, _Child, State) ->
    {ok, delete_child_and_maybe_ask(Producer, Pid, State)};
maybe_restart_child(_, {shutdown, _}, Producer, Pid, _Args, _Child, State) ->
    {ok, delete_child_and_maybe_ask(Producer, Pid, State)};
maybe_restart_child(transient, Reason, Producer, Pid, Args, Child, State) ->
    report_error(child_terminated, Reason, Pid, Args, Child, State),
    restart_child(Producer, Pid, Args, Child, State);
maybe_restart_child(temporary, Reason, Producer, Pid, Args, Child, State) ->
    report_error(child_terminated, Reason, Pid, Args, Child, State),
    {ok, delete_child_and_maybe_ask(Producer, Pid, State)}.

delete_child_and_maybe_ask(dynamic, Pid, #state{children = Children} = State) ->
    State#state{children = maps:remove(Pid, Children)};
delete_child_and_maybe_ask(Ref, Pid, #state{children = Children} = State) ->
    maybe_ask(Ref, Pid, 0, 1, maps:remove(Pid, Children), State).

restart_child(Producer, Pid, Args, Child, State) ->
    case add_restart(State) of
        {ok, #state{strategy = Strategy} = State1} ->
            case restart_child(Strategy, Producer, Pid, Args, Child, State1) of
                {ok, State2} ->
                    {ok, State2};
                {try_again, State2} ->
                    erlang:send(self(), {'$gen_restart', Pid}),
                    {ok, State2}
            end;
        {shutdown, State1} ->
            report_error(shutdown, reached_max_restart_intensity, Pid, Args, Child, State1),
            {shutdown, delete_child_and_maybe_ask(Producer, Pid, State1)}
    end.

add_restart(State) ->
    #state{max_seconds = MaxSeconds, max_restarts = MaxRestarts, restarts = Restarts} = State,
    Now = erlang:monotonic_time(1),
    NewRestarts = add_restart([Now | Restarts], Now, MaxSeconds),
    NewState = State#state{restarts = NewRestarts},
    if
        length(NewRestarts) =< MaxRestarts ->
            {ok, NewState};
        true ->
            {shutdown, NewState}
    end.

add_restart(Restarts, Now, Period) ->
    [Restart || Restart <- Restarts, Now =< Restart + Period].

restart_child(one_for_one, Producer, CurrentPid, Args, Child, State) ->
    {_, {M, F, _}, Restart, _, _, _} = Child,
    case start_child(M, F, Args) of
        {ok, Pid, _} ->
            Children = State#state.children,
            NewState = State#state{children = maps:remove(CurrentPid, Children)},
            {ok, save_child(Restart, Producer, Pid, Args, NewState)};
        {ok, Pid} ->
            Children = State#state.children,
            NewState = State#state{children = maps:remove(CurrentPid, Children)},
            {ok, save_child(Restart, Producer, Pid, Args, NewState)};
        ignore ->
            {ok, delete_child_and_maybe_ask(Producer, CurrentPid, State)};
        {error, Reason} ->
            report_error(start_error, Reason, {restarting, CurrentPid}, Args, Child, State),
            NewState = restart_child(CurrentPid, State),
            {try_again, NewState#state{restarting = NewState#state.restarting + 1}}
    end.

restart_child(Pid, #state{children = Children} = State) ->
    case Children of
        #{Pid := {restarting, _}} ->
            State;
        #{Pid := Info} ->
            NewChildren = maps:put(Pid, {restarting, Info}, Children),
            State#state{children = NewChildren}
    end.

report_error(Error, Reason, Pid, Args, Child, #state{name = Name}) ->
    error_logger:error_report([
                            supervisor_report,
                            {supervisor, Name},
                            {errorContext, Error},
                            {reason, Reason},
                            {offender, extract_child(Pid, Args, Child)}
                           ]).

extract_child(Pid, Args, {Id, {M, F, _}, Restart, Shutdown, Type, _}) ->
    [
     {pid, Pid},
     {id, Id},
     {mfargs, {M, F, Args}},
     {restart_type, Restart},
     {shutdown, Shutdown},
     {child_type, Type}
    ].

normalize_template(#{id := Id, start := {M, _, _} = Start} = Child) ->
    {
     Id,
     Start,
     maps:get(restart, Child, permanent),
     maps:get(shutdown, Child, 5000),
     maps:get(type, Child, worker),
     maps:get(modules, Child, [M])
    };
normalize_template({_, _, _, _, _, _} = Child) ->
    Child.

