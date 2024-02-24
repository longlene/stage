%% @doc A supervisor that starts children as events flow in.
%%
%% A <c>consumer_supervisor</c> can be used as the consumer in a `gen_stage' pipeline.
%% A new child process will be started per event, where the event is appended
%% to the arguments in the child specification.
%%
%% A `consumer_supervisor' can be attached to a producer by returning
%% `subscribe_to' from `init/1' or explicitly with `gen_stage:sync_subscribe/3'
%% and `gen_stage:async_subscribe/2'.
%%
%% Once subscribed, the supervisor will ask the producer for `max_demand' events
%% and start child processes as evets arrive. As child processes terminate, the
%% supervisor will aaccumulate demand and request more events once `min_demand'
%% is reached. This allows the `consumer_supervisor' to work similar to a pool,
%% except a child process is started per event. The minimum amout of concurrent
%% children per producer is specified by `min_demand' and maximum is given
%% by `max_demand'.

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

%% gen_stage callbacks
-export([init/1, handle_subscribe/4, handle_cancel/3, handle_events/3, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Options used by the `start*` functions
-type option() ::
{registry, atom()}
| {strategy, supervisor:strategy()}
| {max_restarts, non_neg_integer()}
| {max_records, non_neg_integer()}
| {subscribe_to, [gen_stage:stage() | {gen_stage:stage(), [{atom(), any()}]}]}.

%% Callback invoked to start the supervisor and during hot code upgrades.
%%
%% Options
%% * `strategy' - the restart strategy optin. Only `one_for_one'
%%   is supported by consumer supervisors.
%% * `max_restarts' - the maximum amount of restarts allowed in
%%   a time frame. Defaults to 3 times.
%% * `max_seconds' - the time frame in which `max_restarts` applies
%%   in seconds. Defaults to 5 seconds.
%% * `subscribe_to' - a list of producers to subscribe to. Each element
%%   represents the producer or a tuple with the producer and the subscription
%%   options, for example, `[producer]' or `[{producer, [{max_demand, 20}, {min_demand, 10}]}]'.
-callback init(Args :: term()) ->
    {ok, [supervisor:child_spec()], Options :: [{atom(), any()}]}
    | ignore.

-record(state, {
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

-type sup_ref() :: (Name :: atom())
| {Name :: atom(), Node :: node()}
| {global, Name :: term()}
| {via, Module :: module(), Name :: any()}
| pid().

%% @doc Starts a supervisor with the given children.
%%
%% A strategy is required to be given as an option. Furthermore,
%% the max_restarts, max_seconds, and subscribe_to values
%% can be configured as described in the documentation for the
%% init/1 callback.
%%
%% The options can also be used to register a supervisor name.
%% The supported values are described under the "Name Registration"
%% section in the gen_server module docs.
%% The child processes specified in children will be started by appending
%% the event to process to the existing function arguments in the child specification.
%%
%% Note that the consumer supervisor is linked to the parent process
%% and will exit not only on crashes but also if the parent process
%% exits with normal reason.
-spec start_link(module(), any()) -> supervisor:startlink_ret().
start_link(Mod, Args) ->
    gen_stage:start_link(?MODULE, {Mod, Args}, []).

-spec start_link(term(), module(), any(), [option()]) -> supervisor:startlink_ret().
start_link(Name, Mod, Args, Opts) ->
    gen_stage:start_link(Name, ?MODULE, {Mod, Args}, Opts).

%% @doc Starts a child in the consumer supervisor.
%%
%% The child process will be started by appending  the given list of
%% `Args' to the existing function arguments in the child specification.
%%
%% This child is started separately from any producer and does not
%% count towards the demand of any of them.
%%
%% If the child process starts, function return `{ok, Child}' or
%% `{ok, Child, Info}', the pid is added to the supervisor, and the
%% function returns the same value.
%%
%% If the child process start function returns `ignore', an error tuple,
%% or an erroneous value, or if it fails, the child is discarded and
%% `ignore' or `{error, Reason}' where `Reason' is a term containing
%% information about the error is returned.
-spec start_child(sup_ref(), [term()]) -> supervisor:startchild_ret().
start_child(Supervisor, Args) when is_list(Args) ->
    call(Supervisor, {start_child, Args}).

%% @doc Terminates the given child pid.
%%
%% If successful, the function returns `ok'. If there is no
%% such pid, the function returns `{error, not_found}'.
-spec terminate_child(sup_ref(), pid()) -> ok | {error, not_found}.
terminate_child(Supervisor, Pid) when is_pid(Pid) ->
    call(Supervisor, {terminate_child, Pid}).

-spec which_children(sup_ref()) -> [{undefined, pid() | restarting, worker | supervisor, dynamic | [module()]}].
which_children(Supervisor) ->
    call(Supervisor, which_children).

-spec count_children(sup_ref()) -> #{specs => non_neg_integer(), active => non_neg_integer(), supervisors => non_neg_integer(), workers => non_neg_integer()}.
count_children(Supervisor) ->
    call(Supervisor, count_children).

call(Supervisor, Req) ->
    gen_stage:call(Supervisor, Req, infinity).

%% Callbacks
init({Mod, Args}) ->
    erlang:put('$initial_call', {supervisor, Mod, 1}),
    process_flag(trap_exit, true),

    case Mod:init(Args) of
        {ok, Children, Opts} ->
            State = #state{mod = Mod, args = Args},
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
    Strategy = proplists:get_value(strategy, Opts),
    MaxRestarts = proplists:get_value(max_restarts, Opts, 3),
    MaxSeconds = proplists:get_value(max_seconds, Opts, 5),
    NewOpts =
        lists:foldl(
            fun(Key, Acc) ->
                    proplists:delete(Key, Acc)
            end, Opts, [strategy, max_restarts, max_seconds]),
    Template = normalize_template(Child),
    NewState = State#state{
                 template = Template,
                 strategy = Strategy,
                 max_restarts = MaxRestarts,
                 max_seconds = MaxSeconds
                },
    {ok, NewState, NewOpts};
init(_State, [_], _Opts) ->
    {error, "supervisor's init expects a list as options"}.

handle_subscribe(producer, Opts, {_, Ref} = From, #state{producers = Producers} = State) ->
    Max = proplists:get_value(max_demand, Opts, 1000),
    Min = proplists:get_value(min_demand, Opts, Max div 2),
    gen_stage:ask(From, Max),
    NewProducers = Producers#{Ref => {From, 0, 0, Min, Max}},
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
            NewAcc = Acc#{Pid => [Ref]},
            start_events(Extras, From, Child, Errors, NewAcc, State);
        {ok, Pid, _} ->
            NewAcc = Acc#{Pid => [Ref | Args]},
            start_events(Extras, From, Child, Errors, NewAcc, State);
        {ok, Pid} when Restart =:= temporary ->
            NewAcc = Acc#{Pid => [Ref]},
            start_events(Extras, From, Child, Errors, NewAcc, State);
        {ok, Pid} ->
            NewAcc = Acc#{Pid => [Ref | Args]},
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
    maps:map(
      fun(Pid, Args) ->
              MaybePid =
              case Args of
                  {restarting, _} -> restarting;
                  _ -> Pid
              end,
              {undefined, MaybePid, Type, Mods}
      end, Children),
    {reply, maps:to_list(Reply), [], State};
handle_call(count_children, _From, State) ->
    #state{children = Children, template = Child, restarting = Restarting} = State,
    {_, _, _, _, Type, _} = Child,
    Specs = map_size(Children),
    Active = Specs - Restarting,
    Reply =
    case Type of
        supervisor -> #{specs => 1, active => Active, workers => 0, supervisrs => Specs};
        worker -> #{specs => 1, active => Active, workers => Specs, supervisors => 0}
    end,
    {reply, Reply, [], State};
handle_call({terminate_child, Pid}, _From, #state{children = Children} = State) ->
    case Children of
        #{Pid := [Producer | _] = Info} ->
            ok = terminate_children(#{Pid => Info}, State),
            {reply, ok, [], delete_child_and_maybe_ask(Producer, Pid, State)};
        #{Pid := {restarting, [Producer | _]} = Info} ->
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
    State#state{children = Children#{Pid => [Producer]}};
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
            maps:map(
              fun(Pid, _) ->
                      exit(Pid, kill)
              end, Pids),
            wait_children(Restart, Shutdown, Pids, Size, undefined, Stacks);
        infinity ->
            maps:map(
              fun(Pid, _) ->
                      exit(Pid, shutdown)
              end, Pids),
            wait_children(Restart, Shutdown, Pids, Size, undefined, Stacks);
        Time ->
            maps:map(
              fun(Pid, _) ->
                      exit(Pid, shutdown)
              end, Pids),
            Timer = erlang:start_timer(Time, self(), kill),
            wait_children(Restart, Shutdown, Pids, Size, Timer, Stacks)
    end,
    lists:foreach(
      fun({Pid, Reason}) ->
              report_error(shutdown_error, Reason, Pid, undefined, Template, State)
      end, Stacks),
    ok.

monitor_children(Children, Restart) ->
    maps:fold(
      fun
          (_, {restarting, _}, {Pids, Stacks}) ->
              {Pids, Stacks};
          (Pid, _, {Pids, Stacks}) ->
              case monitor_child(Pid) of
                  ok ->
                      {Pids#{Pid => true}, Stacks};
                  {error, normal} when Restart =/= permanent ->
                      {Pids, Stacks};
                  {error, Reason} ->
                      {Pids, Stacks#{Pid => Reason}}
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
            State#state{children = Children#{Pid => {restarting, Info}}}
    end.

report_error(Error, Reason, Pid, Args, Child, _State) ->
    error_logger:error_report([
                            supervisor_report,
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

