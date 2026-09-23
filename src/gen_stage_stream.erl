%% @doc
%% Subscribes the current process to one or more gen_stage producers and
%% delivers their events to this process' mailbox, the Erlang
%% equivalent of Elixir's `GenStage.stream/1'.
%%
%% Like the Elixir stream, this module "hijacks" the inbox of the calling
%% process while the stream is open: it guarantees it will not leave
%% unwanted messages in the mailbox after `close/1' (unless a producer
%% comes from a remote node).
%%
%% ## Example
%%
%%     {ok, Stream} = gen_stage_stream:subscribe([{Producer, max_demand: 100}]),
%%     loop(Stream).
%%
%% loop({MonitorPid, MonitorRef, Subscriptions}) ->
%%     receive
%%         {'$gen_consumer', {Pid, {MonitorRef, InnerRef}} = From, Events}
%%           when is_list(Events) ->
%%             %% ... process Events ...
%%             gen_stage:ask(From, 50),  %% ask for more
%%             loop({MonitorPid, MonitorRef, Subscriptions});
%%         {MonitorRef, {DOWN, _InnerRef, _Reason}} ->
%%             ok  %% a producer went down
%%     end.
%%
%% When done:
%%
%%     gen_stage_stream:close(Stream)
%%
%% ## Messages received by the caller
%%
%%   * `{'$gen_consumer', {Producer, {MonitorRef, InnerRef}}, Events}` -
%%     a batch of events from `Producer'. The tuple
%%     `{Producer, {MonitorRef, InnerRef}}` is the `From' to be used with
%%     `gen_stage:ask/3' (to ask for more events) and `gen_stage:cancel/3'.
%%   * `{'$gen_consumer', {Producer, {MonitorRef, InnerRef}},
%%     {cancel, Reason}}` - the producer cancelled the subscription.
%%   * `{MonitorRef, {DOWN, InnerRef, Reason}}` - a producer went down.
%%
%% If a producer process exits, the stream reacts according to the
%% `cancel' subscription option (`permanent', the default, makes the
%% calling process exit with the same reason; `transient' only for
%% abnormal exits; `temporary' never).
%% @end
-module(gen_stage_stream).

-export([subscribe/1, subscribe/2, close/1, close/2, ask/2, ask/3]).

-type stream() :: {pid(), reference(), #{reference() => subscription()}}. %% The type returned by `subscribe/2'.

-type subscription() ::
      {subscribed, pid(), cancel_mode(), non_neg_integer(), non_neg_integer(), non_neg_integer()}
    | {cancel, pid()}.

-type cancel_mode() :: permanent | transient | temporary.

-export_type([stream/0, subscription/0, cancel_mode/0]).

%% @doc
%% Subscribes the current process to the given producers with default
%% options. See `subscribe/2'.
%% @end
-spec subscribe([gen_stage:stage() | {gen_stage:stage(), gen_stage:subscription_options()}]) ->
    {ok, stream()}.
subscribe(Subscriptions) ->
    subscribe(Subscriptions, []).

%% @doc
%% Subscribes the current process to the given producers.
%%
%% `Subscriptions' is a list of producers or `{Producer, Opts}' tuples,
%% where `Opts' are the subscription options of `gen_stage:sync_subscribe/3'
%% (`max_demand', `min_demand', `cancel', ...).
%%
%% Options:
%%
%%   * `demand` - sets the demand mode of the producers to `forward' or
%%     `accumulate' after subscription. Defaults to `forward' so the
%%     stream can receive items.
%%   * `producers` - the processes to set the demand mode on
%%     initialization. Defaults to the processes being subscribed to.
%%     Sometimes the stream subscribes to a `producer_consumer' instead of
%%     a `producer'; in such cases set this option to an empty list or to
%%     the list of actual producers so their demand is properly set.
%%
%% If the caller process exits before calling `close/1', the helper
%% process exits with it and the producers clean up their subscriptions
%% via monitoring.
%% @end
-spec subscribe([gen_stage:stage() | {gen_stage:stage(), gen_stage:subscription_options()}],
                [{atom(), any()}]) -> {ok, stream()}.
subscribe(Subscriptions, Opts) when is_list(Subscriptions), is_list(Opts) ->
    Parent = self(),
    Demand = proplists:get_value(demand, Opts, forward),
    ProducersOpt = proplists:get_value(producers, Opts),
    Pairs = [validate_opts(S) || S <- Subscriptions],
    MonitorPid = spawn(fun() -> init_monitor(Parent, Demand) end),
    MonitorRef = erlang:monitor(process, MonitorPid),
    erlang:send(MonitorPid, {Parent, MonitorRef}),
    erlang:send(MonitorPid, {MonitorRef, {subscribe, Pairs}}),
    receive
        {'DOWN', MonitorRef, process, _, Reason} ->
            %% The monitor process died before the subscriptions were
            %% established (e.g. a producer was not alive).
            erlang:exit(Reason);
        {MonitorRef, {subscriptions, Subscriptions1}} ->
            set_demand(Demand, ProducersOpt, Subscriptions1),
            {ok, {MonitorPid, MonitorRef, Subscriptions1}}
    end.

%% @doc
%% Cancels all subscriptions of the stream and kills the helper process.
%%
%% In-flight events may still arrive after this call; the call drains
%% the caller's mailbox of all messages belonging to the stream before
%% returning. Pass a `Timeout' to stop draining early.
%% @end
-spec close(stream()) -> ok.
close(Stream) ->
    close(Stream, infinity).

-spec close(stream(), timeout()) -> ok.
close({MonitorPid, MonitorRef, Subscriptions}, Timeout) ->
    Subscriptions1 = request_cancels(MonitorRef, Subscriptions),
    drain(MonitorRef, Subscriptions1, Timeout),
    cancel_monitor(MonitorPid, MonitorRef),
    ok.

%% @doc
%% Asks the producer of a subscription for more events. `From' is the
%% `{Producer, {MonitorRef, InnerRef}}' tuple received with the events.
%% @end
-spec ask(gen_stage:from(), non_neg_integer()) -> ok | noconnect | nosuspend.
ask(From, Demand) ->
    gen_stage:ask(From, Demand).

-spec ask(gen_stage:from(), non_neg_integer(), [noconnect | nosuspend]) -> ok | noconnect | nosuspend.
ask(From, Demand, Opts) ->
    gen_stage:ask(From, Demand, Opts).

%% @private
%% Validate a subscription entry, like Elixir's stream_validate_opts/1.
validate_opts(To) when not is_tuple(To) ->
    validate_opts({To, []});
validate_opts({To, Opts}) when is_list(Opts) ->
    case gen_stage_utils:validate_integer(Opts, max_demand, 1000, 1, infinity, false) of
        {ok, Max, _} ->
            case gen_stage_utils:validate_integer(Opts, min_demand, Max div 2, 0, Max - 1, false) of
                {ok, Min, _} ->
                    case gen_stage_utils:validate_in(Opts, cancel, permanent,
                                                    [temporary, transient, permanent]) of
                        {ok, Cancel, _} ->
                            {To, Cancel, Min, Max, Opts};
                        {error, Message} ->
                            error(io_lib:format("invalid options for ~p producer (~s)", [To, Message]))
                    end;
                {error, Message} ->
                    error(io_lib:format("invalid options for ~p producer (~s)", [To, Message]))
            end;
        {error, Message} ->
            error(io_lib:format("invalid options for ~p producer (~s)", [To, Message]))
    end.

%% @private
set_demand(Demand, Producers, Subscriptions) ->
    case Producers of
        undefined ->
            [gen_stage:demand(Pid, Demand) ||
                {_, {subscribed, Pid, _C, _Min, _Max, _D}} <- maps:to_list(Subscriptions)];
        ProducersList when is_list(ProducersList) ->
            [gen_stage:demand(P, Demand) || P <- ProducersList];
        _ ->
            ok
    end.

%% @private
init_monitor(Parent, Demand) ->
    ParentRef = erlang:monitor(process, Parent),
    receive
        {'DOWN', ParentRef, process, _, Reason} ->
            erlang:exit(Reason);
        {Parent, MonitorRef} ->
            loop_monitor(Parent, ParentRef, MonitorRef, Demand, [])
    end.

%% @private
loop_monitor(Parent, ParentRef, MonitorRef, _Demand, Keys) ->
    receive
        {MonitorRef, {subscribe, Pairs}} ->
            Subscriptions = subscriptions_monitor(Parent, MonitorRef, Pairs),
            erlang:send(Parent, {MonitorRef, {subscriptions, Subscriptions}}),
            loop_monitor(Parent, ParentRef, MonitorRef, _Demand,
                         maps:keys(Subscriptions) ++ Keys);
        {'DOWN', ParentRef, process, _, Reason} ->
            erlang:exit(Reason);
        {'DOWN', Ref, process, _, Reason} ->
            case lists:member(Ref, Keys) of
                true ->
                    erlang:send(Parent, {MonitorRef, {'DOWN', Ref, Reason}});
                false ->
                    ok
            end,
            loop_monitor(Parent, ParentRef, MonitorRef, _Demand,
                         [K || K <- Keys, K =/= Ref])
    end.

%% @private
%% Subscribe to each producer on behalf of the parent process. The
%% producers monitor the parent (the consumer in `From'), so if the
%% parent exits the producers clean up on their own.
subscriptions_monitor(Parent, MonitorRef, Pairs) ->
    lists:foldl(
      fun({To, Cancel, Min, Max, Opts}, Acc) ->
              case gen_stage:whereis(To) of
                  undefined when Cancel =/= temporary ->
                      erlang:exit({noproc, {gen_stage_stream, subscribe, [Pairs]}});
                  undefined ->
                      %% cancel: temporary - just skip
                      Acc;
                  ProducerPid ->
                      InnerRef = erlang:monitor(process, ProducerPid),
                      From = {Parent, {MonitorRef, InnerRef}},
                      erlang:send(ProducerPid,
                                  {'$gen_producer', From, {subscribe, undefined, Opts}},
                                  [noconnect]),
                      erlang:send(ProducerPid, {'$gen_producer', From, {ask, Max}}, [noconnect]),
                      maps:put(InnerRef, {subscribed, ProducerPid, Cancel, Min, Max, Max}, Acc)
              end
      end, #{}, Pairs).

%% @private
request_cancels(MonitorRef, Subscriptions) ->
    maps:fold(
      fun(InnerRef, {subscribed, Pid, _C, _Min, _Max, _D}, Acc) ->
              gen_stage:cancel({Pid, {MonitorRef, InnerRef}}, normal, [noconnect]),
              maps:put(InnerRef, {cancel, Pid}, Acc);
         (_, _Entry, Acc) ->
              Acc
      end, Subscriptions, Subscriptions).

%% @private
%% Drain all messages belonging to the stream from the caller's mailbox
%% until every subscription has been cancelled (or the timeout elapses).
%% Cancels received while draining do not stop the caller process, as
%% the stream is being closed explicitly.
drain(_MonitorRef, Subscriptions, _Timeout) when map_size(Subscriptions) =:= 0 ->
    ok;
drain(MonitorRef, Subscriptions, Timeout) ->
    receive
        {'$gen_consumer', {ProducerPid, {MonitorRef, InnerRef}}, Events} when is_list(Events) ->
            case Subscriptions of
                #{InnerRef := {subscribed, ProducerPid, Cancel, Min, Max, Demand}} ->
                    From = {ProducerPid, {MonitorRef, InnerRef}},
                    {NewDemand, _Batches} =
                        gen_stage_utils:split_batches(Events, From, Min, Max, Demand),
                    NewSubs = maps:put(InnerRef, {subscribed, ProducerPid, Cancel, Min, Max, NewDemand}, Subscriptions),
                    drain(MonitorRef, NewSubs, Timeout);
                #{InnerRef := {cancel, _}} ->
                    %% We received this message before the cancellation was processed.
                    drain(MonitorRef, Subscriptions, Timeout);
                _ ->
                    %% Out of order or unknown subscription - cancel it.
                    gen_stage:cancel({ProducerPid, {MonitorRef, InnerRef}},
                                     unknown_subscription, [noconnect]),
                    drain(MonitorRef, maps:remove(InnerRef, Subscriptions), Timeout)
            end;
        {'$gen_consumer', {_, {MonitorRef, InnerRef}}, {cancel, _Reason}} ->
            erlang:demonitor(InnerRef, [flush]),
            drain(MonitorRef, maps:remove(InnerRef, Subscriptions), Timeout);
        {MonitorRef, {'DOWN', InnerRef, _Reason}} ->
            erlang:demonitor(InnerRef, [flush]),
            drain(MonitorRef, maps:remove(InnerRef, Subscriptions), Timeout)
    after
        Timeout -> ok
    end.

%% @private
cancel_monitor(MonitorPid, MonitorRef) ->
    erlang:demonitor(MonitorRef, [flush]),
    Ref = erlang:monitor(process, MonitorPid),
    exit(MonitorPid, kill),
    receive
        {'DOWN', Ref, process, MonitorPid, _} ->
            flush_monitor(MonitorRef)
    end.

%% @private
flush_monitor(MonitorRef) ->
    receive
        {MonitorRef, _} ->
            flush_monitor(MonitorRef)
    after
        0 -> ok
    end.
