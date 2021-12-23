%% @doc
%% Stages are data-exchange steps that send and/or receive data
%% from other stages.
%%
%% When a stage sends data, it acts as a producer. When it receives
%% data, it acts as a consumer. Stages may take both producer and
%% consumer roles at once.
%%
%% ## Stage types
%%
%% Besieds taking both producer and consumer roles, a stage may be
%% called "source" if it only produces items or called "sink" if it
%% only cousumes items.
%%
%% For example, imagine the stages below where A sends data to B
%% that sends data to C:
%%
%%     [A] -> [B] -> [C]
%%
%% we conclude that:
%%
%%   * A is only a producer (and therefore a source)
%%   * B is both producer and consumer
%%   * C is only a consumer (and therefore a sink)
%%
%% As we will see in the upcoming Examples section, we must
%% specify the type of the stage when we implement each of them.
%%
%% To start the flow of events, we subscribe consumers to
%% producers. Once the communication channel between them is
%% established, consumers will ask the producers for events.
%% We typically say the consumer is sending demand upstream.
%% Once demand arrives, the producer will emit items, never
%% emitting more items than the consumer asked for. This provides
%% a back-pressure mechanism.
%%
%% A consumer may have multiple producers and a producer may have
%% multiple consumers. When a consumer asks for data, each producer
%% is handled separately, with its own demand. When a producer
%% receives demand and sends data to multiple consumers, the demand
%% is tracked and the events are sent by a dispatcher. This allows
%% producers to send data using different "strategies". See
%% gen_stage_dispatcher for more information.
%%
%% Many developers tend to create layers of stages, such as A, B and
%% C, for achieving concurrency. If all you want is concurrency, starting
%% multiple instances of the same stage is enough. Layers in gen_stage
%% must be created when there is a need for back-pressure or to route the
%% data in different ways.
%%
%% For example, if you need the data to go over multiple steps but
%% without a need for back-pressure or without a need to break the
%% data apart, do not design it as such:
%%
%%     [Producer] -> [Step 1] -> [Step 2] -> [Step 3]
%%
%% Instead it is better to design it as:
%%
%%                   [Consumer]
%%                  /
%%     [Producer]-&lt;-[Consumer]
%%                  \
%%                   [Consumer]
%%
%% where "Consumer" are multiple processes running the same code that
%% subscribe to the same "Producer".
%% @end
-module(gen_stage).

-behaviour(gen_server).

-export([
         start_link/2,
         start_link/3,
         start_link/4,
         start/2,
         start/3,
         start/4,
         sync_info/2,
         sync_info/3,
         async_info/2,
         demand/1,
         demand/2,
         sync_subscribe/2,
         sync_subscribe/3,
         sync_resubscribe/4,
         sync_resubscribe/5,
         async_subscribe/2,
         async_resubscribe/4,
         ask/2,
         ask/3,
         cancel/2,
         cancel/3,
         call/2,
         call/3,
         cast/2,
         reply/2,
         stop/1,
         stop/3,
         estimate_buffered_count/1,
         estimate_buffered_count/2,

         consumer_receive/4,
         consumer_subscribe/4
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TIMEOUT, 5000).

-record(stage, {
          mod,
          state,
          type,
          dispatcher_mod,
          dispatcher_state,
          buffer,
          buffer_keep,
          events = forward,
          monitors = #{},
          producers = #{},
          consumers = #{}
         }).

% gen_server type
-type server() :: pid() | atom() | {global, term()} | {via, module(), term()} | {atom(), node()}.

-type on_start() :: {ok, pid()} | ignore | {error, {already_started, pid()} | term()}.

-type name() :: {local, atom()} | {global, term()} | {via, module(), term()}.

-type options() :: [option()].
-type option() ::
{debug, [debug()]}
| {timeout, timeout()}
| {spawn_opt, [term()]}
| {hibernate_after, timeout()}.

-type debug() :: trace | log | statistics | {log_to_file, iolist() | binary()}.

%-type type() :: producer | consumer | producer_consumer.

-type subscription_option() ::
{cancel, permanent | transient | temporary}
| {to, server()}
| {min_demand, integer()}
| {max_demand, integer()}
| {atom(), term()}.

-type subscription_options() :: [subscription_option()].

-type producer_only_option() :: {demand, forward | accumulate}.

-type producer_and_producer_consumer_option() ::
{buffer_size, non_neg_integer() | infinity}
| {buffer_keep, first | last}
| {dispatcher, module() | {module(), gen_stage_dispatcher:options()}}.

-type consumer_and_producer_consumer_option() ::
{subscribe_to, [module() | {module(), subscription_options()}]}.

-type producer_option() :: producer_only_option() | producer_and_producer_consumer_option().

-type consumer_option() :: consumer_and_producer_consumer_option().

-type producer_consumer_option() ::
producer_and_producer_consumer_option() | consumer_and_producer_consumer_option().

-type stage() :: pid() | atom() | {global, term()} | {via, module(), term()} | {atom(), node()}.

-type subscription_tag() :: reference().

-type from() :: {pid(), subscription_tag()}.

%% @doc
%% Invoked when the server is started.
%%
%% `start_link/3' (or `start/3') will block until this callback returns.
%% `Args' is the argument term (second argument) passed to `start_link/3'
%% (or `start/3').
%%
%% In case of successful start, this callback must return a tuple
%% where the first element is the stage type, which is one of:
%%   * `producer'
%%   * `consumer'
%%   * `producer_consumer' (if the stage is acting as both)
%%
%% The returned tuple may also contain 3 or 4 elements. The third
%% element may be the `hibernate' atom or a set of options defined
%% below.
%%
%% Returning `ignore` will cause `start_link/3` to return `ignore`
%% and the process will exit normally without entering the loop or
%% calling `terminate/2`.
%%
%% Returning `{stop, Reason}` will cause `start_link/3` to return
%% `{error, Reason}` and the process to exit with reason `reason`
%% without entering the loop or calling `terminate/2`.
%% @end
-callback init(Args :: term()) -> 
    {producer, State}
    | {producer, State, [producer_option()]}
    | {producer_consumer, State}
    | {producer_consumer, State, [producer_consumer_option()]}
    | {consumer, State}
    | {consumer, State, [consumer_option()]}
    | ignore
    | {stop, Reason :: any()}
      when State :: any().

%% @doc
%% Invoked on `producer` stages.
%%
%% This callback is invoked on `producer` stages with the demand from
%% consumers/dispatcher. The producer that implements this callback must
%% either store the demand, or return the amount of requested events.
%%
%% Must always be explicitly implemented by `producer` stages.
%% @end
-callback handle_demand(Demand :: pos_integer(), State :: term()) ->
    {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason, NewState}
      when NewState :: term(), Reason :: term(), Event :: term().

%% @doc
%% Invoked when a consumer subscribes to a producer.
%%
%% This callback is invoked in both producers and consumers.
%% `producer_or_consumer` will be `producer` when this callback is
%% invoked on a consumer that subscribed to a producer, and `consumer`
%% if when this callback is invoked on producers a consumer subscribed
%% to.
%% @end
-callback handle_subscribe(Type :: producer | consumer, subscription_options(), from(), State :: term()) ->
    {automatic | manual, NewState}
    | {stop, Reason, NewState}
      when NewState :: term(), Reason :: term().

%% @doc
%% Invoked when items are discarded from the buffer.
%%
%% It receives the number of excess (discarded) items from this invocation.
%% This callback returns a boolean that controls whether the default error log for discarded items is printed or not.
%% Return true to print the log, return false to skip the log.
%% @end
-callback format_discarded(Discarded :: non_neg_integer(), State :: term()) -> boolean().

%% @doc
%% Invoked when a consumer is no longer subscribed to a producer.
%%
%% It receives the cancellation reason, the `from` tuple representing the
%% cancelled subscription and the state.  The `cancel_reason` will be a
%% `{cancel, _}` tuple if the reason for cancellation was a `gen_stage:cancel/2`
%% call. Any other value means the cancellation reason was due to an EXIT.
%% @end
-callback handle_cancel(CancellationReason :: {cancel | down, Reason}, from(), State :: term()) ->
    {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason, NewState}
      when Event :: term(), NewState :: term(), Reason :: term().

%% @doc
%% Invoked on `producer_consumer` and `consumer` stages to handle events.
%%
%% Must always be explicitly implemented by such types.
%%
%% Return values are the same as `c:handle_cast/2`.
%% @end
-callback handle_events(Events :: [Event], from(), State :: term()) ->
    {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason, NewState}
      when NewState :: term(), Reason :: term(), Event :: term().

%% @doc
%% Invoked to handle synchronous `call/3` messages.
%%
%% `call/3` will block until a reply is received (unless the call times out or
%% nodes are disconnected).
%% @end
-callback handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
    {reply, Reply, [Event], NewState}
    | {reply, Reply, [Event], NewState, hibernate}
    | {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason, Reply, NewState}
    | {stop, Reason, NewState}
      when Reply :: term(), NewState :: term(), Reason :: term(), Event :: term().

%% @doc Invoked to handle asynchronous `cast/2` messages.
-callback handle_cast(Request :: term(), State :: term()) ->
    {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason :: term(), NewState}
      when NewState :: term(), Event :: term().

%% @doc
%% Invoked to handle all other messages.
%%
%% `message` is the message and `state` is the current state of the `gen_stage`.
%% When a timeout occurs the message is `timeout`.
%%
%% Return values are the same as `c:handle_cast/2`.
%% @end
-callback handle_info(Message :: term(), State :: term()) ->
    {noreply, [Event], NewState}
    | {noreply, [Event], NewState, hibernate}
    | {stop, Reason :: term(), NewState}
      when NewState :: term(), Event :: term().

%% @doc
%% The same as `gen_server:terminate/2`.
%% @end
-callback terminate(Reason, State :: term()) -> term()
                                                  when Reason :: normal | shutdown | {shutdown, term()} | term().

%% @doc
%% The same as `gen_server:code_change/3`.
%% @end
-callback code_change(OldVsn :: term(), State :: term(), Extra :: term()) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.

%% @doc The same as `gen_server:format_status/3`.
-callback format_status(normal | terminate, [{term(), term()} | (State :: term())]) ->
    Status :: term().

-optional_callbacks(
   [
    % gen_stage
    handle_subscribe/4,
    handle_cancel/3,
    handle_demand/2,
    handle_events/3,
    format_discarded/2,
    % gen_server
    code_change/3,
    format_status/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
   ]).

%% @doc
%% Starts a `gen_stage` process linked to the current process.
%%
%% This is often used to start the `gen_stage` as part of a supervision tree.
%%
%% Once the server is started, the `init/1` function of the given `Mod` is
%% called with `Args` as its arguments to initialize the stage. To ensure a
%% synchronized start-up procedure, this function does not return until `init/1`
%% has returned.
%%
%% Note that a `gen_stage` started with `start_link/3` is linked to the
%% parent process and will exit in case of crashes from the parent. The `gen_stage`
%% will also exit due to the `normal` reason in case it is configured to trap
%% exits in the `init/1` callback.
%% @end
-spec start_link(module(), term()) -> on_start().
start_link(Mod, Args) ->
    start_link(Mod, Args, []).

-spec start_link(module(), term(), options()) -> on_start().
start_link(Mod, Args, Options) ->
    gen_server:start_link(?MODULE, {Mod, Args}, Options).

-spec start_link(name(), module(), term(), options()) -> on_start().
start_link(Name, Mod, Args, Options) when is_atom(Mod), is_list(Options) ->
    gen_server:start_link(Name, ?MODULE, {Mod, Args}, Options).

%% @doc
%% Starts a `GenStage` process without links (outside of a supervision tree).
%%
%% See `start_link/3` for more information.
%% @end
-spec start(module(), term()) -> on_start().
start(Mod, Args) ->
    start(Mod, Args, []).

-spec start(module(), term(), options()) -> on_start().
start(Mod, Args, Options) ->
    gen_server:start(?MODULE, {Mod, Args}, Options).

-spec start(name(), module(), term(), options()) -> on_start().
start(Name, Mod, Args, Options) when is_atom(Mod), is_list(Options) ->
    gen_server:start(Name, ?MODULE, {Mod, Args}, Options).

%% @doc
%% Queues an info message that is delivered after all currently buffered events.
%%
%% This call is synchronous and will return after the stage has queued
%% the info message. The message will be eventually handled by the
%% `handle_info/2` callback.
%%
%% If the stage is a consumer, it does not have buffered events, so the
%% messaged is queued immediately.
%%
%% This function will return `:ok` if the info message is successfully queued.
%% @end
-spec sync_info(stage(), term()) -> ok.
sync_info(Stage, Msg) ->
    sync_info(Stage, Msg, ?TIMEOUT).

-spec sync_info(stage(), term(), timeout()) -> ok.
sync_info(Stage, Msg, Timeout) ->
    call(Stage, {'$info', Msg}, Timeout).

%% @doc
%% Asynchronously queues an info message that is delivered after all
%% currently buffered events.
%%
%% If the stage is a consumer, it does not have buffered events, so the
%% message is queued immediately.
%%
%% This call returns `ok` regardless if the info has been successfully
%% queued or not. It is typically called from the stage itself.
%% @end
-spec async_info(stage(), term()) -> ok.
async_info(Stage, Msg) ->
    cast(Stage, {'$info', Msg}).

%% @doc
%% Returns the demand mode for a producer.
%%
%% It is either `forward` or `accumulate`. See `demand/2`.
%% @end
-spec demand(stage()) -> forward | accumulate.
demand(Stage) ->
    call(Stage, '$demand').

%% @doc
%% Sets the demand mode for a producer.
%% When `forward`, the demand is always forwarded to the `handle_demand`
%% callback. When `accumulate`, demand is accumulated until its mode is
%% set to `forward`. This is useful as a synchronization mechanism, where
%% the demand is accumulated until all consumers are subscribed. Defaults
%% to `forward`.
%%
%% This command is asynchronous.
%% @end
-spec demand(stage(), forward | accumulate) -> ok.
demand(Stage, Mode) ->
    cast(Stage, {'$demand', Mode}).

%% @doc
%% Asks the consumer to subscribe to the given producer synchronously.
%%
%% This call is synchronous and will return after the called consumer
%% sends the subscribe message to the producer. It does not, however,
%% wait for the subscription confirmation. Therefore this function
%% will return before `handle_subscribe/4` is called in the consumer.
%% In other words, it guarantees the message was sent, but it does not
%% guarantee a subscription has effectively been established.
%%
%% This function will return `{ok, subscription_tag}` as long as the
%% subscription message is sent. It will return `{error, not_a_consumer}`
%% when the stage is not a consumer. `subscription_tag` is the second element
%% of the two-element tuple that will be passed to `handle_subscribe/4`.
%% @end
-spec sync_subscribe(stage(), subscription_options()) -> {ok, subscription_tag()} | {error, not_a_consumer} | {error, {bad_opts, iolist() | binary()}}.
sync_subscribe(Stage, Opts) ->
    sync_subscribe(Stage, Opts, ?TIMEOUT).

-spec sync_subscribe(stage(), subscription_options(), timeout()) -> {ok, subscription_tag()} | {error, not_a_consumer} | {error, {bad_opts, iolist() | binary()}}.
sync_subscribe(Stage, Opts, Timeout) ->
    sync_subscribe(Stage, undefined, Opts, Timeout).
%% TODO check paramter

%% @doc
%% Cancels `SubscriptionTag` with `Reason` and resubscribe
%% to the same stage with the given options.
%%
%% This is useful in case you need to update the options in
%% which you are currently subscribed to in a producer.
%%
%% This function is sync, which means it will wait until the
%% subscription message is sent to the producer, although it
%% won't wait for the subscription confirmation.
%%
%%  See `sync_subscribe/2` for options and more information.
%% @end
-spec sync_resubscribe(stage(), subscription_tag(), term(), subscription_options()) -> {ok, subscription_tag()} | {error, not_a_consumer} | {error, {bad_opts, iolist() | binary()}}.
sync_resubscribe(Stage, SubscriptionTag, Reason, Opts) ->
    sync_resubscribe(Stage, SubscriptionTag, Reason, Opts, ?TIMEOUT).

-spec sync_resubscribe(stage(), subscription_tag(), term(), subscription_options(), timeout()) -> {ok, subscription_tag()} | {error, not_a_consumer} | {error, {bad_opts, iolist() | binary()}}.
sync_resubscribe(Stage, SubscriptionTag, Reason, Opts, Timeout) ->
    sync_subscribe(Stage, {SubscriptionTag, Reason}, Opts, Timeout).

sync_subscribe(Stage, Cancel, Opts, Timeout) ->
    case proplists:lookup(to, Opts) of
        none ->
            throw(iolist_to_binary(io_lib:format("expected to argument in sync_(re)subscribe", [])));
        {to, To} ->
            NewOpts = proplists:delete(to, Opts),
            call(Stage, {'$subscribe', Cancel, To, NewOpts}, Timeout)
    end.

%% @doc
%% Asks the consumer to subscribe to the given producer asynchronously.
%% This function is async, which means it always returns
%% `ok` once the request is dispatched but without waiting
%% for its completion. This particular function is usually
%% called from a stage's `init/1` callback.
%%
%% Options
%%
%% This function accepts the same options as `sync_subscribe/2`.
%% @end
-spec async_subscribe(stage(), subscription_options()) -> ok.
async_subscribe(Stage, Opts) ->
    async_subscribe(Stage, undefined, Opts).

%% @doc
%% Cancels `SubscriptionTag` with `Reason` and resubscribe
%% to the same stage with the given options.
%%
%% This is useful in case you need to update the options in
%% which you are currently subscribed to in a producer.
%%
%% This function is async, which means it always returns
%% `ok` once the request is dispatched but without waiting
%% for its completion.
%%
%% Options
%%
%% This function accepts the same options as `sync_subscribe/2`.
%% @end
-spec async_resubscribe(stage(), subscription_tag(), term(), subscription_options()) -> ok.
async_resubscribe(Stage, SubscriptionTag, Reason, Opts) ->
    async_subscribe(Stage, {SubscriptionTag, Reason}, Opts).

async_subscribe(Stage, Cancel, Opts) ->
    case proplists:lookup(to, Opts) of
        none ->
            throw(iolist_to_binary(io_lib:format("expected to argument in async_(re)subscribe", [])));
        {to, To} ->
            NewOpts = proplists:delete(to, Opts),
            cast(Stage, {'$subscribe', Cancel, To, NewOpts})
    end.

%% @doc
%% Asks the given demand to the producer.
%%
%% `ProducerSubscription` is the subscription this demand will be asked on; this
%% term could be for example stored in the stage when received in
%% `handle_subscribe/4`.
%%
%% The demand is a non-negative integer with the amount of events to
%% ask a producer for. If the demand is `0`, this function simply returns `ok`
%% without asking for data.
%%
%% This function must only be used in the cases when a consumer
%% sets a subscription to `manual` mode in the `handle_subscribe/4`
%% callback.
%%
%% It accepts the same options as `erlang:send/3`, and returns the same value as
%% `erlang:send/3`.
%% @end
-spec ask(from(), non_neg_integer()) -> ok | noconnect | nosuspend.
ask(ProducerSubscription, Demand) ->
    ask(ProducerSubscription, Demand, []).

-spec ask(from(), non_neg_integer(), [noconnect | nosuspend]) -> ok | noconnect | nosuspend.
ask({_Pid, _Ref}, 0, _Opts) ->
    ok;
ask({Pid, Ref}, Demand, Opts) when is_integer(Demand), Demand > 0 ->
    erlang:send(Pid, {'$gen_producer', {self(), Ref}, {ask, Demand}}, Opts).

%% @doc
%% Cancels the given subscription on the producer.
%%
%% The second argument is the cancellation reason. Once the
%% producer receives the request, a confirmation may be
%% forwarded to the consumer (although there is no guarantee
%% as the producer may crash for unrelated reasons before).
%% The consumer will react to the cancellation according to
%% the `cancel` option given when subscribing. For example:
%%
%%     gen_stage:cancel({Pid, Subscription}, shutdown)
%%
%% will cause the consumer to crash if the `cancel` given
%% when subscribing is `permanent` (the default) but it
%% won't cause a crash in other modes. See the options in
%% `sync_subscribe/3` for more information.
%%
%% The `cancel` operation is an asynchronous request. The
%% third argument are same options as `erlang:send/3`,
%% allowing you to pass `noconnect` or `nosuspend` which
%% is useful when working across nodes. This function returns
%% the same value as `erlang:send/3`.
%% @end
-spec cancel(from(), term()) -> ok | noconnect | nosuspend.
cancel({Pid, Ref} = _ProducerSubscription, Reason) ->
    cancel({Pid, Ref}, Reason, []).

-spec cancel(from(), term(), [noconnect | nosuspend]) -> ok | noconnect | nosuspend.
cancel({Pid, Ref} = _ProducerSubscription, Reason, Opts) ->
    erlang:send(Pid, {'$gen_producer', {self(), Ref}, {cancel, Reason}}, Opts).

send_noconnect(Pid, Msg) ->
    erlang:send(Pid, Msg, [noconnect]).

%% @doc
%% Makes a synchronous call to the `stage` and waits for its reply.
%%
%% The client sends the given `request` to the stage and waits until a
%% reply arrives or a timeout occurs. `handle_call/3` will be called on
%% the stage to handle the request.
%%
%% `stage` can be any of the values described in the "Name registration"
%% section of the documentation for this module.
%% 
%% Timeouts
%%
%% `timeout` is an integer greater than zero which specifies how many
%% milliseconds to wait for a reply, or the atom `infinity` to wait
%% indefinitely. The default value is `5000`. If no reply is received
%% within the specified time, the function call fails and the caller
%% exits. If the caller catches the failure and continues running, and
%% the stage is just late with the reply, such reply may arrive at any
%% time later into the caller's message queue. The caller must in this
%% case be prepared for this and discard any such garbage messages that
%% are two-element tuples with a reference as the first element.
%% @end
-spec call(stage(), term()) -> term().
call(Stage, Request) ->
    call(Stage, Request, ?TIMEOUT).

-spec call(stage(), term(), timeout()) -> term().
call(Stage, Request, Timeout) ->
    gen_server:call(Stage, Request, Timeout).

%% @doc
%% Sends an asynchronous request to the `stage`.
%%
%% This function always returns `ok` regardless of whether
%% the destination `stage` (or node) exists. Therefore it
%% is unknown whether the destination stage successfully
%% handled the message.
%% 
%% `handle_cast/2` will be called on the stage to handle
%% the request. In case the `stage` is on a node which is
%% not yet connected to the caller one, the call is going to
%% block until a connection happens.
%% @end
-spec cast(stage(), term()) -> ok.
cast(Stage, Request) ->
    gen_server:cast(Stage, Request).

%% @doc
%% Replies to a client.
%%
%% This function can be used to explicitly send a reply to a client that
%% called `call/3` when the reply cannot be specified in the return value
%% of `handle_call/3`.
%%
%% `client` must be the `from` argument (the second argument) accepted by
%% `handle_call/3` callbacks. `reply` is an arbitrary term which will be
%%  given back to the client as the return value of the call.
%%
%% Note that `reply/2` can be called from any process, not just the `gen_stage`
%% that originally received the call (as long as that `gen_stage` communicated the
%% `from` argument somehow).
%%
%% This function always returns `ok`.
%% @end
reply({To, Tag}, Reply) when is_pid(To) ->
    try
        erlang:send(To, {Tag, Reply}),
        ok
    catch
        _:_ -> ok
    end.

%% @doc
%% Stops the stage with the given `reason`.
%%
%% The `terminate/2` callback of the given `stage` will be invoked before
%% exiting. This function returns `ok` if the server terminates with the
%% given reason; if it terminates with another reason, the call exits.
%% This function keeps OTP semantics regarding error reporting.
%% If the reason is any other than `normal`, `shutdown` or
%% `{shutdown, _}`, an error report is logged.
%% @end
stop(Stage) ->
    stop(Stage, normal, infinity).

-spec stop(stage(), term(), timeout()) -> ok.
stop(Stage, Reason, Timeout) ->
    gen:stop(Stage, Reason, Timeout).

%% @doc Returns the estimated number of buffered items for a producer.
estimate_buffered_count(Stage) ->
    estimate_buffered_count(Stage, 5000).

-spec estimate_buffered_count(stage(), non_neg_integer()) -> non_neg_integer().
estimate_buffered_count(Stage, Timeout) ->
    call(Stage, '$estimate_buffered_count', Timeout).

init({Mod, Args}) ->
    case Mod:init(Args) of
        {producer, State} ->
            init_producer(Mod, [], State);
        {producer, State, Opts} when is_list(Opts) ->
            init_producer(Mod, Opts, State);
        {producer_consumer, State} ->
            init_producer_consumer(Mod, [], State);
        {producer_consumer, State, Opts} when is_list(Opts) ->
            init_producer_consumer(Mod, Opts, State);
        {consumer, State} ->
            init_consumer(Mod, [], State);
        {consumer, State, Opts} when is_list(Opts) ->
            init_consumer(Mod, Opts, State);
        {stop, _} = Stop ->
            Stop;
        ignore ->
            ignore;
        Other ->
            {stop, {bad_return_value, Other}}
    end.

init_producer(Mod, Opts, State) ->
    case init_dispatcher(Opts) of
        {ok, DispatcherMod, DispatcherState, Opts1} ->
            case validate_integer(Opts1, buffer_size, 10000, 0, infinity, true) of
                {ok, BufferSize, Opts2} ->
                    case validate_in(Opts2, buffer_keep, last, [first, last]) of
                        {ok, BufferKeep, Opts3} ->
                            case validate_in(Opts3, demand, forward, [accumulate, forward]) of
                                {ok, Demand, Opts4} ->
                                    case validate_no_opts(Opts4) of
                                        ok ->
                                            Events =
                                            case Demand of
                                                accumulate -> [];
                                                _ -> forward
                                            end,

                                            Stage = #stage{
                                                       mod = Mod,
                                                       state = State,
                                                       type = producer,
                                                       buffer = gen_stage_buffer:new(BufferSize),
                                                       buffer_keep = BufferKeep,
                                                       events = Events,
                                                       dispatcher_mod = DispatcherMod,
                                                       dispatcher_state = DispatcherState
                                                      },
                                            {ok, Stage};
                                        {error, Message} ->
                                            {stop, {bad_opts, Message}}
                                    end;
                                {error, Message} ->
                                    {stop, {bad_opts, Message}}
                            end;
                        {error, Message} ->
                            {stop, {bad_opts, Message}}
                    end;
                {error, Message} ->
                    {stop, {bad_opts, Message}}
            end;
        {error, Message} ->
            {stop, {bad_opts, Message}}
    end.

init_dispatcher(Opts) ->
    case proplists:get_value(dispatcher, Opts, gen_stage_demand_dispatcher) of
        Dispatcher when is_atom(Dispatcher) ->
            {ok, DispatcherState} = Dispatcher:init([]),
            NewOpts = proplists:delete(dispatcher, Opts),
            {ok, Dispatcher, DispatcherState, NewOpts};
        {Dispatcher, DispatcherOpts} when is_atom(Dispatcher), is_list(DispatcherOpts) ->
            {ok, DispatcherState} = Dispatcher:init(DispatcherOpts),
            NewOpts = proplists:delete(dispatcher, Opts),
            {ok, Dispatcher, DispatcherState, NewOpts};
        Other ->
            {error, io_lib:format("expected dispatcher to be an atom or a {atom(), list()}, got ~p", [Other])}
    end.

init_producer_consumer(Mod, Opts, State) ->
    case init_dispatcher(Opts) of
        {ok, DispatcherMod, DispatcherState, Opts1} ->
            case validate_list(Opts1, subscribe_to, []) of
                {ok, SubscribeTo, Opts2} ->
                    case validate_integer(Opts2, buffer_size, infinity, 0, infinity, true) of
                        {ok, BufferSize, Opts3} ->
                            case validate_in(Opts3, buffer_keep, last, [first, last]) of
                                {ok, BufferKeep, Opts4} ->
                                    case validate_no_opts(Opts4) of
                                        ok ->
                                            Stage = #stage{
                                                       mod = Mod,
                                                       state = State,
                                                       type = producer_consumer,
                                                       buffer = gen_stage_buffer:new(BufferSize),
                                                       buffer_keep = BufferKeep,
                                                       events = {queue:new(), 0},
                                                       dispatcher_mod = DispatcherMod,
                                                       dispatcher_state = DispatcherState
                                                      },
                                            consumer_init_subscribe(SubscribeTo, Stage);
                                        {error, Message} ->
                                            {stop, {bad_opts, Message}}
                                    end;
                                {error, Message} ->
                                    {stop, {bad_opts, Message}}
                            end;
                        {error, Message} ->
                            {stop, {bad_opts, Message}}
                    end;
                {error, Msg} ->
                    {stop, {bad_opts, Msg}}
            end;
        {error, Message} ->
            {stop, {bad_opts, Message}}
    end.

init_consumer(Mod, Opts, State) ->
    case validate_list(Opts, subscribe_to, []) of
        {ok, SubscribeTo, NewOpts} ->
            case validate_no_opts(NewOpts) of
                ok ->
                    Stage = #stage{mod = Mod, state = State, type = consumer},
                    consumer_init_subscribe(SubscribeTo, Stage);
                {error, Msg} ->
                    {stop, {bad_opts, Msg}}
            end;
        {error, Msg} ->
            {stop, {bad_opts, Msg}}
    end.

handle_call({'$info', Msg}, _From, Stage) ->
    producer_info(Msg, Stage);
handle_call('$demand', _From, Stage) ->
    producer_demand(Stage);
handle_call({'$subscribe', Current, To, Opts}, _From, Stage) ->
    consumer_subscribe(Current, To, Opts, Stage);
handle_call('$estimate_buffered_count', _From, Stage) ->
    producer_estimate_buffered_count(Stage);
handle_call(Msg, From, #stage{mod = Mod, state = State} = Stage) ->
    case Mod:handle_call(Msg, From, State) of
        {reply, Reply, Events, NewState} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            {reply, Reply, NewStage#stage{state = NewState}};
        {reply, Reply, Events, NewState, hibernate} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            {reply, Reply, NewStage#stage{state = NewState}, hibernate};
        {stop, Reason, Reply, NewState} ->
            {stop, Reason, Reply, Stage#stage{state = NewState}};
        Return ->
            handle_noreply_callback(Return, Stage)
    end.

handle_cast({'$info', Msg}, Stage) ->
    {reply, _, NewStage} = producer_info(Msg, Stage),
    {noreply, NewStage};
handle_cast({'$demand', Mode}, Stage) ->
    producer_demand(Mode, Stage);
handle_cast({'$subscribe', Current, To, Opts}, Stage) ->
    case consumer_subscribe(Current, To, Opts, Stage) of
        {reply, _, NewStage} -> {noreply, NewStage};
        {stop, Reason, _, NewStage} -> {stop, Reason, NewStage};
        {stop, _, _} = Stop -> Stop
    end;
handle_cast(Msg, #stage{state = State} = Stage) ->
    noreply_callback(handle_cast, [Msg, State], Stage).

handle_info({'DOWN', Ref, _, _, Reason} = Msg, Stage) ->
    #stage{producers = Producers, monitors = Monitors, state = State} = Stage,
    case Producers of
        #{Ref := _} ->
            consumer_cancel(Ref, down, Reason, Stage);
        #{} ->
            case Monitors of
                #{Ref := ConsumerRef} ->
                    producer_cancel(ConsumerRef, down, Reason, Stage);
                #{} ->
                    noreply_callback(handle_info, [Msg, State], Stage)
            end
    end;
handle_info({'$gen_producer', _, _} = Msg, #stage{type = consumer} = Stage) ->
    ErrMsg = "gen_stage consumer ~tp received $gen_producer message: ~tp~n",
    error_logger:error_msg(ErrMsg, [self_name(), Msg]),
    {noreply, Stage};
handle_info({'$gen_producer', {ConsumerPid, Ref} = From, {subscribe, Cancel, Opts}}, #stage{consumers = Consumers} = Stage) ->
    case Consumers of
        #{Ref := _} ->
            ErrMsg = "gen_stage producer ~tp received duplicated subscription from: ~tp~n",
            error_logger:error_msg(ErrMsg, [self_name(), From]),
            Msg = {'$gen_consumer', {self(), Ref}, {cancel, duplicated_subscription}},
            send_noconnect(ConsumerPid, Msg),
            {noreply, Stage};
        #{} ->
            case maybe_producer_cancel(Cancel, Stage) of
                {noreply, Stage1} ->
                    MonRef = erlang:monitor(process, ConsumerPid),
                    NewMon = maps:put(MonRef, Ref, Stage1#stage.monitors),
                    NewCon = maps:put(Ref, {ConsumerPid, MonRef}, Stage1#stage.consumers),
                    producer_subscribe(Opts, From, Stage1#stage{monitors = NewMon, consumers = NewCon});
                Other ->
                    Other
            end
    end;
handle_info({'$gen_producer', {ConsumerPid, Ref} = From, {ask, Counter}}, #stage{consumers = Consumers} = Stage) when is_integer(Counter) ->
    case Consumers of
        #{Ref := _} ->
            dispatcher_callback(ask, [Counter, From, Stage#stage.dispatcher_state], Stage);
        #{} ->
            Msg = {'$gen_consumer', {self(), Ref}, {cancel, unknown_subscription}},
            send_noconnect(ConsumerPid, Msg),
            {noreply, Stage}
    end;
handle_info({'$gen_producer', {_, Ref}, {cancel, Reason}}, Stage) ->
    producer_cancel(Ref, cancel, Reason, Stage);
handle_info({'$gen_consumer', _, _} = Msg, #stage{type = producer} = Stage) ->
    ErrMsg = "stage producer ~tp received $gen_consumer message: ~tp~n",
    error_logger:error_msg(ErrMsg, [self_name(), Msg]),
    {noreply, Stage};
handle_info({'$gen_consumer', {ProducerPid, Ref}, Events}, #stage{type = producer_consumer, events = {Queue, Counter}, producers = Producers} = Stage) when is_list(Events) ->
    case maps:is_key(Ref, Producers) of
        true ->
            NewQueue = put_pc_events(Events, Ref, Queue),
            take_pc_events(NewQueue, Counter, Stage);
        false ->
            Msg = {'$gen_producer', {self(), Ref}, {cancel, unknown_subscription}},
            send_noconnect(ProducerPid, Msg),
            {noreply, Stage}
    end;
handle_info({'$gen_consumer', {ProducerPid, Ref} = From, Events}, #stage{type = consumer, producers = Producers, mod = Mod, state = State} = Stage) when is_list(Events) ->
    case Producers of
        #{Ref := Entry} ->
            {Batches, NewStage} = consumer_receive(From, Entry, Events, Stage),
            consumer_dispatch(Batches, From, Mod, State, NewStage, false);
        _ ->
            Msg = {'$gen_producer', {self(), Ref}, {cancel, unknown_subscription}},
            send_noconnect(ProducerPid, Msg),
            {noreply, Stage}
    end;
handle_info({'$gen_consumer', {_, Ref}, {cancel, Reason}}, Stage) ->
    consumer_cancel(Ref, cancel, Reason, Stage);
handle_info(Msg, #stage{state = State} = Stage) ->
    noreply_callback(handle_info, [Msg, State], Stage).

terminate(Reason, #stage{mod = Mod, state = State}) ->
    case erlang:function_exported(Mod, terminate, 2) of
        true -> Mod:terminate(Reason, State);
        false -> ok
    end.

code_change(OldVsn, #stage{mod = Mod, state = State} = Stage, Extra) ->
    case erlang:function_exported(Mod, code_change, 3)  of
        true ->
            case Mod:code_change(OldVsn, State, Extra) of
                {ok, NewState} -> {ok, Stage#stage{state = NewState}};
                Other -> Other
            end;
        false ->
            {ok, Stage}
    end.

% format_status

%% Shared helpers
noreply_callback(handle_info, [Msg, State], #stage{mod = Mod} = Stage) ->
    case erlang:function_exported(Mod, handle_info, 2) of
        true ->
            handle_noreply_callback(Mod:handle_info(Msg, State), Stage);
        false ->
            Log = "undefined handle_info in ~tp~nunhandled message: ~tp~n",
            error_logger:warning_msg(Log, [Mod, Msg]),
            {noreply, Stage#stage{state = State}}
    end;
noreply_callback(handle_cancel, [Subscription, From, State], #stage{mod = Mod} = Stage) ->
    case erlang:function_exported(Mod, handle_cancel, 3) of
        true ->
            handle_noreply_callback(Mod:handle_cancel(Subscription, From, State), Stage);
        false ->
            {noreply, Stage#stage{state = State}}
    end;
noreply_callback(Callback, Args, #stage{mod = Mod} = Stage) ->
    handle_noreply_callback(apply(Mod, Callback, Args), Stage).

handle_noreply_callback(Return, Stage) ->
    case Return of
        {noreply, Events, State} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            {noreply, NewStage#stage{state = State}};
        {noreply, Events, State, hibernate} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            {noreply, NewStage#stage{state = State}, hibernate};
        {stop, Reason, State} ->
            {stop, Reason, Stage#stage{state = State}};
        Other ->
            {stop, {bad_return_value, Other}, Stage}
    end.


%% Producer helpers
producer_demand(#stage{events = forward} = Stage) ->
    {reply, forward, Stage};
producer_demand(#stage{events = Events} = Stage) when is_list(Events) ->
    {reply, accumulate, Stage}.

producer_demand(forward, #stage{type = producer_consumer} = Stage) ->
    {noreply, Stage};
producer_demand(_Mode, #stage{type = Type} = Stage) when type =/= producer ->
    ErrorMsg = "Demand mode can only be set for producers, gen_stage ~tp is a ~ts",
    error_logger:error_msg(ErrorMsg, [self_name(), Type]),
    {noreply, Stage};
producer_demand(forward, #stage{events = Events} = Stage) ->
    NewStage = Stage#stage{events = forward},
    if
        is_list(Events) ->
            Fun =
            fun
                (D, {noreply, #stage{state = State} = StageAcc}) ->
                        noreply_callback(handle_demand, [D, State], StageAcc);
                    (D, {noreply, #stage{state = State} = StageAcc, _}) ->
                        noreply_callback(handle_demand, [D, State], StageAcc);
                    (_, {stop, _, _} = Acc) ->
                        Acc
            end,
            lists:foldl(Fun, {noreply, NewStage}, lists:reverse(Events));
        true ->
            {noreply, NewStage}
    end;
producer_demand(accumulate, #stage{events = Events} = Stage) ->
    if
        is_list(Events) -> {noreply, Stage};
        true -> {noreply, Stage#stage{events = []}}
    end.

producer_subscribe(Opts, From, Stage) ->
    #stage{mod = Mod, state = State, dispatcher_mod = DispatcherMod, dispatcher_state = DispatcherState}  = Stage,
    case maybe_subscribe(Mod, consumer, Opts, From, State) of
        {automatic, NewState} ->
            NewStage = Stage#stage{state = NewState},
            case DispatcherMod:subscribe(Opts, From, DispatcherState) of
                {ok, _, _} = Result -> handle_dispatcher_result(Result, NewStage);
                {error, Reason} -> producer_cancel(element(1, From), cancel, Reason, NewStage)
            end;
        {stop, Reason, NewState} ->
            {stop, Reason, Stage#stage{state = NewState}};
        Other ->
            {stop, {bad_return_value, Other}, Stage}
    end.

maybe_subscribe(Mod, Type, Opts, From, State) ->
    case erlang:function_exported(Mod, handle_subscribe, 4) of
        true ->
            Mod:handle_subscribe(Type, Opts, From, State);
        false ->
            {automatic, State}
    end.

maybe_producer_cancel({Ref, Reason}, Stage) ->
    producer_cancel(Ref, cancel, Reason, Stage);
maybe_producer_cancel(undefined, Stage) ->
    {noreply, Stage}.

maybe_format_discarded(Mod, Excess, State) ->
    case erlang:function_exported(Mod, format_discarded, 2) of
        true -> Mod:format_discarded(Excess, State);
        false -> true
    end.

producer_cancel(Ref, Kind, Reason, Stage) ->
    case maps:take(Ref, Stage#stage.consumers) of
        error ->
            {noreply, Stage};
        {{Pid, MonRef}, Consumers} ->
            erlang:demonitor(MonRef, [flush]),
            send_noconnect(Pid, {'$gen_consumer', {self(), Ref}, {cancel, Reason}}),
            Stage1 = Stage#stage{consumers = Consumers, monitors = maps:remove(MonRef, Stage#stage.monitors)},
            case noreply_callback(handle_cancel, [{Kind, Reason}, {Pid, Ref}, Stage1#stage.state], Stage1) of
                {noreply, #stage{dispatcher_state = DispatcherState} = Stage2} ->
                    dispatcher_callback(cancel, [{Pid, Ref}, DispatcherState], Stage2);
                {stop, _, _} = Stop ->
                    Stop
            end
    end.

dispatcher_callback(Callback, Args, #stage{dispatcher_mod = DispatcherMod} = Stage) ->
    Result = apply(DispatcherMod, Callback, Args),
    handle_dispatcher_result(Result, Stage).

handle_dispatcher_result({ok, Counter, DispatcherState}, Stage) ->
    case Stage of
        #stage{type = producer_consumer, events = {Queue, Demand}} ->
            NewCounter = Counter + Demand,
            Stage1 = Stage#stage{dispatcher_state = DispatcherState, events = {Queue, NewCounter}},
            {ok, _, Stage2} = take_from_buffer(NewCounter, Stage1),
            #stage{events = {Queue1, Counter2}} = Stage2,
            take_pc_events(Queue1, Counter2, Stage2);
        _ ->
            case take_from_buffer(Counter, Stage#stage{dispatcher_state = DispatcherState}) of
                {ok, 0, NewStage} ->
                    {noreply, NewStage};
                {ok, NewCounter, #stage{events = forward, state = State} = NewStage} ->
                    noreply_callback(handle_demand, [NewCounter, State], NewStage);
                {ok, NewCounter, #stage{events = Events} = NewStage} when is_list(Events) ->
                    {noreply, NewStage#stage{events = [NewCounter | Events]}}
            end
    end.

dispatch_events([], _Len, Stage) ->
    Stage;
dispatch_events(Events, _Len, #stage{type = consumer} = Stage) ->
    ErrMsg = "stage consumer ~tp cannot dispatch events (an empty list must be returned): ~tp~n",
    error_logger:error_msg(ErrMsg, [self_name(), Events]),
    Stage;
dispatch_events(Events, _Len, #stage{consumers = Consumers} = Stage) when map_size(Consumers) =:= 0 ->
    buffer_events(Events, Stage);
dispatch_events(Events, Len, Stage) ->
    #stage{dispatcher_mod = DispatcherMod, dispatcher_state = DispatcherState} = Stage,
    {ok, Events1, DispatcherState1} = DispatcherMod:dispatch(Events, Len, DispatcherState),
    NewStage =
    case Stage of
        #stage{type = producer_consumer, events = {Queue, Demand}} ->
            NewDemand = Demand - (Len - length(Events1)),
            Stage#stage{dispatcher_state = DispatcherState1, events = {Queue, max(NewDemand, 0)}};
        _ ->
            Stage#stage{dispatcher_state = DispatcherState1}
    end,
    buffer_events(Events1, NewStage).

take_from_buffer(Counter, #stage{buffer = Buffer} = Stage) ->
    case gen_stage_buffer:take_count_or_until_permanent(Buffer, Counter) of
        empty ->
            {ok, Counter, Stage};
        {ok, NewBuffer, NewCounter, Temps, Perms} ->
            Stage1 = dispatch_events(Temps, Counter - NewCounter, Stage#stage{buffer = NewBuffer}),
            Stage2 = lists:foldl(fun dispatch_info/2, Stage1, Perms),
            take_from_buffer(NewCounter, Stage2)
    end.

buffer_events([], Stage) ->
    Stage;
buffer_events(Events, #stage{mod = Mod, buffer = Buffer, buffer_keep = Keep, state = State} = Stage) ->
    {NewBuffer, Excess, Perms} = gen_stage_buffer:store_temporary(Buffer, Events, Keep),
    case Excess of
        0 -> ok;
        _ ->
            case maybe_format_discarded(Mod, Excess, State) of
                true ->
                    ErrMsg = "stage producer ~tp has discarded ~tp events from buffer",
                    error_logger:warning_msg(ErrMsg, [self_name(), Excess]);
                false ->
                    ok
            end
    end,
    lists:foldl(fun dispatch_info/2, Stage#stage{buffer = NewBuffer}, Perms).

producer_estimate_buffered_count(#stage{type = consumer} = Stage) ->
    ErrorMsg = "Buffered count can only be requested for producers, gen_stage ~tp is a consumer",
    error_logger:error_msg(ErrorMsg, [self_name()]),
    {reply, 0, Stage};
producer_estimate_buffered_count(#stage{buffer = Buffer} = Stage) ->
    {reply, gen_stage_buffer:estimate_size(Buffer), Stage}.

%% Info helpers
producer_info(Msg, #stage{type = consumer} = Stage) ->
    erlang:send(self(), Msg),
    {reply, ok, Stage};
producer_info(Msg, #stage{type = producer_consumer, events = {Queue, Demand}} = Stage) ->
    NewStage =
    case queue:is_empty(Queue) of
        true ->
            buffer_or_dispatch_info(Msg, Stage);
        false ->
            Stage#stage{events = {queue:in({info, Msg}, Queue), Demand}}
    end,
    {reply, ok, NewStage};
producer_info(Msg, #stage{type = produer} = Stage) ->
    {reply, ok, buffer_or_dispatch_info(Msg, Stage)}.

buffer_or_dispatch_info(Msg, #stage{buffer = Buffer} = Stage) ->
    case gen_stage_buffer:store_permanent_unless_empty(Buffer, Msg) of
        empty -> dispatch_info(Msg, Stage);
        {ok, NewBuffer} -> Stage#stage{buffer = NewBuffer}
    end.

dispatch_info(Msg, Stage) ->
    #stage{dispatcher_mod = DispatcherMod, dispatcher_state = DispatcherState} = Stage,
    {ok, NewState} = DispatcherMod:info(Msg, DispatcherState),
    Stage#stage{dispatcher_state = NewState}.

%% Consumer helpers
consumer_init_subscribe(Producers, Stage) ->
    Fun =
    fun
        (To, {ok, StageAcc}) ->
            case consumer_subscribe(To, StageAcc) of
                {reply, _, NewStage} -> {ok, NewStage};
                {stop, Reason, _, _} -> {stop, Reason};
                {stop, Reason, _} -> {stop, Reason}
            end;
        (_, {stop, Reason}) -> {stop, Reason}
    end,
    lists:foldl(Fun, {ok, Stage}, Producers).

consumer_receive({_, Ref} = From, {ProducerId, Cancel, {Demand, Min, Max}}, Events, Stage) ->
    {NewDemand, Batches} = split_batches(Events, From, Min, Max, Demand),
    NewProducers = maps:put(Ref, {ProducerId, Cancel, {NewDemand, Min, Max}}, Stage#stage.producers),
    {Batches, Stage#stage{producers = NewProducers}};
consumer_receive(_, {_, _, manual}, Events, Stage) ->
    {[{Events, 0}], Stage}.

consumer_dispatch([{Batch, Ask} | Batches], From, Mod, State, Stage, _Hibernate) ->
    case Mod:handle_events(Batch, From, State) of
        {noreply, Events, NewState} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            ask(From, Ask, [noconnect]),
            consumer_dispatch(Batches, From, Mod, NewState, NewStage, false);
        {noreply, Events, NewState, hibernate} when is_list(Events) ->
            NewStage = dispatch_events(Events, length(Events), Stage),
            ask(From, Ask, [noconnect]),
            consumer_dispatch(Batches, From, Mod, NewState, NewStage, true);
        {stop, Reason, NewState} ->
            {stop, Reason, Stage#stage{state = NewState}};
        Other ->
            {stop, {bad_return_value, Other}, Stage#stage{state = State}}
    end;
consumer_dispatch([], _From, _Mod, State, Stage, false) ->
    {noreply, Stage#stage{state = State}};
consumer_dispatch([], _From, _Mod, State, Stage, true) ->
    {noreply, Stage#stage{state = State}, hibernate}.

consumer_subscribe({To, Opts}, Stage) when is_list(Opts) ->
    consumer_subscribe(undefined, To, Opts, Stage);
consumer_subscribe(To, Stage) ->
    consumer_subscribe(undefined, To, [], Stage).

consumer_subscribe(_Cancel, To, _Opts, #stage{type = producer} = Stage) ->
    ErrMsg = "stage producer ~tp cannot be subscribed to another stage: ~tp~n",
    error_logger:error_msg(ErrMsg, [self_name(), To]),
    {reply, {error, not_a_consumer}, Stage};
consumer_subscribe(Current, To, Opts, Stage) ->
    case validate_integer(Opts, max_demand, 1000, 1, infinity, false) of
        {ok, Max, _} ->
            case validate_integer(Opts, min_demand, Max div 2, 0, Max - 1, false) of
                {ok, Min, _} ->
                    case validate_in(Opts, cancel, permanent, [temporary, transient, permanent]) of
                        {ok, Cancel, _} ->
                            Producer = whereis_server(To),
                            if
                                Producer =/= undefined ->
                                    Ref = monitor(process, Producer),
                                    Msg = {'$gen_producer', {self(), Ref}, {subscribe, Current, Opts}},
                                    send_noconnect(Producer, Msg),
                                    consumer_subscribe(Opts, Ref, Producer, Cancel, Min, Max, Stage);
                                Cancel =:= permanent orelse Cancel =:= transient ->
                                    {stop, noproc, {ok, make_ref()}, Stage};
                                Cancel =:= temporary ->
                                    {reply, {ok, make_ref()}, Stage}
                            end;
                        {error, Msg} ->
                            ErrMsg = "stage consumer ~tp subscribe received invalid option: ~ts~n",
                            error_logger:error_msg(ErrMsg, [self_name(), Msg]),
                            {reply, {error, {bad_opts, Msg}}, Stage}
                    end;
                {error, Msg} ->
                    ErrMsg = "stage consumer ~tp subscribe received invalid option: ~ts~n",
                    error_logger:error_msg(ErrMsg, [self_name(), Msg]),
                    {reply, {error, {bad_opts, Msg}}, Stage}
            end;
        {error, Msg} ->
            ErrMsg = "stage consumer ~tp subscribe received invalid option: ~ts~n",
            error_logger:error_msg(ErrMsg, [self_name(), Msg]),
            {reply, {error, {bad_opts, Msg}}, Stage}
    end.

consumer_subscribe(Opts, Ref, Producer, Cancel, Min, Max, Stage) ->
    #stage{mod = Mod, state = State} = Stage,
    To = {Producer, Ref},
    case maybe_subscribe(Mod, producer, Opts, To, State) of
        {automatic, NewState} ->
            ask(To, Max, [noconnect]),
            Producers = maps:put(Ref, {Producer, Cancel, {Max, Min, Max}}, Stage#stage.producers),
            {reply, {ok, Ref}, Stage#stage{producers = Producers, state = NewState}};
        {manual, NewState} ->
            Producers = maps:put(Ref, {Producer, Cancel, manual}, Stage#stage.producers),
            {reply, {ok, Ref}, Stage#stage{producers = Producers, state = NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, Stage#stage{state = NewState}};
        Other ->
            {stop, {bad_return_value, Other}, Stage}
    end.

consumer_cancel(Ref, Kind, Reason, #stage{producers = Producers} = Stage) ->
    case maps:take(Ref, Producers) of
        error ->
            {noreply, Stage};
        {{Producer, Mode, _}, NewProducers} ->
            erlang:demonitor(Ref, [flush]),
            NewStage = Stage#stage{producers = NewProducers},
            schedule_cancel(Mode, {Kind, Reason}, {Producer, Ref}, NewStage)
    end.

schedule_cancel(Mode, KindReason, ProducerRef, #stage{type = producer_consumer, events = {Queue, Demand}} = Stage) ->
    case queue:is_empty(Queue) of
        true ->
            invoke_cancel(Mode, KindReason, ProducerRef, Stage);
        false ->
            NewQueue = queue:in({cancel, Mode, KindReason, ProducerRef}, Queue),
            {noreply, Stage#stage{events = {NewQueue, Demand}}}
    end;
schedule_cancel(Mode, KindReason, ProducerRef, Stage) ->
    invoke_cancel(Mode, KindReason, ProducerRef, Stage).

invoke_cancel(Mode, {_, Reason} = KindReason, {Pid, _} = ProducerRef, #stage{state = State} = Stage) ->
    case noreply_callback(handle_cancel, [KindReason, ProducerRef, State], Stage) of
        {noreply, NewStage} ->
            case Mode =:= permanent orelse (Mode =:= transient andalso (not is_transient_shutdown(Reason))) of
                true ->
                    case Reason of
                        already_subscribed ->
                            {noreply, NewStage};
                        _ ->
                            ErrMsg = "stage consumer ~tp is stopping after receiving cancel from producer ~tp with reason: ~tp~n",
                            error_logger:info_msg(ErrMsg, [self_name(), Pid, Reason]),
                            {stop, Reason, Stage}
                    end;
                false ->
                    {noreply, NewStage}
            end;
        Other ->
            Other
    end.


%% Producer consumer helpers
put_pc_events(Events, Ref, Queue) ->
    queue:in({Events, Ref}, Queue).

send_pc_events(Events, Ref, #stage{mod = Mod, state = State, producers = Producers} = Stage) ->
    case Producers of
        #{Ref := Entry} ->
            {ProducerId, _, _} = Entry,
            From = {ProducerId, Ref},
            {Batches, NewStage} = consumer_receive(From, Entry, Events, Stage),
            consumer_dispatch(Batches, From, Mod, State, NewStage, false);
        #{} ->
            consumer_dispatch([{Events, 0}], {pid, Ref}, Mod, State, Stage, false)
    end.


take_pc_events(Queue, Counter, Stage) when Counter > 0 ->
    case queue:out(Queue) of
        {{value, {info, Msg}}, Queue1} ->
            take_pc_events(Queue1, Counter, buffer_or_dispatch_info(Msg, Stage));
        {{value, {cancel, Mode, KindReason, Ref}}, Queue1} ->
            case invoke_cancel(Mode, KindReason, Ref, Stage) of
                {noreply, Stage1} ->
                    take_pc_events(Queue1, Counter, Stage1);
                {noreply, Stage1, hibernate} ->
                    take_pc_events(Queue1, Counter, Stage1);
                {stop, _, _} = Stop ->
                    Stop
            end;
        {{value, {Events, Ref}}, Queue1} ->
            case send_pc_events(Events, Ref, Stage#stage{events = {Queue1, Counter}}) of
                {noreply, #stage{events = {Queue2, Counter1}} = Stage1} ->
                    take_pc_events(Queue2, Counter1, Stage1);
                {noreply, #stage{events = {Queue2, Counter1}} = Stage1, hibernate} ->
                    take_pc_events(Queue2, Counter1, Stage1);
                {stop, _, _} = Stop ->
                    Stop
            end;
        {empty, Queue1} ->
            {noreply, Stage#stage{events = {Queue1, Counter}}}
    end;
take_pc_events(Queue, Counter, Stage) ->
    {noreply, Stage#stage{events = {Queue, Counter}}}.


validate_list(Opts, Key, Default) ->
    case proplists:get_value(Key, Opts, Default) of
        Value when is_list(Value) ->
            NewOpts = proplists:delete(Key, Opts),
            {ok, Value, NewOpts};
        Value ->
            {error, io_lib:format("expected ~p to be a list, got: ~p", [Key, Value])}
    end.

validate_in(Opts, Key, Default, Values) ->
    Value = proplists:get_value(Key, Opts, Default),
    case lists:member(Value, Values) of
        true ->
            NewOpts = proplists:delete(Key, Opts),
            {ok, Value, NewOpts};
        false ->
            {error, io_lib:format("expected ~p to be one of ~p, got: ~p", [Key, Values, Value])}
    end.

validate_integer(Opts, Key, Default, Min, Max, Infinity) ->
    Value = proplists:get_value(Key, Opts, Default),
    if
        Value =:= infinity andalso Infinity ->
            {ok, Value, Opts};
        not is_integer(Value) ->
            ErrorMsg = "expected ~p to be an integer, got: ~p",
            {error, io_lib:format(ErrorMsg, [Key, Value])};
        Value < Min ->
            ErrorMsg = "expected ~p to be equal to or greater than ~p, got: ~p",
            {error, io_lib:format(ErrorMsg, [Key, Min, Value])};
        Value > Max ->
            ErrorMsg = "expected ~p to be equal to or less than ~p, got: ~p",
            {error, io_lib:format(ErrorMsg, [Key, Max, Value])};
        true ->
            NewOpts = proplists:delete(Key, Opts),
            {ok, Value, NewOpts}
    end.

validate_no_opts([]) ->
    ok;
validate_no_opts(Opts) ->
    {error, {badarg, Opts}}.

is_transient_shutdown(normal) -> true;
is_transient_shutdown(shutdown) -> true;
is_transient_shutdown({shutdown, _}) -> true;
is_transient_shutdown(_) -> false.

self_name() ->
    case process_info(self(), registered_name) of
        {registered_name, Name} when is_atom(Name) -> Name;
        _ -> self()
    end.

split_batches(Events, From, Min, Max, Demand) ->
    split_batches(Events, From, Min, Max, Demand, Demand, []).

split_batches([], _From, _Min, _Max, _OldDemand, NewDemand, Batches) ->
    {NewDemand, lists:reverse(Batches)};
split_batches(Events, From, Min, Max, OldDemand, NewDemand, Batches) ->
    {NewEvents, Batch, BatchSize} = split_events(Events, Max - Min, 0, []),
    {OldDemand1, BatchSize1} =
    case OldDemand - BatchSize of
        Diff when Diff < 0 ->
            ErrorMsg = "gen_stage consumer ~tp has received ~tp events in excess from: ~tp~n",
            error_logger:error_msg(ErrorMsg, [self_name(), abs(Diff), From]),
            {0, OldDemand};
        Diff ->
            {Diff, BatchSize}
    end,

    % In case we've reached min, we will ask for more events
    {NewDemand1, BatchSize2} =
    case NewDemand - BatchSize1 of
        Diff1 when Diff1 =< Min ->
            {Max, Max - Diff1};
        Diff1 ->
            {Diff1, 0}
    end,
    split_batches(NewEvents, From, Min, Max, OldDemand1, NewDemand1, [{Batch,  BatchSize2} | Batches]).

split_events(Events, Limit, Limit, Acc) ->
    {Events, lists:reverse(Acc), Limit};
split_events([], _Limit, Counter, Acc) ->
    {[], lists:reverse(Acc), Counter};
split_events([Event | Events], Limit, Counter, Acc) ->
    split_events(Events, Limit, Counter + 1, [Event | Acc]).


whereis_server(Pid) when is_pid(Pid) ->
    Pid;
whereis_server(Name) when is_atom(Name) ->
    erlang:whereis(Name);
whereis_server({global, Name}) ->
    global:whereis_name(Name);
whereis_server({via, Mod, Name}) ->
    apply(Mod, whereis_name, [Name]);
whereis_server({Name, Local}) when is_atom(Name) andalso Local =:= node() ->
    erlang:whereis(Name);
whereis_server({Name, Node} = Server) when is_atom(Name) andalso is_atom(Node) ->
    Server.
