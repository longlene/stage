%% @doc This module defines the behaviour used by `producer' and
%% `producer_consumer' to dispatch events.
%%
%% When using a `producer' or `producer_consumer', the dispatcher
%% may be configured on init as follows:
%%
%%   {producer, State, [{dispatcher, gen_stage_broadcoast_dispatcher}]}
%%
%% Some dispatchers may require options to be given on initialization,
%% those can be done with a tuple:
%%
%%  {producer, State, [{dispatcher, {gen_stage_partition_dispatcher, [{partitions, lists:seq(0, 3)}]}}]}
%%
%% Stage ships with the following dispatcher implementations:
%%
%%   * `gen_stage_demand_dispatcher' - dispatches the given batch of
%%     events to the consumer with the biggest demand in a FIFO
%%     ordering. This is the default dispatcher.
%%
%%   * `gen_stage_broadcast_dispatcher' - dispatches all events to all
%%     consumers. The demand is only sent upstream once all consumers
%%     ask for data.
%%
%%   * `gen_stage_partition_dispatcher' - dispatches all events to a
%%     fixed amount of consumers that works as partitions according
%%     to a hash function.
%%
%% Note that the dispatcher state is stored separately from the state of the
%% `gen_stage' itself and neither side will have direct access
%% to the state of the other.
%% @end

-module(gen_stage_dispatcher).

-type keyword() :: [{atom(), any()}].
-type options() :: keyword().
-export_type([options/0]).

%% Called on initialization with the options given on `gen_stage:init/1'.
-callback init(Opts :: keyword()) -> {ok, State} when State :: any().

%% Called every time the producer gets a new subscriber.
-callback subscribe(Opts :: keyword(), From :: {pid(), reference()}, State :: term()) ->
    {ok, Demand :: non_neg_integer(), NewState} | {error, term()}
        when NewState :: term().

%% Called every time a subscription is cancelled or the consumer goes down.
%% It is guaranteed the reference give in `From' points to a reference
%% previously given in subscribe.
-callback cancel(From :: {pid(), reference()}, State :: term()) ->
    {ok, Demand :: non_neg_integer(), NewState}
        when NewState :: term().

%% Called every time a consumer sends demand.
%% The demand will always be a positive integer (more than 0).
%% This callback must return the `ActualDemand' as part of its
%% return tuple. The returned demand is then sent to producers.
%%
%% It is guaranteed the reference given in `From' points to a
%% reference previously given in subscribe.
-callback ask(Demand :: pos_integer(), From :: {pid(), reference()}, State :: term()) ->
    {ok, ActualDemand :: non_neg_integer(), NewState}
        when NewState :: term().

%% Called every time a producer wants to dispatch an event.
%%
%% The events will always be a non empty list. This callback may
%% receive more events than previously asked and therefore must
%% return events it cannot not effectively deliver as part of its
%% return tuple. Any `leftover_events' will be stored by producers
%% in their buffer.
%%
%% It is important to emphasize that `leftover_events' can happen
% in any dispatcher implementation. After all, a consumer can
%% subscribe, ask for events and crash. Eventually the events
%% the consumer asked will be delivered while the consumer no longer
%% exists, meaning they must be returned as left_over events until
%% another consumer subscribes.
%%
%% This callback is responsible for sending events to consumer
%% stages. In order to do so, you must store a `From' value from a
%% previous `ask/3' callback.
%%
%% It is recommended for these events to be sent with `erlang:send/3'
%% and the `[nonconnect]' option as the consumers are all monitored
%% by the producer.
-callback dispatch(Events :: nonempty_list(term()), Length :: pos_integer(), State :: term()) ->
    {ok, LeftoverEvents :: [term()], NewState}
        when NewState :: term().

%% Used to send an info message to the current process.
%%
%% In case the dispatcher is doing buffering, the message must
%% only be sent after all currently buffered consumer messages are
%% delivered.
-callback info(Msg :: term(), State :: term()) ->
    {ok, NewState} when NewState :: term().
