# Changelog

## v1.3.2 (2026-09-23)

### Bug fixes

- Hold back events returned from any callback while a producer is in `accumulate` demand mode (previously only `handle_demand/2` was held back)
- `gen_stage:from_fun/1,2` kept producing only the first demand batch and never stopped; exceptions raised by the generator are no longer silently treated as `done`
- `gen_stage_partition_dispatcher` crashed on `sync_info/2` / `async_info/2`
- `consumer_supervisor` restarted transient children without the event argument
- `gen_stage_buffer`: return permanents (info messages) in FIFO order for bounded buffers, and flush permanents right after the last taken event for infinite buffers (Elixir GenStage v1.3.1 fix)

## v1.3.1 (2025-01-13)

### 🎉 Major Update: Alignment with Elixir GenStage v1.3.1

This release brings the Erlang `stage` library up to feature parity with Elixir's GenStage v1.3.1, adding 3+ years of improvements, bug fixes, and new functionality.

### ✨ New Features

#### **New Producer Creation APIs**
- **`gen_stage:from_list/1,2`** - Create producers from Erlang lists
- **`gen_stage:from_fun/1,2`** - Create producers from generator functions
- **`gen_stage_list_producer`** - New module supporting list/function-based producers

#### **Enhanced DemandDispatcher**
- **`shuffle_demands_on_first_dispatch`** option for load balancing across consumers
- **`max_demand`** explicit configuration support
- Improved warning messages for demand mismatches

#### **New Utility Module**
- **`gen_stage_utils`** - Validation and helper functions
  - `validate_integer/6` - Integer validation with min/max/infinity support
  - `validate_list/3` - List validation
  - `validate_in/4` - Option value validation
  - `split_batches/5` - Event batching utilities

### 🐛 Bug Fixes

#### **From Elixir GenStage v1.3.x**
- Fixed ordering when there are many buffered info messages
- Send events to dispatchers even if there are no consumers
- Hold demand in broadcast dispatcher until asking

#### **From Elixir GenStage v1.2.x**
- Do not dispatch when accumulating demand
- Better load balancing through demand shuffling

#### **From Elixir GenStage v1.1.x & v1.0.x**
- Improved error logging for stage termination
- Enhanced support for process specifications
- Event discarding support in PartitionDispatcher

### 📚 Documentation & Examples

#### **New Examples**
- `examples/producer_consumer.erl` - Complete pipeline demonstration
- `examples/test_new_features.erl` - New features validation

#### **Enhanced Documentation**
- Updated `CLAUDE.md` with new APIs and usage patterns
- Comprehensive API documentation for new functions
- Better error messages and type specifications

### 🔧 Internal Improvements

- **Better Erlang Conventions**: Code follows Erlang idioms more closely
- **Enhanced Error Handling**: More robust error reporting
- **Type Safety**: Improved type specifications
- **Performance**: Better demand management and event batching

### 📦 Compatibility

- **Backward Compatible**: All existing APIs continue to work
- **Erlang-Native**: No dependencies on Elixir-specific features
- **OTP Compatible**: Works with standard Erlang/OTP supervision trees

### 🚫 Intentionally Not Ported

The following Elixir-specific features were intentionally not ported due to fundamental differences between Erlang and Elixir:

- Complex `Enumerable`/`Stream` protocol integration - no equivalent in Erlang

Instead, we provide Erlang-native alternatives:
- `from_list/1,2` instead of `from_enumerable/1,2`
- `from_fun/1,2` for generator-based producers
- `gen_stage_stream:subscribe/1,2` and `close/1` as the Erlang equivalent of
  `GenStage.stream/1,2` - it delivers `'$gen_consumer'` messages to the
  caller's own mailbox instead of exposing them through the `Enumerable`
  protocol, since Erlang has no such protocol to hook into

### 💻 Usage Examples

```erlang
%% Create producer from list
{ok, Producer} = gen_stage:from_list([1, 2, 3, 4, 5]),

%% Create producer from generator function
Fun = fun() -> {value, rand:uniform(100)} end,
{ok, Producer2} = gen_stage:from_fun(Fun),

%% Enhanced dispatcher with load balancing
{producer, State, [{dispatcher, {gen_stage_demand_dispatcher, [
    {shuffle_demands_on_first_dispatch, true},
    {max_demand, 1000}
]}}]},
```

### 🔄 Migration from v0.3.0

Most code should work without changes. The main differences:

1. **New APIs available** - Optional upgrades to use `from_list/2` and `from_fun/2`
2. **Enhanced dispatcher options** - Can now configure load balancing
3. **Better error messages** - More informative warnings and errors

### 📈 Version Jump Explanation

The version jumped from 0.3.0 to 1.3.1 to align with the upstream Elixir GenStage version, indicating feature parity rather than a breaking change.

---

## v0.3.0 (Previous Release)

Original Erlang port of Elixir's GenStage with basic producer/consumer functionality.