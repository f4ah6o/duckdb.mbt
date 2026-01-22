///|
/// Stateful Testing Framework for DuckDB MoonBit bindings
/// Provides model-based testing for stateful systems like Connection and Appender

///|
/// Abstract command type for state machines
pub enum Command[C] {
  Connect
  Query(String)
  Close
  Create
  BeginRow
  Append(Value)
  EndRow
  Flush
  Custom(C)
}

///|
/// Command result wrapper
pub enum CommandResult[R] {
  Ok(R)
  Error(String)
  NotApplicable
}

///|
/// State machine state with model and optional real state
pub struct StatefulState[S, R] {
  model_state: S
  real_state: R?
}

///|
/// Stateful test configuration
pub struct StatefulTest[S, C, R] {
  initial_state: S
  commands: @pbt.Gen[Array[C]]
  init_real: () -> R
  execute_command: (S, R, C) -> (S, CommandResult[R])
  check_postcondition: (S, C, CommandResult[R]) -> Bool
  shrink_command: (C) -> Iter[C]
  cleanup: (R) -> Unit
}

///|
/// Create a new stateful test configuration
pub fn stateful_test[S, C, R](
  initial_state: S,
  commands: @pbt.Gen[Array[C]],
  init_real: () -> R,
  execute_command: (S, R, C) -> (S, CommandResult[R]),
  check_postcondition: (S, C, CommandResult[R]) -> Bool,
  shrink_command: (C) -> Iter[C],
  cleanup: (R) -> Unit,
) -> StatefulTest[S, C, R] {
  StatefulTest::{
    initial_state,
    commands,
    init_real,
    execute_command,
    check_postcondition,
    shrink_command,
    cleanup,
  }
}

///|
/// Run a stateful test
pub fn run_stateful_test[S : Show, C : Show, R : Show](
  test: StatefulTest[S, C, R],
  config: CheckConfig,
) -> Result[Unit, String] {
  // Generate command sequences
  let rs = @splitmix.new(seed=config.seed.to_uint64())
  let size = config.max_size

  let mut case_index = 0
  while case_index < config.cases {
    let cmds = (test.commands.run)(size, rs)

    // Initialize real system
    let real_state = test.init_real()

    // Execute commands
    let mut model_state = test.initial_state
    let mut cmd_index = 0
    let mut failed = false
    let mut error_msg = ""

    while cmd_index < cmds.length() {
      let cmd = cmds[cmd_index]
      let (new_model, result) = test.execute_command(
        model_state,
        real_state,
        cmd,
      )

      // Check postcondition
      if not(test.check_postcondition(model_state, cmd, result)) {
        failed = true
        error_msg = "Postcondition failed for command \{cmd} at index \{cmd_index}: model_state = \{model_state}, result = \{result}"
        break
      }

      model_state = new_model
      cmd_index = cmd_index + 1
    }

    // Cleanup
    test.cleanup(real_state)

    if failed {
      return Err(error_msg)
    }

    case_index = case_index + 1
  }

  Ok(())
}

///|
/// Assert stateful test passes
pub fn assert_stateful_test[S : Show, C : Show, R : Show](
  label: String,
  test: StatefulTest[S, C, R],
  config? : CheckConfig = CheckConfig::default(),
) -> Unit raise {
  match run_stateful_test(test, config) {
    Ok(_) => ()
    Err(msg) => fail("\{label}: \{msg}")
  }
}

///|
/// Simple state marker for tracking state machine states
pub enum SimpleState {
  NotConnected
  Connected
  InProgress
  Closed
  Error
}

///|
/// Track state transitions for validation
pub struct StateTransition {
  from: SimpleState
  to: SimpleState
  command: String
}

///|
/// State history for debugging
pub struct StateHistory {
  transitions: Array[StateTransition]
  final_state: SimpleState
}

///|
/// Create empty state history
pub fn state_history_empty() -> StateHistory {
  StateHistory::{
    transitions: [],
    final_state: SimpleState::NotConnected,
  }
}

///|
/// Add transition to history
pub fn state_history_add(
  history: StateHistory,
  from: SimpleState,
  to: SimpleState,
  command: String,
) -> StateHistory {
  let transition = StateTransition::{
    from: from,
    to: to,
    command: command,
  }
  StateHistory::{
    transitions: history.transitions.push(transition),
    final_state: to,
  }
}

///|
/// Validate state transition is legal
pub fn is_valid_transition(
  from: SimpleState,
  to: SimpleState,
  command: String,
) -> Bool {
  match (from, to) {
    // NotConnected can only transition to Connected or stay NotConnected
    (SimpleState::NotConnected, SimpleState::Connected) => true
    (SimpleState::NotConnected, SimpleState::NotConnected) => true

    // Connected can transition to InProgress (query starts) or Closed
    (SimpleState::Connected, SimpleState::InProgress) => true
    (SimpleState::Connected, SimpleState::Closed) => true
    (SimpleState::Connected, SimpleState::Connected) => true

    // InProgress can transition back to Connected (query done)
    (SimpleState::InProgress, SimpleState::Connected) => true
    (SimpleState::InProgress, SimpleState::InProgress) => true

    // Closed cannot transition (terminal state)
    (SimpleState::Closed, SimpleState::Closed) => true
    (SimpleState::Closed, _) => false

    // Error state can only transition to Closed or stay Error
    (SimpleState::Error, SimpleState::Closed) => true
    (SimpleState::Error, SimpleState::Error) => true

    // All other transitions are invalid
    _ => false
  }
}

///|
/// Appender state machine states
pub enum AppenderState {
  NotCreated
  Ready
  RowInProgress
  Flushed
  Closed
  Error
}

///|
/// Validate appender state transition
pub fn is_valid_appender_transition(
  from: AppenderState,
  to: AppenderState,
  command: String,
) -> Bool {
  match (from, to) {
    // NotCreated can only go to Ready
    (AppenderState::NotCreated, AppenderState::Ready) => true
    (AppenderState::NotCreated, AppenderState::NotCreated) => true
    (AppenderState::NotCreated, _) => false

    // Ready can go to RowInProgress or Flushed or Closed
    (AppenderState::Ready, AppenderState::RowInProgress) => true
    (AppenderState::Ready, AppenderState::Flushed) => true
    (AppenderState::Ready, AppenderState::Closed) => true
    (AppenderState::Ready, AppenderState::Ready) => true

    // RowInProgress must go to Ready (after EndRow) or stay RowInProgress
    (AppenderState::RowInProgress, AppenderState::Ready) => true
    (AppenderState::RowInProgress, AppenderState::RowInProgress) => true
    (AppenderState::RowInProgress, AppenderState::Closed) => true

    // Flushed can go back to Ready or stay Flushed or go to Closed
    (AppenderState::Flushed, AppenderState::Ready) => true
    (AppenderState::Flushed, AppenderState::Flushed) => true
    (AppenderState::Flushed, AppenderState::Closed) => true

    // Closed is terminal
    (AppenderState::Closed, AppenderState::Closed) => true
    (AppenderState::Closed, _) => false

    // Error can only go to Closed or stay Error
    (AppenderState::Error, AppenderState::Closed) => true
    (AppenderState::Error, AppenderState::Error) => true

    _ => false
  }
}

///|
/// Generate random command sequence for connection state machine
pub fn gen_connection_commands() -> @pbt.Gen[Array[String]] {
  @pbt.int_range(0, 20).bind(fn(len) {
    array_of(@pbt.one_of([
      @pbt.pure("connect"),
      @pbt.pure("query"),
      @pbt.pure("close"),
      @pbt.pure("prepare"),
      @pbt.Gen::fmap(@pbt.int_range(0, 100), fn(n) {
        "query_" + n.to_string()
      }),
    ]))
  })
}

///|
/// Generate random command sequence for appender state machine
pub fn gen_appender_commands() -> @pbt.Gen[Array[String]] {
  @pbt.int_range(0, 30).bind(fn(len) {
    array_of(@pbt.one_of([
      @pbt.pure("create"),
      @pbt.pure("begin_row"),
      @pbt.pure("append"),
      @pbt.pure("end_row"),
      @pbt.pure("flush"),
      @pbt.pure("close"),
    ]))
  })
}

///|
/// Check state machine invariants
pub fn check_state_machine_invariants(
  states: Array[SimpleState],
  transitions: Array[String],
) -> Result[Unit, String] {
  if states.length() == 0 {
    return Ok(())
  }

  let mut i = 0
  while i < states.length() - 1 {
    let from = states[i]
    let to = states[i + 1]
    let cmd = if i < transitions.length() {
      transitions[i]
    } else {
      "unknown"
    }

    if not(is_valid_transition(from, to, cmd)) {
      return Err(
        "Invalid transition: \{from} -> \{to} via command '\{cmd}' at index \{i}",
      )
    }

    i = i + 1
  }

  Ok(())
}

///|
/// Check appender state machine invariants
pub fn check_appender_invariants(
  states: Array[AppenderState],
  transitions: Array[String],
) -> Result[Unit, String] {
  if states.length() == 0 {
    return Ok(())
  }

  let mut i = 0
  while i < states.length() - 1 {
    let from = states[i]
    let to = states[i + 1]
    let cmd = if i < transitions.length() {
      transitions[i]
    } else {
      "unknown"
    }

    if not(is_valid_appender_transition(from, to, cmd)) {
      return Err(
        "Invalid appender transition: \{from} -> \{to} via command '\{cmd}' at index \{i}",
      )
    }

    i = i + 1
  }

  Ok(())
}

///|
/// Model state for Connection
pub struct ConnectionModel {
  is_connected: Bool
  is_closed: Bool
  query_count: Int
  has_pending_query: Bool
}

///|
/// Initial connection model state
pub fn connection_model_initial() -> ConnectionModel {
  ConnectionModel::{
    is_connected: false,
    is_closed: false,
    query_count: 0,
    has_pending_query: false,
  }
}

///|
/// Model state for Appender
pub struct AppenderModel {
  is_created: Bool
  is_closed: Bool
  row_in_progress: Bool
  column_count: Int
  expected_columns: Int
  row_count: Int
  flushed_row_count: Int
}

///|
/// Initial appender model state
pub fn appender_model_initial(expected_columns: Int) -> AppenderModel {
  AppenderModel::{
    is_created: false,
    is_closed: false,
    row_in_progress: false,
    column_count: 0,
    expected_columns: expected_columns,
    row_count: 0,
    flushed_row_count: 0,
  }
}
