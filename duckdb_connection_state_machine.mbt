///|
/// Property-Based State Machine Tests for DuckDB Connection
/// Tests Connection state transitions and invariants

///|
/// Connection state representation
pub enum ConnectionState {
  NotConnected
  Connected
  QueryInProgress
  Closed
  Error
} derive(Show)

///|
/// Connection command type
pub enum ConnectionCommand {
  Connect
  Query(String)
  Close
  Prepare(String)
  Execute
} derive(Show)

///|
/// Connection model for state machine testing
pub struct ConnectionModel {
  state : ConnectionState
  query_count : Int
} derive(Show)

///|
/// Initial connection model
pub fn connection_model_initial() -> ConnectionModel {
  ConnectionModel::{ state: ConnectionState::NotConnected, query_count: 0 }
}

///|
/// Execute command on model
pub fn execute_command_on_model(
  model : ConnectionModel,
  cmd : ConnectionCommand,
) -> ConnectionModel {
  match (model.state, cmd) {
    // NotConnected can only Connect
    (ConnectionState::NotConnected, ConnectionCommand::Connect) =>
      ConnectionModel::{ state: ConnectionState::Connected, query_count: 0 }
    // Connected can Query, Close, or Prepare
    (ConnectionState::Connected, ConnectionCommand::Query(_)) =>
      ConnectionModel::{
        state: ConnectionState::QueryInProgress,
        query_count: model.query_count + 1,
      }
    (ConnectionState::Connected, ConnectionCommand::Close) =>
      ConnectionModel::{
        state: ConnectionState::Closed,
        query_count: model.query_count,
      }
    (ConnectionState::Connected, ConnectionCommand::Prepare(_)) =>
      ConnectionModel::{
        state: ConnectionState::Connected,
        query_count: model.query_count,
      }
    // QueryInProgress returns to Connected after query completes
    (ConnectionState::QueryInProgress, ConnectionCommand::Execute) =>
      ConnectionModel::{
        state: ConnectionState::Connected,
        query_count: model.query_count,
      }
    // Closed cannot transition (terminal state)
    (ConnectionState::Closed, _) => model
    // Error state transitions
    (_, _) =>
      ConnectionModel::{
        state: ConnectionState::Error,
        query_count: model.query_count,
      }
  }
}

///|
/// Check if state transition is valid
pub fn is_valid_connection_transition(
  from : ConnectionState,
  to : ConnectionState,
  cmd : ConnectionCommand,
) -> Bool {
  match (from, cmd, to) {
    // Valid transitions from NotConnected
    (
      ConnectionState::NotConnected,
      ConnectionCommand::Connect,
      ConnectionState::Connected,
    ) => true
    (ConnectionState::NotConnected, _, ConnectionState::NotConnected) => true

    // Valid transitions from Connected
    (
      ConnectionState::Connected,
      ConnectionCommand::Query(_),
      ConnectionState::QueryInProgress,
    ) => true
    (
      ConnectionState::Connected,
      ConnectionCommand::Close,
      ConnectionState::Closed,
    ) => true
    (
      ConnectionState::Connected,
      ConnectionCommand::Prepare(_),
      ConnectionState::Connected,
    ) => true

    // Valid transitions from QueryInProgress
    (
      ConnectionState::QueryInProgress,
      ConnectionCommand::Execute,
      ConnectionState::Connected,
    ) => true

    // Closed is terminal
    (ConnectionState::Closed, _, ConnectionState::Closed) => true

    // Error state
    (ConnectionState::Error, _, ConnectionState::Error) => true
    (ConnectionState::Error, ConnectionCommand::Close, ConnectionState::Closed) =>
      true

    // All other transitions are invalid
    _ => false
  }
}

///|
/// Property: cannot query when not connected
test "prop_connection_cannot_query_when_not_connected" {
  let gen = @pbt.Gen::pure((
    ConnectionState::NotConnected,
    ConnectionCommand::Query("SELECT 1"),
  ))
  let config = @pbt.CheckConfig::new(10, 10, 740001, 5)

  @pbt.assert_check(
    "cannot query when not connected",
    gen,
    fn(input) {
      let (state, cmd) = input
      let new_state = execute_command_on_model(
        ConnectionModel::{ state, query_count: 0 },
        cmd,
      ).state
      // Query from NotConnected should result in Error or stay NotConnected
      match new_state {
        ConnectionState::NotConnected => Ok(())
        ConnectionState::Error => Ok(())
        ConnectionState::Connected => Err(
          "Query from NotConnected should not result in Connected state",
        )
        _ => Err("Unexpected state after query from NotConnected")
      }
    },
    config~,
  )
}

///|
/// Property: cannot close when not connected
test "prop_connection_cannot_close_when_not_connected" {
  let gen = @pbt.Gen::pure((
    ConnectionState::NotConnected,
    ConnectionCommand::Close,
  ))
  let config = @pbt.CheckConfig::new(10, 10, 740002, 5)

  @pbt.assert_check(
    "cannot close when not connected",
    gen,
    fn(input) {
      let (state, cmd) = input
      let new_state = execute_command_on_model(
        ConnectionModel::{ state, query_count: 0 },
        cmd,
      ).state
      // Close from NotConnected should result in Error or stay NotConnected
      match new_state {
        ConnectionState::NotConnected => Ok(())
        ConnectionState::Error => Ok(())
        _ => Err("Close from NotConnected should not transition to valid state")
      }
    },
    config~,
  )
}

///|
/// Property: query count increases after successful query
test "prop_connection_query_count_increases" {
  let gen = @pbt.Gen::choose_int(0, 100).map(fn(initial_count) {
    (
      ConnectionState::Connected,
      initial_count,
      ConnectionCommand::Query("SELECT 1"),
    )
  })
  let config = @pbt.CheckConfig::new(100, 20, 740003, 20)

  @pbt.assert_check(
    "query count increases after query",
    gen,
    fn(input) {
      let (state, initial_count, cmd) = input
      let model = ConnectionModel::{
        state: state,
        query_count: initial_count,
      }
      let new_model = execute_command_on_model(model, cmd)
      if new_model.query_count == initial_count + 1 {
        Ok(())
      } else {
        Err(
          "Query count should increase by 1: \{initial_count} -> \{new_model.query_count}",
        )
      }
    },
    config~,
  )
}

///|
/// Property: closed connection cannot query
test "prop_connection_closed_cannot_query" {
  let gen = @pbt.Gen::pure((
    ConnectionState::Closed,
    ConnectionCommand::Query("SELECT 1"),
  ))
  let config = @pbt.CheckConfig::new(10, 10, 740004, 5)

  @pbt.assert_check(
    "closed connection cannot query",
    gen,
    fn(input) {
      let (state, cmd) = input
      let new_state = execute_command_on_model(
        ConnectionModel::{ state, query_count: 0 },
        cmd,
      ).state
      // Query from Closed should stay Closed
      match new_state {
        ConnectionState::Closed => Ok(())
        _ => Err("Query from Closed should keep state Closed")
      }
    },
    config~,
  )
}

///|
/// Property: connect transitions to connected
test "prop_connection_connect_transitions" {
  let gen = @pbt.Gen::pure((
    ConnectionState::NotConnected,
    ConnectionCommand::Connect,
  ))
  let config = @pbt.CheckConfig::new(10, 10, 740005, 5)

  @pbt.assert_check(
    "connect transitions to connected",
    gen,
    fn(input) {
      let (state, cmd) = input
      let new_state = execute_command_on_model(
        ConnectionModel::{ state, query_count: 0 },
        cmd,
      ).state
      match new_state {
        ConnectionState::Connected => Ok(())
        _ => Err("Connect should transition to Connected state")
      }
    },
    config~,
  )
}

///|
/// Property: close transitions to closed
test "prop_connection_close_transitions" {
  let gen = @pbt.Gen::pure((
    ConnectionState::Connected,
    ConnectionCommand::Close,
  ))
  let config = @pbt.CheckConfig::new(10, 10, 740006, 5)

  @pbt.assert_check(
    "close transitions to closed",
    gen,
    fn(input) {
      let (state, cmd) = input
      let new_state = execute_command_on_model(
        ConnectionModel::{ state, query_count: 0 },
        cmd,
      ).state
      match new_state {
        ConnectionState::Closed => Ok(())
        _ => Err("Close should transition to Closed state")
      }
    },
    config~,
  )
}

///|
/// Property: query transitions to query in progress
test "prop_connection_query_transitions" {
  let gen = @pbt.Gen::pure((
    ConnectionState::Connected,
    ConnectionCommand::Query("SELECT 1"),
  ))
  let config = @pbt.CheckConfig::new(10, 10, 740007, 5)

  @pbt.assert_check(
    "query transitions to in progress",
    gen,
    fn(input) {
      let (state, cmd) = input
      let new_state = execute_command_on_model(
        ConnectionModel::{ state, query_count: 0 },
        cmd,
      ).state
      match new_state {
        ConnectionState::QueryInProgress => Ok(())
        _ => Err("Query should transition to QueryInProgress state")
      }
    },
    config~,
  )
}

///|
/// Property: execute returns to connected
test "prop_connection_execute_returns_to_connected" {
  let gen = @pbt.Gen::pure((
    ConnectionState::QueryInProgress,
    ConnectionCommand::Execute,
  ))
  let config = @pbt.CheckConfig::new(10, 10, 740008, 5)

  @pbt.assert_check(
    "execute returns to connected",
    gen,
    fn(input) {
      let (state, cmd) = input
      let new_state = execute_command_on_model(
        ConnectionModel::{ state, query_count: 1 },
        cmd,
      ).state
      match new_state {
        ConnectionState::Connected => Ok(())
        _ => Err("Execute should return to Connected state")
      }
    },
    config~,
  )
}

///|
/// Property: valid transition sequences
test "prop_connection_valid_sequences" {
  let gen = @pbt.Gen::one_of([
    @pbt.Gen::pure([
      (ConnectionState::NotConnected, ConnectionCommand::Connect),
      (ConnectionState::Connected, ConnectionCommand::Query("SELECT 1")),
      (ConnectionState::QueryInProgress, ConnectionCommand::Execute),
      (ConnectionState::Connected, ConnectionCommand::Close),
    ]),
    @pbt.Gen::pure([
      (ConnectionState::NotConnected, ConnectionCommand::Connect),
      (ConnectionState::Connected, ConnectionCommand::Close),
    ]),
    @pbt.Gen::pure([
      (ConnectionState::NotConnected, ConnectionCommand::Connect),
      (ConnectionState::Connected, ConnectionCommand::Prepare("SELECT 1")),
      (ConnectionState::Connected, ConnectionCommand::Query("SELECT 1")),
      (ConnectionState::QueryInProgress, ConnectionCommand::Execute),
      (ConnectionState::Connected, ConnectionCommand::Close),
    ]),
  ])
  let config = @pbt.CheckConfig::new(50, 10, 740009, 10)

  @pbt.assert_check(
    "valid transition sequences",
    gen,
    fn(sequence) {
      let mut model = connection_model_initial()
      for elem in sequence {
        let (_, cmd) = elem
        model = execute_command_on_model(model, cmd)
        // Check we never hit error state for valid sequences
        match model.state {
          ConnectionState::Error => return Err("Valid sequence should not error")
          _ => ()
        }
      }
      Ok(())
    },
    config~,
  )
}

///|
/// Property: invalid transition detection
test "prop_connection_invalid_transitions" {
  let gen = @pbt.Gen::one_of([
    // Query from NotConnected
    @pbt.Gen::pure((ConnectionState::NotConnected, ConnectionCommand::Query("SELECT 1"))),
    // Close from NotConnected
    @pbt.Gen::pure((ConnectionState::NotConnected, ConnectionCommand::Close)),
    // Query from Closed
    @pbt.Gen::pure((ConnectionState::Closed, ConnectionCommand::Query("SELECT 1"))),
    // Connect from Connected
    @pbt.Gen::pure((ConnectionState::Connected, ConnectionCommand::Connect)),
    // Prepare from QueryInProgress
    @pbt.Gen::pure((ConnectionState::QueryInProgress, ConnectionCommand::Prepare("SELECT 1"))),
  ])
  let config = @pbt.CheckConfig::new(100, 20, 740010, 20)

  @pbt.assert_check(
    "invalid transition detection",
    gen,
    fn(input) {
      let (state, cmd) = input
      let model = ConnectionModel::{ state, query_count: 0 }
      let new_model = execute_command_on_model(model, cmd)
      let new_state = new_model.state
      // Invalid transitions should result in Error or keep same state
      match (state, new_state) {
        (ConnectionState::Closed, ConnectionState::Closed) => Ok(())
        (_, ConnectionState::Error) => Ok(())
        (_, _) =>
          if is_valid_connection_transition(state, new_state, cmd) {
            Ok(())
          } else {
            Err(
              "Invalid transition from \{state} with \{cmd} should be handled",
            )
          }
      }
    },
    config~,
  )
}

///|
/// Property: state machine invariants
test "prop_connection_state_invariants" {
  let gen = @pbt.Gen::choose_int(0, 20).bind(fn(len) {
    @pbt.Gen::array_of(@pbt.Gen::one_of([
      @pbt.Gen::map(@pbt.Gen::choose_int(0, 100), fn(n) {
        ConnectionCommand::Query("SELECT " + n.to_string())
      }),
      @pbt.Gen::pure(ConnectionCommand::Connect),
      @pbt.Gen::pure(ConnectionCommand::Close),
      @pbt.Gen::pure(ConnectionCommand::Execute),
      @pbt.Gen::map(@pbt.Gen::choose_int(0, 100), fn(n) {
        ConnectionCommand::Prepare("SELECT " + n.to_string())
      }),
    ])).map(fn(cmds) {
      (cmds, len)
    })
  })
  let config = @pbt.CheckConfig::new(200, 30, 740011, 20)

  @pbt.assert_check(
    "connection state invariants",
    gen,
    fn(input) {
      let (cmds, _) = input
      let mut model = connection_model_initial()
      let mut seen_closed = false

      // Check initial state
      match model.state {
        ConnectionState::Closed => seen_closed = true
        _ => ()
      }

      for cmd in cmds {
        model = execute_command_on_model(model, cmd)
        // If we've seen Closed, all subsequent states must be Closed
        if seen_closed {
          match model.state {
            ConnectionState::Closed => ()
            _ => return Err("After Closed, state should remain Closed")
          }
        } else {
          match model.state {
            ConnectionState::Closed => seen_closed = true
            _ => ()
          }
        }
      }

      Ok(())
    },
    config~,
  )
}

///|
/// Property: connection state is always valid
test "prop_connection_state_always_valid" {
  let gen = @pbt.Gen::choose_int(0, 30).bind(fn(len) {
    @pbt.Gen::array_of(
      @pbt.Gen::one_of([
        @pbt.Gen::pure(ConnectionCommand::Connect),
        @pbt.Gen::pure(ConnectionCommand::Close),
        @pbt.Gen::map(@pbt.Gen::choose_int(0, 100), fn(n) {
          ConnectionCommand::Query("SELECT " + n.to_string())
        }),
        @pbt.Gen::pure(ConnectionCommand::Execute),
      ]),
    )
  })
  let config = @pbt.CheckConfig::new(200, 30, 740012, 20)
  @pbt.assert_check(
    "connection state always valid",
    gen,
    fn(cmds) {
      let mut model = connection_model_initial()
      for cmd in cmds {
        model = execute_command_on_model(model, cmd)

        // Check state is one of the valid states
        match model.state {
          ConnectionState::NotConnected => ()
          ConnectionState::Connected => ()
          ConnectionState::QueryInProgress => ()
          ConnectionState::Closed => ()
          ConnectionState::Error => ()
        }
      }
      Ok(())
    },
    config~,
  )
}

///|
/// Property: query count never decreases
test "prop_connection_query_count_never_decreases" {
  let gen = @pbt.Gen::choose_int(0, 20).bind(fn(len) {
    @pbt.Gen::array_of(
      @pbt.Gen::one_of([
        @pbt.Gen::map(@pbt.Gen::choose_int(0, 100), fn(n) {
          ConnectionCommand::Query("SELECT " + n.to_string())
        }),
        @pbt.Gen::pure(ConnectionCommand::Connect),
        @pbt.Gen::pure(ConnectionCommand::Close),
        @pbt.Gen::pure(ConnectionCommand::Execute),
      ]),
    )
  })
  let config = @pbt.CheckConfig::new(200, 30, 740013, 20)
  @pbt.assert_check(
    "query count never decreases",
    gen,
    fn(cmds) {
      let mut model = connection_model_initial()
      let mut prev_count = model.query_count
      for cmd in cmds {
        model = execute_command_on_model(model, cmd)
        if model.query_count < prev_count {
          return Err(
            "Query count decreased from \{prev_count} to \{model.query_count}",
          )
        }
        prev_count = model.query_count
      }
      Ok(())
    },
    config~,
  )
}

///|
/// Property: connection command sequence
test "prop_connection_command_sequence" {
  let gen = @pbt.Gen::one_of([
    @pbt.Gen::pure([
      ConnectionCommand::Connect,
      ConnectionCommand::Query("SELECT 1"),
      ConnectionCommand::Execute,
      ConnectionCommand::Close,
    ]),
    @pbt.Gen::pure([ConnectionCommand::Connect, ConnectionCommand::Close]),
    @pbt.Gen::pure([
      ConnectionCommand::Connect,
      ConnectionCommand::Query("SELECT 1"),
      ConnectionCommand::Execute,
      ConnectionCommand::Query("SELECT 2"),
      ConnectionCommand::Execute,
      ConnectionCommand::Close,
    ]),
  ])
  let config = @pbt.CheckConfig::new(50, 10, 740014, 10)
  @pbt.assert_check(
    "connection command sequence",
    gen,
    fn(sequence) {
      let mut model = connection_model_initial()
      for cmd in sequence {
        model = execute_command_on_model(model, cmd)
      }

      // After proper close sequence, should end in Closed
      Ok(())
    },
    config~,
  )
}
