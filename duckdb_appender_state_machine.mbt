///|
/// Property-Based State Machine Tests for DuckDB Appender
/// Tests Appender state transitions and invariants

///|
/// Appender state representation
pub enum AppenderState {
  NotCreated
  Ready
  RowInProgress
  Flushed
  Closed
  Error
} derive(Show)

///|
/// Appender command type
pub enum AppenderCommand {
  Create(String) // table name
  BeginRow
  AppendInt(Int)
  AppendDouble(Double)
  AppendString(String)
  AppendBool(Bool)
  EndRow
  Flush
  Close
} derive(Show)

///|
/// Appender model for state machine testing
pub struct AppenderModel {
  state : AppenderState
  column_count : Int
  expected_columns : Int
  row_count : Int
  flushed_row_count : Int
} derive(Show)

///|
/// Initial appender model
pub fn appender_model_initial(expected_columns : Int) -> AppenderModel {
  AppenderModel::{
    state: AppenderState::NotCreated,
    column_count: 0,
    expected_columns,
    row_count: 0,
    flushed_row_count: 0,
  }
}

///|
/// Execute command on model
pub fn execute_appender_command_on_model(
  model : AppenderModel,
  cmd : AppenderCommand,
) -> AppenderModel {
  match (model.state, cmd) {
    // NotCreated can only Create
    (AppenderState::NotCreated, AppenderCommand::Create(_)) =>
      AppenderModel::{
        state: AppenderState::Ready,
        column_count: 0,
        expected_columns: model.expected_columns,
        row_count: 0,
        flushed_row_count: 0,
      }
    // Ready can BeginRow or Flush or Close
    (AppenderState::Ready, AppenderCommand::BeginRow) =>
      AppenderModel::{
        state: AppenderState::RowInProgress,
        column_count: 0,
        expected_columns: model.expected_columns,
        row_count: model.row_count,
        flushed_row_count: model.flushed_row_count,
      }
    (AppenderState::Ready, AppenderCommand::Flush) =>
      AppenderModel::{
        state: AppenderState::Flushed,
        column_count: 0,
        expected_columns: model.expected_columns,
        row_count: model.row_count,
        flushed_row_count: model.row_count,
      }
    (AppenderState::Ready, AppenderCommand::Close) =>
      AppenderModel::{
        state: AppenderState::Closed,
        column_count: 0,
        expected_columns: model.expected_columns,
        row_count: model.row_count,
        flushed_row_count: model.flushed_row_count,
      }
    // RowInProgress can Append* or EndRow
    (AppenderState::RowInProgress, AppenderCommand::AppendInt(_)) =>
      if model.column_count < model.expected_columns {
        AppenderModel::{
          state: AppenderState::RowInProgress,
          column_count: model.column_count + 1,
          expected_columns: model.expected_columns,
          row_count: model.row_count,
          flushed_row_count: model.flushed_row_count,
        }
      } else {
        AppenderModel::{
          state: AppenderState::Error,
          column_count: model.column_count,
          expected_columns: model.expected_columns,
          row_count: model.row_count,
          flushed_row_count: model.flushed_row_count,
        }
      }
    (AppenderState::RowInProgress, AppenderCommand::AppendDouble(_)) =>
      if model.column_count < model.expected_columns {
        AppenderModel::{
          state: AppenderState::RowInProgress,
          column_count: model.column_count + 1,
          expected_columns: model.expected_columns,
          row_count: model.row_count,
          flushed_row_count: model.flushed_row_count,
        }
      } else {
        AppenderModel::{
          state: AppenderState::Error,
          column_count: model.column_count,
          expected_columns: model.expected_columns,
          row_count: model.row_count,
          flushed_row_count: model.flushed_row_count,
        }
      }
    (AppenderState::RowInProgress, AppenderCommand::AppendString(_)) =>
      if model.column_count < model.expected_columns {
        AppenderModel::{
          state: AppenderState::RowInProgress,
          column_count: model.column_count + 1,
          expected_columns: model.expected_columns,
          row_count: model.row_count,
          flushed_row_count: model.flushed_row_count,
        }
      } else {
        AppenderModel::{
          state: AppenderState::Error,
          column_count: model.column_count,
          expected_columns: model.expected_columns,
          row_count: model.row_count,
          flushed_row_count: model.flushed_row_count,
        }
      }
    (AppenderState::RowInProgress, AppenderCommand::AppendBool(_)) =>
      if model.column_count < model.expected_columns {
        AppenderModel::{
          state: AppenderState::RowInProgress,
          column_count: model.column_count + 1,
          expected_columns: model.expected_columns,
          row_count: model.row_count,
          flushed_row_count: model.flushed_row_count,
        }
      } else {
        AppenderModel::{
          state: AppenderState::Error,
          column_count: model.column_count,
          expected_columns: model.expected_columns,
          row_count: model.row_count,
          flushed_row_count: model.flushed_row_count,
        }
      }
    (AppenderState::RowInProgress, AppenderCommand::EndRow) =>
      if model.column_count == model.expected_columns {
        AppenderModel::{
          state: AppenderState::Ready,
          column_count: 0,
          expected_columns: model.expected_columns,
          row_count: model.row_count + 1,
          flushed_row_count: model.flushed_row_count,
        }
      } else {
        AppenderModel::{
          state: AppenderState::Error,
          column_count: model.column_count,
          expected_columns: model.expected_columns,
          row_count: model.row_count,
          flushed_row_count: model.flushed_row_count,
        }
      }
    (AppenderState::RowInProgress, AppenderCommand::Close) =>
      AppenderModel::{
        state: AppenderState::Closed,
        column_count: model.column_count,
        expected_columns: model.expected_columns,
        row_count: model.row_count,
        flushed_row_count: model.flushed_row_count,
      }
    // Flushed can go back to Ready or stay Flushed or Close
    (AppenderState::Flushed, AppenderCommand::Flush) =>
      AppenderModel::{
        state: AppenderState::Flushed,
        column_count: 0,
        expected_columns: model.expected_columns,
        row_count: model.row_count,
        flushed_row_count: model.row_count,
      }
    (AppenderState::Flushed, AppenderCommand::BeginRow) =>
      AppenderModel::{
        state: AppenderState::RowInProgress,
        column_count: 0,
        expected_columns: model.expected_columns,
        row_count: model.row_count,
        flushed_row_count: model.flushed_row_count,
      }
    (AppenderState::Flushed, AppenderCommand::Close) =>
      AppenderModel::{
        state: AppenderState::Closed,
        column_count: 0,
        expected_columns: model.expected_columns,
        row_count: model.row_count,
        flushed_row_count: model.flushed_row_count,
      }
    // Closed is terminal
    (AppenderState::Closed, _) => model
    // Error state - terminal except for Close
    (AppenderState::Error, AppenderCommand::Close) =>
      AppenderModel::{
        state: AppenderState::Closed,
        column_count: model.column_count,
        expected_columns: model.expected_columns,
        row_count: model.row_count,
        flushed_row_count: model.flushed_row_count,
      }
    (AppenderState::Error, _) => model
    // All other transitions result in Error
    (_, _) =>
      AppenderModel::{
        state: AppenderState::Error,
        column_count: model.column_count,
        expected_columns: model.expected_columns,
        row_count: model.row_count,
        flushed_row_count: model.flushed_row_count,
      }
  }
}

///|
/// Property: cannot append before begin row
test "prop_appender_cannot_append_before_begin_row" {
  let gen = @pbt.Gen::pure((
    AppenderState::Ready,
    AppenderCommand::AppendInt(42),
  ))
  let config = @pbt.CheckConfig::new(10, 10, 750001, 5)

  @pbt.assert_check(
    "cannot append before begin row",
    gen,
    fn(input) {
      let (state, cmd) = input
      let init_model = appender_model_initial(3)
      let model_with_state = AppenderModel::{
        state: state,
        column_count: 0,
        expected_columns: 3,
        row_count: 0,
        flushed_row_count: 0,
      }
      let new_model = execute_appender_command_on_model(model_with_state, cmd)
      // Append from Ready should result in Error
      match new_model.state {
        AppenderState::Error => Ok(())
        _ => Err("Append from Ready should result in Error state")
      }
    },
    config~,
  )
}

///|
/// Property: must call end row after all columns
test "prop_appender_must_end_row_after_columns" {
  let gen = @pbt.Gen::choose_int(1, 10).map(fn(col_count) {
    (
      AppenderState::RowInProgress,
      col_count,
      AppenderCommand::EndRow,
    )
  })
  let config = @pbt.CheckConfig::new(100, 20, 750002, 20)

  @pbt.assert_check(
    "must call end row after all columns",
    gen,
    fn(input) {
      let (state, expected_cols, cmd) = input
      let model = AppenderModel::{
        state: state,
        column_count: expected_cols, // All columns appended
        expected_columns: expected_cols,
        row_count: 0,
        flushed_row_count: 0,
      }
      let new_model = execute_appender_command_on_model(model, cmd)
      // EndRow with correct column count should succeed
      match new_model.state {
        AppenderState::Ready => Ok(())
        AppenderState::Error => Err(
          "EndRow with correct column count should not error",
        )
        _ => Err("Unexpected state after EndRow")
      }
    },
    config~,
  )
}

///|
/// Property: flush preserves row count
test "prop_appender_flush_preserves_row_count" {
  let gen = @pbt.Gen::choose_int(0, 100).map(fn(row_count) {
    let model = AppenderModel::{
      state: AppenderState::Ready,
      column_count: 0,
      expected_columns: 3,
      row_count: row_count,
      flushed_row_count: 0,
    }
    (model, row_count)
  })
  let config = @pbt.CheckConfig::new(200, 50, 750003, 30)

  @pbt.assert_check(
    "flush preserves row count",
    gen,
    fn(input) {
      let (model, expected_count) = input
      let new_model = execute_appender_command_on_model(
        model,
        AppenderCommand::Flush,
      )
      // Row count should be preserved
      if new_model.row_count == expected_count {
        Ok(())
      } else {
        Err(
          "Row count changed from \{expected_count} to \{new_model.row_count}",
        )
      }
    },
    config~,
  )
}

///|
/// Property: row count increases only after end row and flush
test "prop_appender_row_count_increases" {
  let gen = @pbt.Gen::choose_int(1, 10).map(fn(col_count) {
    appender_model_initial(col_count)
  })
  let config = @pbt.CheckConfig::new(100, 20, 750004, 20)

  @pbt.assert_check(
    "row count increases correctly",
    gen,
    fn(model) {
      let expected_cols = model.expected_columns

      // Sequence: Create -> BeginRow -> Append... -> EndRow -> Flush
      let after_create = execute_appender_command_on_model(
        model,
        AppenderCommand::Create("test_table"),
      )

      let after_begin = execute_appender_command_on_model(
        after_create,
        AppenderCommand::BeginRow,
      )

      // Append columns
      let mut after_append = after_begin
      let mut i = 0
      while i < expected_cols {
        after_append = execute_appender_command_on_model(
          after_append,
          AppenderCommand::AppendInt(i),
        )
        i = i + 1
      }

      let after_end = execute_appender_command_on_model(
        after_append,
        AppenderCommand::EndRow,
      )

      let after_flush = execute_appender_command_on_model(
        after_end,
        AppenderCommand::Flush,
      )

      // After EndRow, row_count should be 1
      if after_end.row_count != 1 {
        return Err(
          "Row count should be 1 after EndRow, got \{after_end.row_count}",
        )
      }

      // After Flush, flushed_row_count should be 1
      if after_flush.flushed_row_count != 1 {
        return Err(
          "Flushed row count should be 1, got \{after_flush.flushed_row_count}",
        )
      }

      Ok(())
    },
    config~,
  )
}

///|
/// Property: create transitions to ready
test "prop_appender_create_transitions" {
  let gen = @pbt.Gen::pure((
    AppenderState::NotCreated,
    AppenderCommand::Create("test_table"),
  ))
  let config = @pbt.CheckConfig::new(10, 10, 750005, 5)

  @pbt.assert_check(
    "create transitions to ready",
    gen,
    fn(input) {
      let (state, cmd) = input
      let model = appender_model_initial(3)
      let new_model = execute_appender_command_on_model(model, cmd)
      match new_model.state {
        AppenderState::Ready => Ok(())
        _ => Err("Create should transition to Ready state")
      }
    },
    config~,
  )
}

///|
/// Property: begin row transitions to row in progress
test "prop_appender_begin_row_transitions" {
  let gen = @pbt.Gen::pure((
    AppenderState::Ready,
    AppenderCommand::BeginRow,
  ))
  let config = @pbt.CheckConfig::new(10, 10, 750006, 5)

  @pbt.assert_check(
    "begin row transitions to row in progress",
    gen,
    fn(input) {
      let (state, cmd) = input
      let model = AppenderModel::{
        state: state,
        column_count: 0,
        expected_columns: 3,
        row_count: 0,
        flushed_row_count: 0,
      }
      let new_model = execute_appender_command_on_model(model, cmd)
      match new_model.state {
        AppenderState::RowInProgress => Ok(())
        _ => Err("BeginRow should transition to RowInProgress state")
      }
    },
    config~,
  )
}

///|
/// Property: close transitions to closed
test "prop_appender_close_transitions" {
  let gen = @pbt.Gen::one_of([
    @pbt.Gen::pure((
      AppenderState::Ready,
      AppenderCommand::Close,
    )),
    @pbt.Gen::pure((
      AppenderState::RowInProgress,
      AppenderCommand::Close,
    )),
    @pbt.Gen::pure((
      AppenderState::Flushed,
      AppenderCommand::Close,
    )),
  ])
  let config = @pbt.CheckConfig::new(30, 10, 750007, 10)

  @pbt.assert_check(
    "close transitions to closed",
    gen,
    fn(input) {
      let (state, cmd) = input
      let model = AppenderModel::{
        state: state,
        column_count: 0,
        expected_columns: 3,
        row_count: 0,
        flushed_row_count: 0,
      }
      let new_model = execute_appender_command_on_model(model, cmd)
      match new_model.state {
        AppenderState::Closed => Ok(())
        _ => Err("Close should transition to Closed state")
      }
    },
    config~,
  )
}

///|
/// Property: appending beyond column count errors
test "prop_appender_append_beyond_columns_errors" {
  let gen = @pbt.Gen::choose_int(1, 10).map(fn(col_count) {
    let model = AppenderModel::{
      state: AppenderState::RowInProgress,
      column_count: col_count, // Already at max
      expected_columns: col_count,
      row_count: 0,
      flushed_row_count: 0,
    }
    (model, col_count)
  })
  let config = @pbt.CheckConfig::new(100, 20, 750008, 20)

  @pbt.assert_check(
    "appending beyond column count errors",
    gen,
    fn(input) {
      let (model, col_count) = input
      let new_model = execute_appender_command_on_model(
        model,
        AppenderCommand::AppendInt(42),
      )
      // Should result in Error state
      match new_model.state {
        AppenderState::Error => Ok(())
        _ => Err(
          "Appending beyond column count should result in Error state",
        )
      }
    },
    config~,
  )
}

///|
/// Property: end row with incomplete columns errors
test "prop_appender_end_row_incomplete_errors" {
  let gen = @pbt.Gen::choose_int(1, 10).bind(fn(col_count) {
    @pbt.Gen::choose_int(0, col_count - 1).map(fn(current_count) {
      let model = AppenderModel::{
        state: AppenderState::RowInProgress,
        column_count: current_count, // Incomplete
        expected_columns: col_count,
        row_count: 0,
        flushed_row_count: 0,
      }
      (model, col_count, current_count)
    })
  })
  let config = @pbt.CheckConfig::new(100, 20, 750009, 20)

  @pbt.assert_check(
    "end row with incomplete columns errors",
    gen,
    fn(input) {
      let (model, col_count, current_count) = input
      let new_model = execute_appender_command_on_model(
        model,
        AppenderCommand::EndRow,
      )
      // Should result in Error state since columns incomplete
      match new_model.state {
        AppenderState::Error => Ok(())
        _ => Err(
          "EndRow with incomplete columns should result in Error state",
        )
      }
    },
    config~,
  )
}

///|
/// Property: closed appender cannot accept commands
test "prop_appender_closed_no_commands" {
  let gen = @pbt.Gen::one_of([
    @pbt.Gen::pure(AppenderCommand::BeginRow),
    @pbt.Gen::pure(AppenderCommand::AppendInt(42)),
    @pbt.Gen::pure(AppenderCommand::EndRow),
    @pbt.Gen::pure(AppenderCommand::Flush),
  ])
  let config = @pbt.CheckConfig::new(50, 10, 750010, 10)

  @pbt.assert_check(
    "closed appender cannot accept commands",
    gen,
    fn(cmd) {
      let model = AppenderModel::{
        state: AppenderState::Closed,
        column_count: 0,
        expected_columns: 3,
        row_count: 0,
        flushed_row_count: 0,
      }
      let new_model = execute_appender_command_on_model(model, cmd)
      // Should stay Closed
      match new_model.state {
        AppenderState::Closed => Ok(())
        _ => Err("Close should transition to Closed state")
      }
    },
    config~,
  )
}

///|
/// Property: valid appender sequence
test "prop_appender_valid_sequence" {
  let gen = @pbt.Gen::choose_int(1, 5).map(fn(col_count) {
    (
      col_count,
      [
        AppenderCommand::Create("test_table"),
        AppenderCommand::BeginRow,
        AppenderCommand::EndRow,
        AppenderCommand::Flush,
        AppenderCommand::Close,
      ],
    )
  })
  let config = @pbt.CheckConfig::new(50, 10, 750011, 10)

  @pbt.assert_check(
    "valid appender sequence",
    gen,
    fn(input) {
      let (col_count, sequence) = input
      let mut model = appender_model_initial(col_count)

      for cmd in sequence {
        model = execute_appender_command_on_model(model, cmd)

        // Check we never hit error
        match model.state {
          AppenderState::Error => return Err("Sequence should not error")
          _ => ()
        }
      }

      // Should end in Closed
      match model.state {
        AppenderState::Closed => Ok(())
        _ => Err("Sequence should end in Closed state")
      }
    },
    config~,
  )
}

///|
/// Property: multiple rows can be appended
test "prop_appender_multiple_rows" {
  let gen = @pbt.Gen::choose_int(1, 10).map(fn(row_count) {
    (row_count, 3) // 3 columns
  })
  let config = @pbt.CheckConfig::new(100, 20, 750012, 20)

  @pbt.assert_check(
    "multiple rows can be appended",
    gen,
    fn(input) {
      let (row_count, col_count) = input
      let mut model = appender_model_initial(col_count)

      // Create
      model = execute_appender_command_on_model(
        model,
        AppenderCommand::Create("test_table"),
      )

      // Append rows
      let mut i = 0
      while i < row_count {
        // BeginRow
        model = execute_appender_command_on_model(
          model,
          AppenderCommand::BeginRow,
        )

        // Append columns
        let mut j = 0
        while j < col_count {
          model = execute_appender_command_on_model(
            model,
            AppenderCommand::AppendInt(j),
          )
          j = j + 1
        }

        // EndRow
        model = execute_appender_command_on_model(
          model,
          AppenderCommand::EndRow,
        )

        i = i + 1
      }

      // Flush
      model = execute_appender_command_on_model(
        model,
        AppenderCommand::Flush,
      )

      // Check row count
      if model.row_count == row_count &&
        model.flushed_row_count == row_count {
        Ok(())
      } else {
        Err(
          "Row count mismatch: expected \{row_count}, got row_count=\{model.row_count}, flushed=\{model.flushed_row_count}",
        )
      }
    },
    config~,
  )
}

///|
/// Property: column count resets after end row
test "prop_appender_column_count_resets" {
  let gen = @pbt.Gen::choose_int(1, 10).map(fn(col_count) {
    appender_model_initial(col_count)
  })
  let config = @pbt.CheckConfig::new(100, 20, 750013, 20)

  @pbt.assert_check(
    "column count resets after end row",
    gen,
    fn(model) {
      let col_count = model.expected_columns

      // Create and begin row
      let mut m = execute_appender_command_on_model(
        model,
        AppenderCommand::Create("test_table"),
      )
      m = execute_appender_command_on_model(m, AppenderCommand::BeginRow)

      // Append all columns
      let mut i = 0
      while i < col_count {
        m = execute_appender_command_on_model(m, AppenderCommand::AppendInt(i))
        i = i + 1
      }

      // EndRow
      m = execute_appender_command_on_model(m, AppenderCommand::EndRow)

      // Column count should be 0 after EndRow
      if m.column_count == 0 {
        Ok(())
      } else {
        Err(
          "Column count should reset to 0 after EndRow, got \{m.column_count}",
        )
      }
    },
    config~,
  )
}

///|
/// Property: appender state is always valid
test "prop_appender_state_always_valid" {
  let gen = @pbt.Gen::choose_int(1, 5).bind(fn(col_count) {
    @pbt.Gen::choose_int(0, 30).bind(fn(len) {
      @pbt.Gen::array_of(@pbt.Gen::one_of([
        @pbt.Gen::pure(AppenderCommand::Create("test_table")),
        @pbt.Gen::pure(AppenderCommand::BeginRow),
        @pbt.Gen::pure(AppenderCommand::AppendInt(42)),
        @pbt.Gen::pure(AppenderCommand::EndRow),
        @pbt.Gen::pure(AppenderCommand::Flush),
        @pbt.Gen::pure(AppenderCommand::Close),
      ])).map(fn(cmds) {
        (col_count, cmds)
      })
    })
  })
  let config = @pbt.CheckConfig::new(200, 30, 750014, 20)

  @pbt.assert_check(
    "appender state always valid",
    gen,
    fn(input) {
      let (col_count, cmds) = input
      let mut model = appender_model_initial(col_count)

      for cmd in cmds {
        model = execute_appender_command_on_model(model, cmd)

        // Check state is one of the valid states
        match model.state {
          AppenderState::NotCreated => ()
          AppenderState::Ready => ()
          AppenderState::RowInProgress => ()
          AppenderState::Flushed => ()
          AppenderState::Closed => ()
          AppenderState::Error => ()
        }
      }

      Ok(())
    },
    config~,
  )
}
