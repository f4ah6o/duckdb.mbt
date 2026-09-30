///|
/// Runtime conformance for `Connection::capabilities` on the native backend:
/// the capability table must agree with what the backend actually does.
test "native connection reports native backend capabilities" {
  let caps_ref : Ref[BackendCapabilities?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        caps_ref.val = Some(conn.capabilities())
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => error_ref.val = Some("connect failed: \{err.message()}")
    }
  })
  if error_ref.val is Some(msg) {
    fail(msg)
  }
  match caps_ref.val {
    Some(caps) => {
      inspect(caps.backend, content="Native")
      inspect(caps.backend.name(), content="native")
      inspect(caps.supports(BackendFeature::Appender), content="true")
      inspect(caps.supports(BackendFeature::Streaming), content="true")
      inspect(caps.persistent_storage, content="true")
    }
    None => fail("connection reported no capabilities")
  }
}

///|
test "native appender works where capabilities report it" {
  let done_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) =>
        if !conn.capabilities().supports(BackendFeature::Appender) {
          done_ref.val = Some("appender unexpectedly gated")
          conn.close(on_done=fn(_) { () })
        } else {
          conn.query("CREATE TABLE caps_probe (v INTEGER)", on_done=fn(
            created,
          ) {
            match created {
              Ok(_) =>
                conn.create_appender("", "caps_probe", on_done=fn(
                  appender_result,
                ) {
                  match appender_result {
                    Ok(appender) => {
                      let mut ok = appender.begin_row() is Ok(_)
                      ok = ok && appender.append_int(1) is Ok(_)
                      ok = ok && appender.end_row() is Ok(_)
                      ok = ok && appender.flush() is Ok(_)
                      appender.close(on_done=fn(_) {
                        conn.close(on_done=fn(_) {
                          done_ref.val = Some(
                            if ok {
                              "ok"
                            } else {
                              "append failed"
                            },
                          )
                        })
                      })
                    }
                    Err(err) => {
                      done_ref.val = Some("appender failed: \{err.message()}")
                      conn.close(on_done=fn(_) { () })
                    }
                  }
                })
              Err(err) => {
                done_ref.val = Some("create failed: \{err.message()}")
                conn.close(on_done=fn(_) { () })
              }
            }
          })
        }
      Err(err) => done_ref.val = Some("connect failed: \{err.message()}")
    }
  })
  inspect(
    done_ref.val,
    content=(
      #|Some(ok)
    ),
  )
}
