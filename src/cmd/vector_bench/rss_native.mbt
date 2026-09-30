///|
/// Peak RSS via `getrusage(2)` — no C stub required; links straight to libc.
#borrow(usage)
extern "C" fn c_getrusage(who : Int, usage : FixedArray[Int64]) -> Int = "getrusage"

///|
/// Peak resident set size in KiB (Linux `ru_maxrss` of RUSAGE_SELF — the
/// same value `/proc/self/status` reports as `VmHWM`).
fn peak_rss_kb() -> Int64 {
  // struct rusage: ru_utime, ru_stime (2 longs each), then ru_maxrss.
  let usage = FixedArray::make(20, 0L)
  if c_getrusage(0, usage) != 0 {
    return -1L
  }
  usage[4]
}
