///|
/// Peak RSS via `getrusage(2)` — no C stub required; links straight to libc.
#borrow(usage)
extern "C" fn c_getrusage(who : Int, usage : FixedArray[Int64]) -> Int = "getrusage"

///|
/// `uname(2)` — POSIX on both Linux and Darwin; `sysname` distinguishes
/// them. utsname char fields are 65 bytes on Linux and 256 on Darwin, so a
/// 2048-byte buffer covers either layout.
#borrow(buf)
extern "C" fn c_uname(buf : FixedArray[Byte]) -> Int = "uname"

///|
/// Peak resident set size in KiB. Linux reports `ru_maxrss` in KiB — the
/// same value `/proc/self/status` reports as `VmHWM` — while Darwin reports
/// it in bytes (XNU `getrusage.2`), so we normalize by 1024 there.
fn peak_rss_kb() -> Int64 {
  // struct rusage: ru_utime, ru_stime (2 longs each), then ru_maxrss.
  let usage = FixedArray::make(20, 0L)
  if c_getrusage(0, usage) != 0 {
    return -1L
  }
  if is_darwin() {
    usage[4] / 1024
  } else {
    usage[4]
  }
}

///|
/// Whether the host kernel is Darwin (macOS), per `uname(2)` `sysname`.
fn is_darwin() -> Bool {
  let buf = FixedArray::make(2048, b'\x00')
  if c_uname(buf) != 0 {
    return false
  }
  @encoding/utf8.decode_lossy(Bytes::from_fixedarray(buf)).has_prefix("Darwin")
}
