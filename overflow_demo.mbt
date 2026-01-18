// Minimal reproduction of int32_t overflow in native backend
// MoonBit Int type is supposed to be 64-bit, but native backend uses int32_t
//
// To reproduce, move this to a main package and run:
//
//   let date_days = 43553
//   let date_seconds = date_days * 86400
//   let date_micros = date_seconds * 1000000
//
// Expected: date_micros = 3762995200000000
// Actual (with overflow): date_micros = -561815552
