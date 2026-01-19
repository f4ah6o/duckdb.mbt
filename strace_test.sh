#!/bin/bash
strace -f -e trace=execve -s 200 moon test --target native 2>&1 | grep "execve.*duckdb.blackbox_test.exe" | head -5
