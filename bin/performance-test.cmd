@REM Copyright (C) 2020-2026 Confluent, Inc.
@REM
@REM Run only the performance test suite (tests tagged @Tag("performance")).
@REM These are excluded from the regular CI build because they take a long time
@REM and need substantial hardware. Used by the self-hosted Windows runner.
@REM
@REM Usage: bin\performance-test.cmd [extra-maven-args...]

@echo off
setlocal

call mvnw.cmd --batch-mode ^
  -Pci ^
  clean verify ^
  -Dincluded.groups=performance ^
  -Dexcluded.groups= ^
  -Dlicense.skip ^
  %*
