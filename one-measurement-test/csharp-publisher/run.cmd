@echo off
REM Always run with Configuration=Development - see build.cmd for rationale.
REM All extra args (e.g. --auto, --no-tssc, --once) pass through to the program after `--`.
dotnet run -c Development -- %*
