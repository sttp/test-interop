@echo off
REM Always run with Configuration=Development - see build.cmd for rationale.
dotnet run -c Development -- %*
