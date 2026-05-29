@echo off
REM Always build the buffer-block publisher with Configuration=Development so the local
REM gsfapi sources (and their live Gemstone ProjectReferences) are used.
dotnet build -c Development %*
