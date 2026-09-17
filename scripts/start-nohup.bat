@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0manage-bridge.ps1" start %*
exit /b %ERRORLEVEL%
