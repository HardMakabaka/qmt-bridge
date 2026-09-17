@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0manage-bridge.ps1" start -Foreground %*
exit /b %ERRORLEVEL%
