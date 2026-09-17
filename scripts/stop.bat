@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0manage-bridge.ps1" stop %*
exit /b %ERRORLEVEL%
