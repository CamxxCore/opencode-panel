@echo off
rem Starts the OpenCode Panel on 127.0.0.1:7777. Arguments pass through to the script.
start "" /B python "%~dp0opencode-panel.py" %*
