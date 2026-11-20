@echo off
setlocal
if exist "C:\opt\lakewire\cli.jar" (
  set "JAR=C:\opt\lakewire\cli.jar"
) else if exist "%~dp0..\lakewire-cli\target\lakewire-cli-0.1.0-SNAPSHOT-bin.jar" (
  set "JAR=%~dp0..\lakewire-cli\target\lakewire-cli-0.1.0-SNAPSHOT-bin.jar"
) else (
  echo lakewire jar not found. Build with mvnw -pl lakewire-cli -am package.
  exit /b 1
)
java %JAVA_OPTS% -jar "%JAR%" %*
