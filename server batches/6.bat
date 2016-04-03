@ECHO OFF
ECHO %~n0
java -classpath ../bin Peer %~n0 "224.0.0.3" 4445 "224.0.0.4" 4446 "224.0.0.5" 4447