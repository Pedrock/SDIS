@ECHO OFF
for %%a in (.) do set currentfolder=%%~na
ECHO %currentfolder%
java -classpath ../../bin Peer %currentfolder% "224.0.0.3" 4445 "224.0.0.4" 4446 "224.0.0.5" 4447