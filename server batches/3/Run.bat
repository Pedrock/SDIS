@ECHO OFF
for %%a in (.) do set currentfolder=%%~na
ECHO %currentfolder%
java -classpath ../../bin Peer %currentfolder% "224.0.0.0" 4445 "224.0.0.0" 4446 "224.0.0.0" 4447