@ECHO OFF
for %%a in (.) do set currentfolder=%%~na
ECHO %currentfolder%
java -classpath ../../bin main.Interface %currentfolder%