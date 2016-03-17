@ECHO OFF
for %%a in (.) do set currentfolder=%%~na
ECHO %currentfolder%
java -classpath ../../bin Server %currentfolder%