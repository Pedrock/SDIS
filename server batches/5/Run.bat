@ECHO OFF
for %%a in (.) do set currentfolder=%%~na
java -classpath ../../bin main.Interface %currentfolder%