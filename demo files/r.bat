@echo off
echo ---------------------------
echo r naming
echo r storage 127.0.0.1 127.0.0.1 s1
echo r cd 127.0.0.1 /
echo ---------------------------
echo 

java -jar ddu-dfs.jar %1 %2 %3 %4 %5 %6 %7 %8 %9

if %1 == cd (
	set DFSHOST=%2
	echo DFSHOST is %DFSHOST%
	set DFSCWD=%3
	echo DFSCWD is %DFSCWD%
);
