::@echo off
jar cf %1.jar %1*.class
call hadoop fs -rm -r %3
call hadoop jar %1.jar %1 %2 %3