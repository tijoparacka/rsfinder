# rsfinder
1. Replace the resources folder with the hbase-site.xml , core-site.xxml and  hdfs-site.xml
2. package with mvn package 
3. Execute  Regionfinder with program argument <tablename > <row key> 

eg java -cp <hbase client jar , hbase-common ,guava and phoenix core >  Regionfinder STUDENTS 1 3



