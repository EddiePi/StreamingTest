#!/bin/bash
#~/spark-Tracing/bin/spark-submit --class Simple \ 
#--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/home/eddie/StreamingTest/log4j-streaming.properties" \ 
#--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/home/eddie/StreamingTest/log4j-streaming.properties" \ 
#target/1.0-1.0-SNAPSHOT-jar-with-dependencies.jar 
/home/eddie/spark-Tracing/bin/spark-submit --files /home/eddie/StreamingTest/log4j-streaming.properties --properties-file /home/eddie/StreamingTest/spark-streaming.conf --class Simple target/1.0-1.0-SNAPSHOT-jar-with-dependencies.jar 2>/dev/null
