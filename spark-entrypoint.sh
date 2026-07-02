#!/bin/sh
# spark.driver.memory can't be set from inside the driver JVM in local mode
# (the JVM heap is already fixed by the time SparkSession.builder runs), so
# it must be passed to spark-submit itself, here, once, for every job.
exec spark-submit --driver-memory 6g "$@"
