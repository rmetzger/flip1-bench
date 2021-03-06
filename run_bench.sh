#!/bin/bash

FLINK="/Users/robert/Projects/tmp-flip1-benchmarks/flink-1.12-SNAPSHOT"
JAR="/Users/robert/Projects/tmp-flip1-benchmarks/flip1-bench/flip1-bench-jobs/target/flip1-bench-jobs-1.0-SNAPSHOT.jar"
DATA="/Users/robert/Projects/tmp-flip1-benchmarks/data/scale50"
OUT_DATA="/Users/robert/Projects/tmp-flip1-benchmarks/data"

do_run() {
	job=$1
	args=$2

	CMD="$FLINK/bin/flink run -p 8 -c $job $JAR $args"
	echo "Running command='$CMD'"
	$($CMD > /tmp/_output)
	cat /tmp/_output
	runtime=$(cat /tmp/_output | grep "Job Runtime" | cut -d " " -f 3)
	NEW_UUID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
	cp /tmp/_output $NEW_UUID.out

	echo "$job;$runtime;$args" >> out2.csv
}

for job in "com.ververica.TPCHQuery3"; do
	for executionMode in BATCH PIPELINED; do
		# never, 20m, 15m, 10m, 8m, 6m, 4m, 2m, 1m
		# now 9, 7, 5m
		for failure in "--meanKillFrequency 50" "--meanKillFrequency 40" "--meanKillFrequency 30" "--meanKillFrequency 20" "--meanKillFrequency 10" "--meanKillFrequency 5" "--meanKillFrequncy 1"; do
		#for failure in "--meanKillFrequency 900" "--meanKillFrequency 600" "--meanKillFrequency 540" "--meanKillFrequency 480" "--meanKillFrequency 420" "--meanKillFrequency 360" "--meanKillFrequency 300" "--meanKillFrequency 240" "--meanKillFrequency 180" "--meanKillFrequency 120" "--meanKillFrequency 60" ; do
		#for failure in "--meanKillFrequency -1"; do
			for repetition in 1 2 3; do
				if [[ $job == *"TPC"* ]]; then
					JOB_ARGS="--lineitem $DATA/lineitem.tbl --customer $DATA/customer.tbl --orders $DATA/orders.tbl --output $OUT_DATA/output"
				fi
				if [[ $job == *"Parallel"* ]]; then
					JOB_ARGS="--paths $DATA/lineitem.tbl,$DATA/orders.tbl,$DATA/customer.tbl,$DATA/nation.tbl,$DATA/region.tbl --lineCounter file:///tmp/lcresult --wordCounter file:///tmp/wcresult"
				fi
				ARGS="$JOB_ARGS --executionMode $executionMode --restart 10000 $failure"
				do_run $job "$ARGS"
			done
			echo "Repetition done. Recycling Flink cluster"
			$FLINK/bin/stop-cluster.sh
			$FLINK/bin/start-cluster.sh
			sleep 10
			echo "Cluster restarted. Continuing ..."
		done
	done
done


