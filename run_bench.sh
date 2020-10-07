#!/bin/bash

FLINK="/tmp/flink-1.12-SNAPSHOT"
JAR="/Users/robert/Projects/flink-workdir/flip1-bench/flip1-bench-jobs/target/flip1-bench-jobs-1.0-SNAPSHOT.jar"
DATA="/Users/robert/Projects/tmp-flip1-benchmarks/data/scale10"
OUT_DATA="/Users/robert/Projects/tmp-flip1-benchmarks/data"

do_run() {
	job=$1
	args=$2

	CMD="$FLINK/bin/flink run -p 8 -c $job $JAR $ARGS"
	echo "Running command='$CMD'"
	$($CMD > /tmp/_output)
	cat /tmp/_output
	runtime=$(cat /tmp/_output | grep "Job Runtime" | cut -d " " -f 3)

	"$job;$runtime$;$args" >> out.csv
}

for job in "com.ververica.TPCHQuery3" "com.ververica.Parallel"; do
	for executionMode in BATCH PIPELINED; do
		for failure in "" "--reduce-fail" "--reduce-fail 2" "--reduce-fail 3"; do
			for repetition in 1 2; do
				if [[ $job == *"TPC"* ]]; then
					JOB_ARGS="--lineitem $DATA/lineitem.tbl --customer $DATA/customer.tbl --orders $DATA/orders.tbl --output $OUT_DATA/output"
				fi
				if [[ $job == *"Parallel"* ]]; then
					JOB_ARGS="--paths $DATA/lineitem.tbl,$DATA/orders.tbl,$DATA/customer.tbl,$DATA/nation.tbl,$DATA/region.tbl --lineCounter file:///tmp/lcresult --wordCounter file:///tmp/wcresult"
				fi
				ARGS="$JOB_ARGS --executionMode $executionMode --restart 1000 $failure"
				do_run $job $ARGS
			done
		done
	done
done


