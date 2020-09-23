package com.ververica;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.StreamSupport;

import scala.annotation.meta.param;

/**
 * Testing input args:
 *
 * --paths file:///tmp/batchout,file:///tmp/top --lineCounter file:///tmp/lcresult --wordCounter file:///tmp/wcresult
 */
public class Parallel {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        if(params.has("executionMode")) {
            env.getConfig().setExecutionMode(ExecutionMode.valueOf(params.get("executionMode")));
        }

        if(params.has("restart")) {
            env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(params.getInt("restart", Integer.MAX_VALUE), 10L));
        }

        String[] paths = params.get("paths").split(",");
        Arrays.asList(paths).forEach(path -> {
            System.out.println("Constructing batch job for " + path);

            // read all the lines
            DataSet<String> lines = env.readTextFile(path);

            // maybe artificially slow processing down
            if (params.has("delayer")) {

                lines = lines.map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        Thread.sleep(params.getLong("delayer", 1));
                        return value;
                    }
                }).setParallelism(params.getInt("delayer-par", env.getParallelism()));
            }

            // maybe let the data set size explode
            if (params.has("exploder")) {
                lines = lines.flatMap(new FlatMapFunction<String, String>() {
                    final int factor = params.getInt("exploder", 2);
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (int i = 0; i < factor; i++) {
                            out.collect(value);
                        }
                    }
                }).setParallelism(params.getInt("exploder-para", env.getParallelism()));
            }

            if (params.has("lineCounter")) {
                DataSet<Long> count = lines.reduceGroup(new GroupReduceFunction<String, Long>() {
                    @Override
                    public void reduce(Iterable<String> values, Collector<Long> out) throws Exception {
                        out.collect(StreamSupport.stream(values.spliterator(), false).count());
                    }
                });

                DataSet<Tuple2<String, Long>> countWithFilename = count.map(new MapFunction<Long, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Long value) throws Exception {
                        return Tuple2.of(path, value);
                    }
                });

                countWithFilename.writeAsText(params.get("lineCounter") + "--" + UUID.randomUUID(), FileSystem.WriteMode.OVERWRITE);
            }

            if (params.has("wordCounter")) {
                DataSet<Tuple2<String, Integer>> counts =
                    // split up the lines in pairs (2-tuples) containing: (word,1)
                    lines.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);
                counts.writeAsCsv(params.get("wordCounter") + "--" + UUID.randomUUID(), "\n", " ", FileSystem.WriteMode.OVERWRITE);
            }
        });

        env.execute("Massively parallel = " + paths.length);
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
