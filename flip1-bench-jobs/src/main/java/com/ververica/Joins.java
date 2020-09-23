/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica;

import org.apache.flink.api.common.ExecutionMode;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.util.Collector;

import core.DistributedTPCH;
import io.prestosql.tpch.LineItem;
import io.prestosql.tpch.Part;
import io.prestosql.tpch.PartSupplier;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class Joins {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setExecutionMode(ExecutionMode.BATCH);
		env.setParallelism(4);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 10L));

		DistributedTPCH generator = new DistributedTPCH(env);
		generator.setScale(1);

		DataSet<Part> parts = generator.generateParts();
		parts = parts.map(new MapFunction<Part, Part>() {
			@Override
			public Part map(Part value) throws Exception {
				Thread.sleep(1);
				return value;
			}
		});

		DataSet<PartSupplier> supplierDataSet = generator.generatePartSuppliers();

		DataSet<LineItem> items = generator.generateLineItems();

		JoinOperator.EquiJoin<Part, PartSupplier, String> joined = parts.join(supplierDataSet)
			.where(Part::getPartKey)
			.equalTo(PartSupplier::getPartKey)
			.with((part, supplier, out) -> {
				// System.out.println("got part " + part);
				out.collect(part.toString());
			});
		joined.returns(String.class);

		// find strings containing "a"
		DataSet<Tuple2<String, Integer>> aStrings = joined.flatMap((String str, Collector<Tuple2<String, Integer>> coll) -> {
			if (str.contains("a")) {
				coll.collect(Tuple2.of(str, str.length()));
			}
		}).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

		aStrings = aStrings.map(s -> {
			Thread.sleep(1);
			return s;
		}).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
		SortPartitionOperator<Tuple2<String, Integer>> sorted = aStrings.sortPartition(tup -> "s", Order.ASCENDING);
		DataSet<String> topAStrings = sorted.mapPartition(new MapPartitionFunction<Tuple2<String, Integer>, String>() {
			@Override
			public void mapPartition(Iterable<Tuple2<String, Integer>> values, Collector<String> out) throws Exception {
				List<Tuple2<String, Integer>> top = StreamSupport.stream(values.spliterator(), false).limit(5).collect(Collectors.toList());
				out.collect(top.toString());
			}
		});

		joined.writeAsText("/tmp/joined", FileSystem.WriteMode.OVERWRITE);
		topAStrings.writeAsText("/tmp/top", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
	//
		//	joined.print();
		env.execute("Run join");
	}
}
