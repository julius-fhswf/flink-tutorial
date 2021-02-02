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

package org.julius;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Date;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

  /**
   * To make Redis connector work add the following dependency to your pom.xml
   *
   * <p>
   *     <dependency>
   *         <groupId>org.apache.flink</groupId>
   *     	 <artifactId>flink-connector-redis_2.10</artifactId>
   *     	 <version>1.1.5</version>
   *     </dependency>
   */
  public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1000);
		//set up Redis connector - change to IP used by your redis container (needs to be in flink-network!)
		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("172.22.0.4").build();
		//folder to search for new csv Files - change it to the folder you mapped into your container
		String inputPath = "/opt/flink/julius-dir/input/";
		TextInputFormat format = new TextInputFormat(
				new Path(inputPath));
		//open a new DataStream and instruct Flink to look for new and changed files continously every 100ms
		DataStream<String> inputStream = env
				.readFile(format, inputPath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100);
		//map inputStream to a typed stream of persons
		DataStream<Person> personStream = inputStream
				.map((line) -> {
					String[] cells = line.split(";");
					Person p = new Person(cells[0], Integer.parseInt(cells[1]));
					return p;
				});
		//filer the personStream and create a new stream with persons with age equal or higher 60
		DataStream<Person> seniors = personStream
				.filter(new FilterFunction<Person>() {
					@Override
					public boolean filter(Person person) throws Exception {
						return person.age >= 60;
					}
				});
		//write out seniors to Redis using a simple mapper
		seniors.addSink(new RedisSink<Person> (conf, new RedisTestMapper() ));

		//@todo: change the combined age to a rolling average
		//@todo: use state - i.e. make sure you are not processing a person twice (key name...)
		// create a 10s combined age
		SingleOutputStreamOperator<Tuple2<String, Integer>> combinedAge = inputStream
				.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
						String[] cells = s.split(";");
						Tuple2 t = new Tuple2<String, Integer>("seniors", Integer.parseInt(cells[1]));
						collector.collect(t);
					}
				})
				.keyBy(t -> t.f0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.sum(1);

		combinedAge.addSink(new RedisSink<>(conf, new RedisTuple2Mapper()) );

		env.execute("A stream of seniors");

	}
	public static class Person {
		public String name;
		public Integer age;
		public Date birthday;
		public Person() {};

		public Person(String name, Integer age) {
			this.name = name;
			this.age = age;
			this.birthday = new Date();
		};

		public String toString() {
			return this.name.toString() + ": age " + this.age.toString();
		};
	}

	public static class RedisTestMapper implements RedisMapper<Person>{
		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(RedisCommand.SET);
		}

		@Override
		public String getKeyFromData(Person data) {
			return data.name;
		}

		@Override
		public String getValueFromData(Person data) {
			return data.toString();
		}
	}

	public static class RedisTuple2Mapper implements RedisMapper<Tuple2<String, Integer>>{
		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(RedisCommand.SET);
		}

		@Override
		public String getKeyFromData(Tuple2<String, Integer> data) {
			return data.f0;
		}

		@Override
		public String getValueFromData(Tuple2<String, Integer> data) {
			return data.f1.toString();
		}
	}



}
