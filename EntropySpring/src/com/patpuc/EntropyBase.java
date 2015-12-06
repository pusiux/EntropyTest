package com.patpuc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

public class EntropyBase {

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.addResource(new Path("/home/pusiux/hadoop-2.6.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/home/pusiux/hadoop-2.6.1/etc/hadoop/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		Path inputPath = new Path("hdfs://localhost:9000/user/pusiux/input");
		fs.delete(new Path("hdfs://localhost:9000/user/pusiux/output"), true);
		Path outputPath = new Path("hdfs://localhost:9000/user/pusiux/output");

		JobConf job = new JobConf(conf);
		job.setJobName("Entropy");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(EntropyMapper.class);
		job.setReducerClass(EntropyReduce.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setJarByClass(EntropyBase.class);
		JobClient.runJob(job);
	}

	public static class EntropyMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable longWritable, Text text, OutputCollector<Text, DoubleWritable> outputCollector,
				Reporter reporter) throws IOException {
			String line = text.toString();
			StringTokenizer stringTokenizer = new StringTokenizer(line);
			outputCollector.collect(word, one);
			while (stringTokenizer.hasMoreTokens()) {
				word.set(stringTokenizer.nextToken());
				if (word.getLength() > 0)
					outputCollector.collect(word, one);
			}
		}
	}

	public static class EntropyReduce extends MapReduceBase
			implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text text, Iterator<DoubleWritable> iterator,
				OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
			double res = 0.0;
			res = prepareStringToCalcEntropy(text);
			outputCollector.collect(text, new DoubleWritable(res));

		}
	}

	/*
	 * 
	 * check and fix hint : don't add to list, cut string.
	 */
	public static Double prepareStringToCalcEntropy(Text text) {

		List<String> list = new LinkedList<>();
		double res = 0.0;
		if (text.getLength() > 0)
			list.add(text.toString());
		List<String> passTmp = new ArrayList<String>();
		for (int j = 0; j < list.size(); j++) {

			for (int i = 0; i < list.get(j).length(); i++) {
				passTmp.add(String.valueOf(list.get(j).charAt(i)));
			}
			res = calculateEntropy(passTmp);

		}
		return res;
	}

	public static Double calculateEntropy(List<String> values) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		// count the occurrences of each value
		for (String sequence : values) {
			if (!map.containsKey(sequence)) {
				map.put(sequence, 0);
			}
			map.put(sequence, map.get(sequence) + 1);
		}

		// calculate the entropy
		Double result = 0.0;
		for (String sequence : map.keySet()) {
			Double frequency = (double) map.get(sequence) / values.size();
			result -= frequency * (Math.log(frequency) / Math.log(2));
		}
		return result;
	}
}
