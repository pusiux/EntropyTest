package com.patpuc;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class WordCount {

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration ();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.addResource(new Path("/home/pusiux/hadoop-2.6.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/home/pusiux/hadoop-2.6.1/etc/hadoop/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		
		Path inputPath=new Path("hdfs://localhost:9000/user/pusiux/input");
		fs.delete(new Path("hdfs://localhost:9000/user/pusiux/output"), true);
		Path outputPath = new Path("hdfs://localhost:9000/user/pusiux/output");
			
		JobConf job = new JobConf(conf);
		job.setJobName("WordCount");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(EntropyMapper.class);
		job.setCombinerClass(EntropyReduce.class);
		job.setReducerClass(EntropyReduce.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);  
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setJarByClass(WordCount.class);
		JobClient.runJob(job);
	}

	public static class EntropyMapper extends MapReduceBase implements Mapper<LongWritable , Text , Text , IntWritable> {
	    private final IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    @Override
	    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
	    	System.out.println("a");
	        String line = text.toString();
	        StringTokenizer stringTokenizer = new StringTokenizer(line);

	        outputCollector.collect(word , one);
	        while (stringTokenizer.hasMoreTokens()){
	            word.set(stringTokenizer.nextToken());
	            outputCollector.collect(word , one);
	        }
	    }
	}

	public static class EntropyReduce extends MapReduceBase implements Reducer<Text,IntWritable, Text , IntWritable> {

	    @Override
	    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
	    	System.out.println("b");
	        int sum = 0;
	        while (iterator.hasNext()){
	            sum += iterator.next().get();
	        }
	        System.out.println("text: " + text + "sum: " +sum);
	        outputCollector.collect(text , new IntWritable(sum));
	    }
	}
}