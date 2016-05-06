package com.sunny.hadooptest;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * 1.this class let we to execute like below
 * 		hadoop com.sunny.hadooptest.Process2 -conf /BBBB/hadoop2.7.2/etc/hadoop/conf/hadoop-local.xml
 * 2.we can assign xml configuration file
 * 3.we must extends Configured implements Tool and override run function
 * @author root
 *
 */
public class Process2 extends Configured implements Tool{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String year = line.substring(15, 19);
			int airTemperature ;
			if(line.charAt(87)=='+')
				airTemperature = Integer.parseInt(line.substring(88, 92));
			else
				airTemperature = Integer.parseInt(line.substring(87,92));
			String quality = line.substring(92, 93);
			if(quality.matches("[01459]")){
				context.write(new Text(year), new IntWritable(airTemperature));
			}
		}
	}
	public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for(IntWritable value:values){
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(key, new IntWritable(maxValue));
		}
		
	}
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Process2(), args);
	}
	
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(com.sunny.hadooptest.Process2.class);
		job.setJobName("Max temperature");
		//((JobConf)job.getConfiguration()).setJar("a.jxr");
		FileInputFormat.setInputPaths(job, new Path("/1901"),new Path("/1902"));
		FileOutputFormat.setOutputPath(job, new Path("output"));	
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		return 0;
	}

	/*unit test*/
	@Test
	public void testMapper(){
		Text value = new Text("0067011990999991950051507004+68750+023550FM-12+038299999V0203301N00671220001CN9999999N9+00001+99999999999");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MyMapper())
		.withInputValue(value)
		.withOutput(new Text("1950"), new IntWritable(0))
		.runTest();
	}
	//@Test
//	public void testReducer(){
//		new ReduceDriver<Text,IntWritable,Text,IntWritable>()
//		//.withReducer(new MyReduce())
//		.withInputKey(new Text("1950"))
//		.withInputValues(Arrays.asList(new IntWritable(10),new IntWritable(5)))
//		.withOutput(new Text("1950"), new IntWritable(10))
//		.runTest();
//
//	}
}
