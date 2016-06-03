package com.sunny.hadooptest;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mrunit.ReduceDriver;
//import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class Process{
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
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		if(args.length!=2){
//			System.out.println("ERROR");
//			System.exit(-1);
//		}
		Job job = new Job();
		job.setJarByClass(Process.class);
		job.setJobName("Max temperature");
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		
		FileInputFormat.addInputPath(job, new Path("hdfs:///sample.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs:///output"));	
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
	}
	/*unit test*/
	/*@Test
	public void testMapper(){
		Text value = new Text("0067011990999991950051507004+68750+023550FM-12+038299999V0203301N00671220001CN9999999N9+00001+99999999999");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MyMapper())
		.withInputValue(value)
		.withOutput(new Text("1950"), new IntWritable(0))
		.runTest();
	}
	//@Test
	public void testReducer(){
		new ReduceDriver<Text,IntWritable,Text,IntWritable>()
		//.withReducer(new MyReduce())
		.withInputKey(new Text("1950"))
		.withInputValues(Arrays.asList(new IntWritable(10),new IntWritable(5)))
		.withOutput(new Text("1950"), new IntWritable(10))
		.runTest();

	}*/
}
