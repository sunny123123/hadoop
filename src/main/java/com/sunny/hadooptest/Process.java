package com.sunny.hadooptest;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Process {
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
		FileInputFormat.addInputPath(job, new Path("sample.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output"));	
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
	}
}
