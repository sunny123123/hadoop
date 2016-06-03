package com.sunny.hadooptest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
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

import com.sunny.util.CommonUtil;

public class Process3{
	public static final long startTimeStamp = 1425139200;
	public static final long endTimeStamp =   1440950400;
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] linearr = line.split(",");
			
			context.write(new Text(linearr[1]), new Text(linearr[2]));
			
		}
	}
	public static class MyReduce extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			//Iterator<Text> it = values.iterator();
			//Text first = it.next();
			String max = "0";
			String min = "z";
			StringBuffer sb = new StringBuffer();
			for(Text value:values){
				if((value.toString().compareTo(max))>0)
					max = value.toString();
				if((value.toString().compareTo(min))<0)
					min = value.toString();
				sb.append(value).append(",");
			}
			String[] datas = sb.toString().split(",");
			Arrays.sort(datas);
			CommonUtil.convertToStringTime(datas);
			String data = StringUtils.join(datas, ",");
			//context.write(key, new Text(min+","+max+","+datas.length+","+data));
			context.write(key, new Text(datas.length+","+data));
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		if(args.length!=2){
//			System.out.println("ERROR");
//			System.exit(-1);
//		}
		Job job = new Job();
		job.setJarByClass(Process3.class);
		job.setJobName("Max date");
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		
		FileInputFormat.addInputPath(job, new Path("hdfs:///mars_tianchi_user_actions_taobao.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs:///output"));	
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
