package com.sunny.hadooptest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mrunit.ReduceDriver;
//import org.apache.hadoop.mrunit.mapreduce.MapDriver;

/**
 * time from 20150301 to 20150830
 * total 183 days
 * @author root
 *
 */
public class Process6{
	public static final long startTimeStamp = 1425139200;  //2015-03-01 0:0:0
	public static final long endTimeStamp =   1440950400;  //2015-08-30 24:0:0
	public static final long totalDays = 183;
	static String hdfsUri = "hdfs://10.124.22.213:9000";
	/*
	 * map_every_song_playNumber   <Key,Value> = <songName,{1,2,3,4,......}> 
	 * map_every_singer_totalPlayNumbers <Key,Value> = <singerName,{10,20,30,40,......}>
	 */
	static Map<String, List<String>> map_every_song_playNumber = new HashMap<String, List<String>>();
	
	
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] linearr = line.split(",");
			
			context.write(new Text(linearr[1]), new Text(linearr[0]));
			
		}
	}
	public static class MyReduce extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
		    Map<String, List<String>> map_every_singer_totalPlayNumbers = new HashMap<String, List<String>>();
			System.out.println("static song number:"+map_every_song_playNumber.size());
			int i = 0;
			int total_playNumber = 0;//to check result whether or not right
			int lost_song = 0;
			int handle_col = 0;
			
			for(Text text:values){
				
				if(map_every_song_playNumber.containsKey(text.toString())){
					if(map_every_singer_totalPlayNumbers.containsKey(key.toString())){
						List<String> list_song = map_every_song_playNumber.get(text.toString());// This song play number
						List<String> list_singer = map_every_singer_totalPlayNumbers.get(key.toString());
						List<String> list_rs = new ArrayList<String>();
						//int r1 = test(list_song);
						//int r2 = test(list_singer);
						//System.out.println("list song:"+list_song.size());
						for(int j=0;j<list_song.size();j++){
							int tmp = Integer.valueOf(list_singer.get(j))+Integer.valueOf(list_song.get(j));
							
							list_rs.add(String.valueOf(tmp));
						}
						//total_playNumber+=tmp;
						map_every_singer_totalPlayNumbers.put(key.toString(), list_rs);
						//System.out.println("list rs:"+list_rs.size());
						list_rs = null;
						
					}else{
						map_every_singer_totalPlayNumbers.put(key.toString(), map_every_song_playNumber.get(text.toString()));
					}
				}else{
					lost_song++;
				}
				i++;
				if(i%50==0)
					System.out.println();
				System.out.print("*");
			}
			
			System.out.println();
			System.out.println("singer_table read finished");
			
			System.out.println("start print result");
			
			System.out.println("singer number: "+map_every_singer_totalPlayNumbers.size());
			
			System.out.println("total handle singer numbers: "+handle_col);
			
			System.out.println("total loss song numbers: "+lost_song);
			
			Iterator<String> it = map_every_singer_totalPlayNumbers.keySet().iterator();
			int totalsum = 0;
			String singer_datas = null;
			while(it.hasNext()){
				String k = it.next();
				System.out.println("key:"+k);
				List<String> list_rs = map_every_singer_totalPlayNumbers.get(k);
				
				//totalsum+=test(list_rs);
				if(list_rs==null){
					System.out.println("ERROR");
					continue;
				}
				singer_datas = StringUtils.join(list_rs, ',');
				list_rs = null;
			}
			if(singer_datas!=null)
				context.write(key, new Text(","+singer_datas));
		}
		
	}
	public static void f1(){
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI(hdfsUri),new Configuration());
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
			FSDataInputStream input = null;
			BufferedReader br = null;
			try {
				
				input = fs.open(new Path("hdfs:///mars_tianchi_everysongs_statics.csv"));
				 br = new BufferedReader(new InputStreamReader(input));
				
				int i = 0;
				String line = null;
				while((line=br.readLine())!=null){	
					String k = line.substring(0,line.indexOf(",")).trim();
					String vc =line.substring(line.indexOf(",")+1);
					String c = vc.substring(vc.indexOf(",")+1);
					List<String> list = Arrays.asList(c.split(","));
					map_every_song_playNumber.put(k, list);
					i++;
					if(i%50==0)
						System.out.println();
					System.out.print(".");
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	
			System.out.println();
			System.out.println("song_table read finished,total "+map_every_song_playNumber.size()+"entries");
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		if(args.length!=2){
//			System.out.println("ERROR");
//			System.exit(-1);
//		}
		
		f1();// fill  map_every_song_playNumber
		
		Job job = new Job();
		job.setJarByClass(Process6.class);
		job.setJobName("songs_statistics");
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		
		FileInputFormat.addInputPath(job, new Path("hdfs:///taobaoset/p2_mars_tianchi_songs.csv"));
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
