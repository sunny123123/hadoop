package com.sunny.hdfs;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
/*
 * 1.use hdfs system
 * 2.use local system
 * */
public class FileSystemCat {
	public static void main(String[] args) throws Exception {
		new FileSystemCat().WriteDataToHDFSByLocalFileSystem();
	
	}
	public void useHDFSsystem() throws IOException, URISyntaxException{
		String uri = "c";
		Configuration conf = new Configuration();//haoop will use core-site.xml configuration
		//FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);//use hdfs ststem
		InputStream in = null;
		in = fs.open(new Path(uri));
		IOUtils.copyBytes(in, System.out, 4096,false);
	}
	public void useLocalFileSyetem() throws IOException{
		InputStream in = null;
		FileSystem localfs = FileSystem.getLocal(new Configuration());//use local system
		in = localfs.open(new Path("file:///BBBB/panzha.txt"));
		IOUtils.copyBytes(in, System.out, 4096,false);
	}
	public void WriteDataToHDFSByLocalFileSystem() throws IOException{
		String hdfsUri = "hdfs://localhost:9000/panzha100.txt";
		String localUri = "/BBBB/panzha.txt";
		FileSystem fs = FileSystem.get(URI.create(hdfsUri),new Configuration());
		InputStream in = new FileInputStream(localUri);
		OutputStream out = fs.create(new Path(hdfsUri), new Progressable() {
			
			public void progress() {
				System.out.println(".");
			}
		});
		IOUtils.copyBytes(in, out, 1024,true);
	}
}
