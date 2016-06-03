package com.sunny.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonUtil {

	public static String getStringByUnixtimestamp(long time){
		SimpleDateFormat format =  new SimpleDateFormat("MMdd:HH");
		Long t = time*1000;
		String d = format.format(new Date(t));
		return d;
	}
	
	public static void convertToStringTime(String[] time){
		for(int i=0;i<time.length;i++){
			time[i] = getStringByUnixtimestamp(Long.valueOf(time[i]));
		}
	}
}
