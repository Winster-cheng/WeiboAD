package com.cpl.WeiboAD;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStream;



import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cpl.WeiboAD.CalcDF.CalcNMapper;
import com.cpl.WeiboAD.CalcDF.CalcNReducer;

public class CalcALL {
	static class CalcAllMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			
			String uriDF = "/user/WeiboAD/DF/part-r-00000";
			String uriN="/user/WeiboAD/TFAndN/part-r-00001";
			float df=Float.parseFloat(getMessageFromHDFS((uriDF)));
			float n=Float.parseFloat(getMessageFromHDFS((uriN)));
			float logN_DF=(float) (Math.log(12)/Math.log(12));
			
			String s[] = value.toString().split("\\s+");
			float r=logN_DF+Float.parseFloat(s[1]);//最终结果
			context.write(new Text(s[0]), new Text(String.valueOf(r)));
			

		}
	}

	public static String getMessageFromHDFS(String uri) throws IllegalArgumentException, IOException{
		BufferedReader br =null;
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem. get(URI.create (uri), conf);
	    InputStream in = null;
	    try {
	         in = fs.open( new Path(uri));
	         br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
	         return br.readLine().split("\\s+")[1];
	        } finally {
	         IOUtils.closeStream(in);

	        }
	}
	
	public static Job getJobCalcAll() throws IOException {
		Configuration conf = new Configuration();
		String input = "/user/WeiboAD/TFAndN/part-r-00000";
		String output = "/user/WeiboAD/result";
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}
		Job job = Job.getInstance(conf);
		job.setJarByClass(CalcDF.class);
		job.setJobName("Calculate  Result");

		job.setMapperClass(CalcAllMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job;
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.exit(getJobCalcAll().waitForCompletion(true)?0:1);
	}
}