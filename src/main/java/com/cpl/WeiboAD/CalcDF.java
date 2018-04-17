package com.cpl.WeiboAD;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalcDF {

	static class CalcNMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		Text k = new Text("N");
		IntWritable v = new IntWritable(1);

		@Override
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(k, v);
		}
	}

	static class CalcNReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key,Iterable<IntWritable> values,org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context context)throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable i : values) {
				count++;
			}
			context.write(new Text("DF"), new IntWritable(count));
		}
	}

	public static Job getJobCalcN() throws IOException {
		Configuration conf = new Configuration();
		String input = "/user/WeiboAD/TFAndN/part-r-00000";
		String output = "/user/WeiboAD/DF";
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}
		Job job = Job.getInstance(conf);
		job.setJarByClass(CalcDF.class);
		job.setJobName("Calculate  N");

		job.setMapperClass(CalcNMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(CalcNReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job;
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException, Exception {
		System.exit(getJobCalcN().waitForCompletion(true) ? 0 : 1);
	}

}
