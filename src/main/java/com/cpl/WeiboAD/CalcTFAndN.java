package com.cpl.WeiboAD;

import java.io.IOException;

/**
 * Created by: wintercheng
 * time:       16/11/23 15:36
 * <p/>
 * 从原始微博数据中计算TF和N的值
 */


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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class CalcTFAndN {

	
	// 输入数据 WeiboID KEY1,KEY2,KEY3,KEY4
	public static class CalcTFAndNMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private IntWritable valueofN = new IntWritable(1);
		private IntWritable valueofTF = new IntWritable();
		private Text k1 = new Text();
		private Text k2 = new Text("count");

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String content[] = value.toString().split("\\s+");
			System.out.println("content[1] is"+content[1] );
			String message[] = content[1].split(",");
			int count = 0; // count为这条微博关键词的数量
			for (String word : message) {
				if (word.equals("打车") || word.equals("出租车")||word.equals("滴滴") ) {
					count++;
				}
				if(count!=0){       //注意去除没提到过的微博不要写到输出结果里面，不然DF会算错
				k1.set(content[0]); //weiboID
				valueofTF.set(count);
				context.write(k1, valueofTF); // 输出TF,key为微博ID，value为该条微博的关键词的数量
				}
				context.write(k2, valueofN); // 输出N，key为count
			}
		}
	}

	public static class CalcTFAndNReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable v = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable a : values) {
				count += a.get();
			}
			v.set(count);
			context.write(key, v);
		}
	}

	static class CountPartitioner extends HashPartitioner<Text, IntWritable> {

		/**
		 * 采用4个reducer,count交给最后一个
		 */
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			if (key.equals(new Text("count"))) {
				return 1;
			} else {
				// 其余使用默认的分区方式,此时传递的分区数应该-1
				return 0;
			}
		}
	}

	public static Job getJobTFAndN() throws Exception {

		Configuration conf = new Configuration();
		String input = "/user/WeiboAD/test_after";
		String output = "/user/WeiboAD/TFAndN/";
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}
		Job job = Job.getInstance(conf);
		job.setJarByClass(CalcTFAndN.class);
		job.setJobName("Calculate TF and N");

		job.setMapperClass(CalcTFAndNMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(CalcTFAndNReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(CountPartitioner.class);
		job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job;
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException, Exception {
		System.exit(getJobTFAndN().waitForCompletion(true) ? 0 : 1);
	}
}
