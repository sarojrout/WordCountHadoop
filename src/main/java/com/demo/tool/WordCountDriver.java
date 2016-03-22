package com.demo.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountDriver extends Configured implements Tool{
	private static Logger logger = LoggerFactory.getLogger(WordCountDriver.class);

	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new WordCountDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			logger.debug("Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}
		/*
		 * Instantiate a Job object for your job's configuration.
		 */
		Job job = Job.getInstance(getConf());
		job.setJarByClass(WordCountDriver.class);
		job.setJobName("Word Count Job");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes successfully, return 0. If not, return 1.
		 */
		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);
	}

}
