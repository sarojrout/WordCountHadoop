/**
 * 
 */
package com.demo.tool;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sarojrout
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static Logger logger = LoggerFactory.getLogger(WordCountMapper.class);
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		logger.info("Entering inside the map method");
		String line = value.toString();
		for(String word:line.split("\\W+")){
			if(word.length()>0){
				context.write(new Text(word), new IntWritable(1));
			}
		}
		logger.info("exiting the method map method");
	}

}
