package edu.cmu;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndex {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text filename = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			FileSplit fs = (FileSplit) context.getInputSplit();
			String location = fs.getPath().getName().toString();
			String line = value.toString().trim()
					.replaceAll("[^A-Za-z0-9 ]", " ").toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String str = tokenizer.nextToken();
				word.set(str);
				filename.set(location);
				context.write(word, filename);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashSet<String> filenameValidate = new HashSet<String>();
			StringBuilder str = new StringBuilder();
			for (Text value : values) {
				if (!filenameValidate.contains(value.toString())) {
					filenameValidate.add(value.toString());
					str.append(value.toString() + " ");
				}
			}
			result.set(str.toString());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "invertedindex");
		job.setJarByClass(InvertedIndex.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
