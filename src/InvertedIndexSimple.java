package assignment1.mapreduce.ii;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

;

public class InvertedIndexSimple extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new InvertedIndexSimple(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "InvertedIndexSimple");

		job.setJarByClass(InvertedIndexSimple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", " -> ");
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text filename = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			HashSet<String> stopwords = new HashSet<String>();
			BufferedReader Reader = new BufferedReader(
					new FileReader(
							new File(
									"/home/cloudera/workspace/InvertedIndex/sw.txt")));
			// for reading the stopwords file created previously
			
			String pattern;
			while ((pattern = Reader.readLine()) != null) {
				stopwords.add(pattern.toLowerCase());
			}

			String filenameStr = ((FileSplit) context.getInputSplit())
					.getPath().getName();
			filename = new Text(filenameStr);

			for (String token : value.toString().split("\\s+")) {
				if (!stopwords.contains(token.toLowerCase())) {
					word.set(token.toLowerCase()); //for case sensitivity
				}
			}

			context.write(word, filename);
			
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashSet<String> set = new HashSet<String>();
			// to add document names 
			
			for (Text value : values) {
				set.add(value.toString());
			}

			StringBuilder builder = new StringBuilder();

			String prefix = "";
			for (String value : set) {
				builder.append(prefix);
				prefix = ", ";
				builder.append(value);
			}

			context.write(key, new Text(builder.toString()));

		}
	}
}