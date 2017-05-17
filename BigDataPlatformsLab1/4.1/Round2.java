package ecp.Lab1.TFIDF;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.fs.FileSystem;

public class Round2 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new Round2(), args);

		System.exit(res);
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "Round2");

		job.setJarByClass(Round2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combine.class);
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

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

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] val = value.toString().split(";");
			String id = val[0].split("/")[1];
			String words = val[0].split("/")[0];
			String counter = val[1];
			String wordcount = words + "/" + counter;
			
			context.write(new Text(id), new Text(wordcount));	
		}
	}

	
	public static HashMap<String,String> Words_per_Doc = new HashMap<String, String>();
	
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		
		@Override
	    public void reduce(Text key, Iterable<Text> value, Context context)
	            throws IOException, InterruptedException {
	    	
			int sum = 0;
			for (Text val : value){
				String[] wordcount = val.toString().split("/");
				sum += Integer.valueOf(wordcount[1]);
				
				context.write(key,val);
			}
			String ID = key.toString();
			Words_per_Doc.put(ID,String.valueOf(sum));
		}
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException {
			
			for (Text val : value){
				
				String words = val.toString().split("/")[0];
				String counter = val.toString().split("/")[1];
				String id = key.toString();
				String wordsPerDoc = Words_per_Doc.get(id);
				String wordDoc = words + "/" + id;
				String wordCounter = counter + "/" + wordsPerDoc;
				
				context.write(new Text(wordDoc), new Text(wordCounter));
			}
		}
	}
	
}