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


public class Round3 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new Round3(), args);

		System.exit(res);
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "Round3");

		job.setJarByClass(Round3.class);
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
			String words = val[0].split("/")[0];
			String id = val[0].split("/")[1];
			String wordCount = val[1].split("/")[0];
			String wordsPerDoc = val[1].split("/")[1];
			
			String op = id + "/" + wordCount + "/" + wordsPerDoc;
			
			context.write(new Text(words), new Text(op));	 
		}
	}
	
	
	public static HashMap<String,Integer> DocsPerWord = new HashMap<String, Integer>();
	
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		
		@Override
	    public void reduce(Text key, Iterable<Text> value, Context context)
	            throws IOException, InterruptedException {
	    	
			int sum = 0;
			for (Text val : value){
				sum += 1;
				
				context.write(key,val);
			}
			String word = key.toString();
			DocsPerWord.put(word,sum);
		}
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		double numberDoc = 2;
		
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException {
			
			for (Text val : value){
				
				String word = key.toString();
				String ID = val.toString().split("/")[0];
				double wordCount = Double.valueOf(val.toString().split("/")[1]);
				double wordsPerDoc = Double.valueOf(val.toString().split("/")[2]);
				double docsPerWord = (double) DocsPerWord.get(word);
				
				double tf_idf = ( wordCount / wordsPerDoc ) * Math.log ( numberDoc / docsPerWord );
				
				String output_key = word + "/" + ID;
				String output_value = Double.toString(tf_idf);
				
				context.write(new Text(output_key), new Text(output_value));
			}
		}
	}
	
}
