package ecp.Lab1.WordCount;

import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCountDriver extends Configured implements Tool {


	 @SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "WordCountDriver");
	      job.setJarByClass(WordCountDriver.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);

	      job.setMapperClass(WordCountMapper.class);
	      job.setReducerClass(WordCountReducer.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));
	      
	      /* to change the separator in the output file */
	      job.getConfiguration().set("mapred.textoutputformat.separator",",");
	      
	      /* to set the number of reducers to 10*/
	      job.setNumReduceTasks(10);
	      
	      /* Delete output file path if already exists */
		  FileSystem fs = FileSystem.newInstance(getConf());
		  
		  if (fs.exists(new Path(args[1]))) {
				fs.delete(new Path(args[1]), true);
			}

	      job.waitForCompletion(true);
	      
	      return 0;
	   }

    public static void main(String[] args) throws Exception {

        WordCountDriver exempleDriver = new WordCountDriver();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

