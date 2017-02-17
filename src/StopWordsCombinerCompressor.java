package assignment1.mapreduce.ii;

import java.io.IOException;
import java.util.Arrays;

/*import javax.sound.sampled.Line;*/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.io.compress.SnappyCodec; 

@SuppressWarnings("unused")
public class StopWordsCombinerCompressor extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new StopWordsCombinerCompressor(), args);
      
      System.exit(res);
   }

   @SuppressWarnings("deprecation")
@Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "StopWords");
      job.setJarByClass(StopWordsCombinerCompressor.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      /* adding combiner */
      job.setCombinerClass(Reduce.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      /* to change the separator in the output file */
      Configuration con = new Configuration(); 
      job.getConfiguration().set("mapred.textoutputformat.separator",",");
      
      /* to compress the output of map */
      con.setBoolean("mapreduce.map.output.compress",true);
      con.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
      
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
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
       private final static IntWritable ONE = new IntWritable(1);
       private Text word = new Text();

       @Override
       public void map(LongWritable key, Text value, Context context)
               throws IOException, InterruptedException {
           for (String token : value.toString().split("\\s+")) {
               word.set(token.toLowerCase());
               context.write(word, ONE);
           }
       }
   }
   
   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         /*stop words have frequency more than 4000*/
         if (sum > 4000){
         context.write(key, new IntWritable(sum));
         }
      }
      
   }
}
