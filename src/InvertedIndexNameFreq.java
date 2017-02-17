package assignment1.mapreduce.ii;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InvertedIndexNameFreq extends Configured implements Tool {
    
	public static void main(String[] args)
			throws Exception {
        int res = ToolRunner.run(new Configuration(),new InvertedIndexNameFreq(), args);
        System.exit(res);
    }

    @SuppressWarnings("deprecation")
	@Override
    public int run(String[] args) 
    		throws Exception {
        System.out.println(Arrays.toString(args));
        Job job = new Job(getConf(), "InvertedIndexNameFreq");

        job.setJarByClass(InvertedIndexNameFreq.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " - ");
        job.setNumReduceTasks(1);
       
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem txtfile = FileSystem.newInstance(getConf());

        if (txtfile.exists(new Path(args[1]))) {
            txtfile.delete(new Path(args[1]), true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
       
    	private Text word = new Text();
        private Text docName = new Text();

        @SuppressWarnings("resource")
		@Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            HashSet<String> stopWordsSet = new HashSet<String>();
            File stopWordsFile = new File("/home/cloudera/workspace/InvertedIndex/sw.txt");
            BufferedReader buffReader = new BufferedReader(new FileReader(stopWordsFile));
            String line;
            
            while ((line = buffReader.readLine()) != null) {
                stopWordsSet.add(line.toLowerCase());
            }

            String docNameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
            docName = new Text(docNameStr);

            for (String token : value.toString().split("\\s+")) {
                if (!stopWordsSet.contains(token.toLowerCase())) {
                    word.set(token.toLowerCase());
                    context.write(word, docName);
                }
            }
           
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
           
            ArrayList<String> docName = new ArrayList<String>();

            for (Text value : values) {
                docName.add(value.toString());
            }

            HashSet<String> set = new HashSet<String>(docName);
            StringBuilder strBuilder = new StringBuilder();
            String prefix = "";
            
            for (String docNumber : set) {
                strBuilder.append(prefix);
                prefix = ", ";
                strBuilder.append(docNumber+"#"+Collections.frequency(docName, docNumber));
               
            }

            context.write(key, new Text(strBuilder.toString()));

        }
    }
}