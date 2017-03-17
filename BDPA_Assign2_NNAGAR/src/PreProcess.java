package Assignment2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class PreProcess {
	public static void main(String[] args) throws Exception {
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "pre process");
		
	    job.setJarByClass(PreProcess.class); 
	    
	    job.setMapperClass(PPMapper.class); 
	    job.setNumReduceTasks(1); 
		job.setReducerClass(PPReducer.class); 
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//When the job gets completed we use a counter to get the number of output lines
		job.waitForCompletion(true);
		try {
			Long oplines = job.getCounters().findCounter("org.apache.hadoop.mapred.Task.Counter", "MAP_OUTPUT_RECORDS").getValue();
			BufferedWriter writer = new BufferedWriter(new FileWriter("home/cloudera/workspace/SetSimilarity/oplines.txt"));
			writer.write(String.valueOf(oplines));
			writer.close();
		} catch(Exception e) {
			throw new Exception(e);
		}
		
		  System.exit(0);
	}
 
	public static class PPMapper extends Mapper<Text, Text, Text, Text> {
		      private Text token = new Text();
		 	 
		     //We use the stop words output, created from the previous assignment, 
		     //as an input list of words that are to be removed from the given input corpus
		     File stopWordsFile = new File("/home/cloudera/workspace/SetSimilarity/StopWordsList.txt");
		     Set<String> stopWordsList = new HashSet<String>();
		     
		    //We use the buffered reader in this method to read the stop words file and store it as a list
		  	 protected void setup(Context context) throws IOException, InterruptedException {
		    	 
		  	  	 BufferedReader reader = new BufferedReader(new FileReader(stopWordsFile));
		     	 String sw = null;
		     	 while ((sw = reader.readLine()) != null){ 
		     		 stopWordsList.add(sw);
		     	 }
		     	 reader.close();
			 }
		      @Override
		      public void map(Text key, Text value, Context context)
		              throws IOException, InterruptedException {
		    	 
		 		List <String> count = new ArrayList <>();
		 		//We remove blank lines
		 		if (value.toString().length() != 0){ 
		 			//We keep only A-Z a-z 0-9
		            for (String word: value.toString().replaceAll("[^A-Za-z0-9]"," ").split("\\s+")){ 
		            	// We check if the word is contained in the stop words list
		            	if (!stopWordsList.contains(word.toLowerCase())){ 
		            		// we check for redundancies
		            		if (!count.contains(word.toLowerCase())){ 
		            			count.add(word.replaceAll("[^A-Za-z0-9]","").toString().toLowerCase());
		            			token.set(word.toString());
		            			if(token.toString().length() != 0){
		            				context.write(key, token);
		            			}
		            		}
		            	}
		            }
		 		}
		      }
		   }

	

	public static class PPReducer extends Reducer<Text, Text, LongWritable, Text> {
		
		// We create a Hash Map to store the number of occurrences of each word
		private HashMap<String, Integer> wordUnique = new HashMap<String, Integer>(); 
		
		// We create and initialize a counter to denote the number of lines outputted
		private long count = 0; 
		
	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// We create and initialise a counter to check the word frequency
			int wordFreq = 0;
			//We create a list that will store the words for the doc being read
			LinkedList<String> wordDoc = new LinkedList<String>(); 
			for(Text v : values) {
				//They key below is associated to each word and we use it to check the word frequency
				if(key.charAt(0) != '{') {
					wordFreq += Integer.parseInt(v.toString());
				} 
				else {
					// This gives the key of every doc 
					String[] wordsVal = v.toString().split("\\s");
					for(int i = 0; i < wordsVal.length; i++) {
						int j = 0;
						String wordFinal = wordsVal[i];
						//we check if the word is unique and remove redundancies
						if(!wordDoc.contains(wordFinal)) {
							int freqNow = wordUnique.get(wordFinal);
							while(j < wordDoc.size() && freqNow > wordUnique.get(wordDoc.get(j))) { //find its position in the list
								j++;
							}
							// We use the loop to successively add the words to the wordDoc
							wordDoc.add(j, wordFinal); 
						}
					}
				}
			}
			if(key.charAt(0) != '{') { 
				wordUnique.put(key.toString(), wordFreq);
			}
			else {
				String str = key.toString();
				int firstIndex = str.lastIndexOf("#") + 1;
				long id = Long.parseLong(str.substring(firstIndex));
				String output = "";
				for(String string : wordDoc) {
					output = output + string + " ";
				}
				// We start the counter 
				if(count == 0) { 
					count = id + 1;
				}
				// we create the final output
				context.write(new LongWritable(count), new Text(output)); 
				count++; 
			}
		}
	}
}
