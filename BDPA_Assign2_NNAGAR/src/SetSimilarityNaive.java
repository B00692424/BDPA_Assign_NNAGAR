package Assignment2;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SetSimilarityNaive {

   public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "set similarity naive");

    job.setJarByClass(SetSimilarityNaive.class);

    job.setMapperClass(NaiveMapper.class);
    job.setReducerClass(NaiveReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class NaiveMapper
       extends Mapper<LongWritable, Text, Text, Text>{
		
	//Input to the mapper is a key value pair
	//Key is a line number that has an associated text value to it
	private Text opKey = new Text();
	private Text opValue = new Text();

	//Max number of document ids
	//Each line is on input is treated as a separate document id
	//Here there are 100 for consideration
	// L signifies the long type
	public Long maxdID = 100L;
		
  public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			
			
		Long dId = Long.parseLong(value.toString().split("\t")[0]);
		opValue.set(value.toString().split("\t")[1]);
			
		//This for loop checks for the current document id and emits a key value pair	
		for (Long i = 1L; i < dId; i = i + 1L) {
			opKey.set(Long.toString(i) + "$" + Long.toString(dId));
			context.write(opKey, opValue);
		}

		//This for loop checks for the max document id as specified earlier and emits a key value pair	
		for (Long i = dId + 1L; i < maxdID + 1L; i = i + 1L) {
			opKey.set(Long.toString(dId) + "$" + Long.toString(i));
			context.write(opKey, opValue);
		}
    }
  }

  public static class NaiveReducer
       extends Reducer<Text,Text,Text,FloatWritable> {
		
		static enum CountersEnum { NUM_COMP }
		
		public static Float simT = 0.8f;
		private FloatWritable opVal = new FloatWritable();
		
    public void reduce(Text key, Iterable<Text> values, Context context
                       ) throws IOException, InterruptedException {
       			
		//We start with a document pair of A and B
		String docA = values.iterator().next().toString();
		String docB = values.iterator().next().toString();
			
		//We initialise a counter to check for the number of comparisons 
		Counter count = context.getCounter(CountersEnum.class.getName(),
		CountersEnum.NUM_COMP.toString());
		//The counter increments by one after each comparison 
		count.increment(1);
			
		//We compute the similarity between the document pair A and B	
		Float sim = computeSimilarity(docA, docB);
		
		//This if loop check if the calculated similarity is more than the threshold similarity
		// If yes we consider this pair
		if (sim > simT) {
				opVal.set(sim);
			  context.write(key, opVal);
		  }
    }
		
    public static Float computeSimilarity(String docA, String docB) {
			
		// We set the similarity threshold as given in the question
		Float sim = 0.8f;	
		Set<String> docASet = new HashSet<String>(Arrays.asList(docA.split(" ")));
		Set<String> docBSet = new HashSet<String>(Arrays.asList(docB.split(" ")));
		Set<String> union = new HashSet<String>(docASet);
		Set<String> intersection = new HashSet<String>(docBSet);
		union.addAll(docBSet);
		intersection.retainAll(docASet);
			
		return (new Float(intersection.size())) / (new Float(union.size()));
		}
  }

  }

