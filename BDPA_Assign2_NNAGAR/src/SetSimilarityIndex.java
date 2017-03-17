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

public class SetSimilarityIndex {

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "set similarity index");

    job.setJarByClass(SetSimilarityIndex.class);

    job.setMapperClass(IndexMapper.class);
    job.setReducerClass(IndexReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


	public static Float simT = 0.8f;

  public static class IndexMapper
       extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text opKey = new Text();
		
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
											
			String[] words = value.toString().split("\t")[1].split(" ");
			
			// Straightforward mapper implementation
			//Reads each doc , calculates the number of words as per the formula and assigns it to the index Count
			// The idea is to create key value pairs of words and docs
			int Count = ((Double) (words.length - Math.ceil(simT * words.length) + 1)).intValue();
			
			for (int i = 0; i < Count; i += 1) {
				opKey.set(words[i]);
				context.write(opKey, value);
			}
    }
  }

  public static class IndexReducer
       extends Reducer<Text,Text,Text,FloatWritable> {
		
		static enum CountersEnum { NUM_COMP }
		
		private Text opKey = new Text();
		private FloatWritable opValue = new FloatWritable();
		
    public void reduce(Text key, Iterable<Text> values, Context context
                       ) throws IOException, InterruptedException {

      			// The key value pairs of word indexes and each line as a doc,created after the mapper are given to the reducer 
			// as per the logic of this approach the reducer will iterate over all the pair and calculate similarities

			List<Long> dId = new ArrayList<Long>();
			List<String> d = new ArrayList<String>();
			
			for (Text val : values) {
				dId.add(Long.parseLong(val.toString().split("\t")[0]));
				d.add(val.toString().split("\t")[1]);
			}
			
			for (int i = 0; i < dId.size(); i += 1) {
				Long dId1 = dId.get(i);
				String d1 = d.get(i);
				
				for (int j = i + 1; j < dId.size(); j += 1) {
					Long dId2 = dId.get(j);
					String d2 = d.get(j);
					
					// Calculates similarity by invoking the method at the bottom for calculation as per the formula
					Float sim = computeSimilarity(d1, d2);
					
					Counter count = context.getCounter(CountersEnum.class.getName(),
					CountersEnum.NUM_COMP.toString());
					count.increment(1);	
					// The counter above is the same as used in the previous approach
					// It counts the number of comparisons and increments by one for each successive one
					// below if the if loop to check if the calculated similarity is more than the threshold 
					// I yes we consider that pair
				  if (sim > simT) {
						String idPair = (dId1 < dId2) ? Long.toString(dId1) + "$" + Long.toString(dId2):Long.toString(dId2) + "$" + Long.toString(dId1);
						opKey.set(idPair);
						opValue.set(sim);
					  context.write(opKey, opValue);
				  }
				}
			}
    }
		
		public static Float computeSimilarity(String d1, String d2) {
			
			//Float simT = 0.8f;	
			Set<String> d1Set = new HashSet<String>(Arrays.asList(d1.split(" ")));
			Set<String> d2Set = new HashSet<String>(Arrays.asList(d2.split(" ")));
			Set<String> union = new HashSet<String>(d1Set);
			Set<String> intersection = new HashSet<String>(d2Set);
			union.addAll(d2Set);
			intersection.retainAll(d1Set);
			// as given in the instructions 
			return (new Float(intersection.size())) / (new Float(union.size()));
		}
  }

  }


