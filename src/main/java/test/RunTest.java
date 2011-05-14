/*
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * <p>
 * Simple word count demo. This Hadoop Tool counts words in flat text file, and
 * takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path] input path</li>
 * <li>[output-path] output path</li>
 * <li>[num-reducers] number of reducers</li>
 * </ul>
 *
 */


public class RunTest extends Configured implements Tool{
	private static final Logger logger = Logger.getLogger(RunTest.class);
	
	private static class mapper extends Mapper<LongWritable,Text,WritableComparableTuple2<Text,Text>,ArrayWritable>{
		//these were all static
		Text word = new Text();
		Text title = new Text();
		WritableComparableTuple2<Text,Text> wordplustitle;
		final ArrayWritable positionArray = new ArrayWritable(IntWritable.class);
		ArrayList<IntWritable> positionsList;
		final IntWritable[] dummyArray = new IntWritable[0];
		
		public mapper() {}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t",2);
			title.set(line[0]);
			HashMap<WritableComparableTuple2<Text,Text>,ArrayList<IntWritable>> postings = new HashMap<WritableComparableTuple2<Text,Text>,ArrayList<IntWritable>>();
			int position = 0;
			for (String w : line[1].split("\\s")){
				word.set(w);
				wordplustitle = new WritableComparableTuple2<Text,Text>(word, title);
				positionsList = postings.containsKey(wordplustitle)?postings.get(wordplustitle):new ArrayList<IntWritable>();
				positionsList.add(new IntWritable(position));
				postings.put(wordplustitle , positionsList);
				logger.debug("word: "+word+" title: "+title+" data: "+positionsList);
				position += w.length();
			}
			for ( Map.Entry<WritableComparableTuple2<Text,Text>,ArrayList<IntWritable>> entry : postings.entrySet() ){
				positionArray.set(entry.getValue().toArray(dummyArray));
				context.write(entry.getKey(), positionArray);
			}
			
			// emit/save total document length
		}
	}
	
	static class partitioner extends HashPartitioner<WritableComparableTuple2<Text,Text>, ArrayWritable>{
		
		public int getPartition(WritableComparableTuple2<Text,Text> key, ArrayWritable value, int numPartitions){
			return (key._1.hashCode())%numPartitions;
			//super.getPartition(key._1, value, numPartitions)
		}
	}
		
	private class reducer extends Reducer<WritableComparableTuple2<Text,Text>,ArrayWritable,Text,ArrayWritable>{
			Text previousWord = null;
			final ArrayWritable postings = new ArrayWritable(WritableTuple2.class);
			final ArrayList<WritableTuple2<Text, ArrayWritable>> postingsBuffer = new ArrayList<WritableTuple2<Text, ArrayWritable>>();
			ArrayWritable tempArray = new ArrayWritable(ArrayWritable.class);
			final ArrayList<IntWritable> mergedList = new ArrayList<IntWritable>();
			
			final IntWritable[] dummyArrayInt = new IntWritable[0];
			@SuppressWarnings("unchecked")
			final WritableTuple2[] dummyArrayTuple = new WritableTuple2[0]; 
					
			@Override
			protected void reduce(WritableComparableTuple2<Text,Text> key, Iterable<ArrayWritable> values, Context context)
				throws IOException, InterruptedException{
				
				logger.info("REDUCER CALLED");
				if (key._1 != previousWord && previousWord != null){
					postings.set(postingsBuffer.toArray(dummyArrayTuple));
					context.write(previousWord, postings);
					postingsBuffer.clear();
				}
				
				for (ArrayWritable v : values){
					mergedList.addAll(Arrays.asList((IntWritable[])v.toArray()));
				}
				tempArray.set(mergedList.toArray(dummyArrayInt));
				mergedList.clear();
						
				postingsBuffer.add(new WritableTuple2<Text, ArrayWritable>(key._2, tempArray));
				previousWord = key._1;
				
			}
			
			@Override
			protected void cleanup(Context context) throws IOException, InterruptedException{
				postings.set(postingsBuffer.toArray(dummyArrayTuple));
				context.write(previousWord, postings);
			}
		}
		
		private void printUsage(){
			System.out.println("usage: [input-path] [output-path] [num-reducers]");
			ToolRunner.printGenericCommandUsage(System.out);
		}
		
		public int run(String[] args) throws Exception{
			if (args.length != 3) {
				printUsage();
				return -1;
			}
			
			String inputPath = args[0];
			String outputPath = args[1];
			int reduceTasks = Integer.parseInt(args[2]);
			
			logger.info("Tool: DemoWordCount");
			logger.info(" - input path: " + inputPath);
			logger.info(" - output path: " + outputPath);
			logger.info(" - number of reducers: " + reduceTasks);
			
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Indexer");
			job.setJarByClass(RunTest.class);
			
			job.setNumReduceTasks(reduceTasks);
			
			FileInputFormat.setInputPaths(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setMapOutputKeyClass(WritableComparableTuple2.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(ArrayWritable.class);

			job.setMapperClass(mapper.class);
			job.setCombinerClass(reducer.class);
			job.setPartitionerClass(partitioner.class);
			job.setReducerClass(reducer.class);

			// Delete the output directory if it exists already
			Path outputDir = new Path(outputPath);
			FileSystem.get(conf).delete(outputDir, true);

			Long startTime = System.currentTimeMillis();
			
			job.waitForCompletion(true);
			
			logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds");

			return 0;
		}
		
		public RunTest() {}
	
		/**
		 * Dispatches command-line arguments to the tool via the
		 * <code>ToolRunner</code>.
		 */
		public static void main (String[] args) throws Exception{
			System.exit(ToolRunner.run(new Configuration(), new RunTest(), args));
		}

}