/*
 * Cloud9: A MapReduce Library for Hadoop
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

package test

//import java.io.IOException;
import java.util.Iterator
import java.util.StringTokenizer


import org.apache.hadoop.conf.{Configuration,Configured}
import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hadoop.io.{IntWritable,LongWritable,Text}
import org.apache.hadoop.mapreduce.{Job,Mapper,Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{Tool,ToolRunner}
import org.apache.log4j.Logger

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
 * @author Jimmy Lin
 */

abstract class runTest extends Configured with Tool{}

object runTest extends Configured with Tool{
	private val logger = Logger.getLogger(classOf[runTest])
	
	private class mapper extends Mapper[LongWritable,Text,Text,IntWritable] {}
	private object mapper extends Mapper[LongWritable,Text,Text,IntWritable] {
		private val one = new IntWritable(1)
		private var word = new Text()
		
		def map(key: LongWritable, value: Text, context: Context) = {
			var line = value.asInstanceOf[Text].toString()
			var iterator = new StringTokenizer(line)
			
			while(iterator.hasMoreTokens()){
				word.set(iterator.nextToken())
				context.write(word, one)
			}
		}
	}
	
	private class reducer extends Reducer[Text, IntWritable, Text, IntWritable]{}
	private object reducer extends Reducer[Text, IntWritable, Text, IntWritable]{
		private val sumValue = new IntWritable()
		
		def reduce(key: Text, values: Iterable[IntWritable], context: Context) = {
			var iter = values.iterator
			var sum = 0;
			while(iter.hasNext){
				sum += iter.next().get()
			}
			sumValue.set(sum)
			context.write(key, sumValue)
		}
	}
	
	private def printUsage() = {
		System.out.println("usage: [input-path] [output-path] [num-reducers]")
		ToolRunner.printGenericCommandUsage(System.out)
	}
	
	def run(args: Array[String]): Int = {
		if (args.length != 3) {
			printUsage()
			return -1
		}
		
		var (inputPath, outputPath, reduceTasks) = (args(0), args(1), args(2).toInt)
		
		logger.info("Tool: DemoWordCount");
		logger.info(" - input path: " + inputPath);
		logger.info(" - output path: " + outputPath);
		logger.info(" - number of reducers: " + reduceTasks);
		
		var conf = new Configuration()
		var job = new Job(conf, "DemoWordCount")
		job.setJarByClass(classOf[runTest])
		
		job.setNumReduceTasks(reduceTasks)
		
		FileInputFormat.setInputPaths(job, new Path(inputPath))
		FileOutputFormat.setOutputPath(job, new Path(outputPath))

		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[IntWritable])

		job.setMapperClass(classOf[mapper])
		job.setCombinerClass(classOf[reducer])
		job.setReducerClass(classOf[reducer])

		// Delete the output directory if it exists already
		var outputDir = new Path(outputPath)
		FileSystem.get(conf).delete(outputDir, true)

		var startTime = System.currentTimeMillis()
		job.waitForCompletion(true)
		logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds")

		return 0
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	def main (args: Array[String]) = {
		//println("hello")
		System.exit(ToolRunner.run(new Configuration(), this, args))
	}
}

/*public class RunTest extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(RunTest.class);

	// mapper: emits (token, 1) for every word occurrence
	private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// reuse objects to save overhead of object creation
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	// reducer: sums up all the counts
	private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// reuse objects
		private final static IntWritable SumValue = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// sum up values
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			SumValue.set(sum);
			context.write(key, SumValue);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public RunTest() {
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int reduceTasks = Integer.parseInt(args[2]);

		sLogger.info("Tool: DemoWordCount");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of reducers: " + reduceTasks);

		Configuration conf = new Configuration();
		Job job = new Job(conf, "DemoWordCount");
		job.setJarByClass(RunTest.class);

		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RunTest(), args);
		System.exit(res);
	}
}*/


/*package test



/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
  }

}*/
