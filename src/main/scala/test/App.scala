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

package test

import java.lang.Iterable

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
 */

abstract class runTest extends Configured with Tool{}

object runTest extends Configured with Tool{
	private val logger = Logger.getLogger(classOf[runTest])
	type mapperType = Mapper[LongWritable,Text,Text,IntWritable]
	type reducerType = Reducer[Text, IntWritable, Text, IntWritable]
	
	private class mapper extends mapperType {
		override def map(key: LongWritable, value: Text, context: mapperType#Context) = {
			var line = value.asInstanceOf[Text].toString()
			var iterator = new StringTokenizer(line)
			
			while(iterator.hasMoreTokens()){
				mapper.word.set(iterator.nextToken())
				context.write(mapper.word, mapper.one)
			}
		}
	}
	private object mapper extends mapperType {
		private val one = new IntWritable(1)
		private var word = new Text()
	}
	
	
	private class reducer extends reducerType{
		override def reduce(key: Text, values: Iterable[IntWritable], context: reducerType#Context) = {
			var iter = values.iterator
			var sum = 0;
			while(iter.hasNext){
				sum += iter.next().get()
			}
			reducer.sumValue.set(sum)
			context.write(key, reducer.sumValue)
		}
	}
	private object reducer extends reducerType{
		private val sumValue = new IntWritable()
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
		System.exit(ToolRunner.run(new Configuration(), this, args))
	}
}
