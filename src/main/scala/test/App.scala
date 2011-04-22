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

import java.io.{DataOutput, DataInput}
import java.util.{Iterator,StringTokenizer}

import org.apache.hadoop.conf.{Configuration,Configured}
import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hadoop.io.{IntWritable,LongWritable,ArrayWritable,Text,BinaryComparable,Writable,WritableComparable,GenericWritable}
import org.apache.hadoop.mapreduce.{Job,Mapper,Reducer,TaskAttemptID,RecordReader,RecordWriter,OutputCommitter,StatusReporter,InputSplit}
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{Tool,ToolRunner}
import org.apache.log4j.Logger

import scala.collection.mutable.{Map,ArrayBuffer}
import scala.collection.JavaConversions._

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
	type mapperInKey = LongWritable
	type mapperInValue = Text
	type mapperOutKey = WritableComparableObj[BinaryComparable,BinaryComparable]
	type mapperOutValue = ArrayWritable
	type mapperType = Mapper[mapperInKey, mapperInValue, mapperOutKey, mapperOutValue]
	type reducerOutValue = ArrayWritable
	type reducerType = Reducer[mapperOutKey, mapperOutValue, Text, reducerOutValue]
	type partitionerType = HashPartitioner[mapperOutKey/*Text*/, mapperOutValue]
	
	/*implicit def tuple2ToWritable[T1 <: WritableComparable[T1], T2 <: WritableComparable[T2]](t: Tuple2[T1, T2]): WritableObj[T1, T2, T1, T2] = {
				new WritableObj[T1, T2, T1, T2](t._1, t._2)
	}*/
	
	implicit def textTuple2ToWritable(t: Tuple2[Text, Text]): WritableComparableObj[Text,Text] = {
		new WritableComparableObj[Text,Text](t._1, t._2)
	}
	
	implicit def postTuple2ToWritable(t: Tuple2[Text, ArrayWritable]) = {
		new WritableObj[Text,ArrayWritable](t._1, t._2)
	}
	
	class WritableTuple2[T1 <: WritableComparable[T1], T2 <: WritableComparable[T2]](_1: T1, _2: T2) extends Tuple2[T1, T2](_1: T1, _2: T2) with WritableComparable[WritableTuple2[T1, T2]]{
		override def write(out: DataOutput) = {
			_1.write(out)
			_2.write(out)
		}
		override def readFields(in: DataInput) = {
			_1.readFields(in)
			_2.readFields(in)
		}
		override def compareTo(o: WritableTuple2[T1, T2]):Int = {
			var result = _1.compareTo(o._1)
			if (result == 0){
				result = _2.compareTo(o._2)
			}
			result
		}
	}
	
	class WritableObj[T1 <: Writable, T2 <: Writable]()
		extends GenericWritable{
		
		private var _a: Option[T1] = None
		private var _b: Option[T2] = None
		
		def _1 = {
			_a match {
				case Some(e) => e
				case None => throw new NullPointerException()
			}
		}
		
		def _1_=(a: T1) ={
			_a = Some(a)
		}
		
		def _2 = {
			_b match {
				case Some(e) => e
				case None => throw new NullPointerException()
			}
		}
		
		def _2_=(b: T2) ={
			_b = Some(b)
		}
		
		def this(a: T1, b: T2){
			this()
			_1 = a
			_2 = b
		}
		
		def getTypes() = {
			Array(_1.getClass.asInstanceOf[Class[T1]], _2.getClass.asInstanceOf[Class[T2]])
			//see: http://stackoverflow.com/questions/1135248/scala-equivalent-of-java-java-lang-classt-object
			//and: http://lampsvn.epfl.ch/trac/scala/ticket/490
		}
	}
	
	class WritableComparableObj[T1 <: WritableComparable[T1], T2 <: WritableComparable[T2]]
		extends WritableObj[WritableComparable[_ >: T1], WritableComparable[_ >: T2]]()
		with WritableComparable[WritableComparableObj[T1, T2]]{
		
		private var _a: Option[T1] = None
		private var _b: Option[T2] = None
		
		override def _1: T1 = {
			super._1.asInstanceOf[T1]
		}
		
		override def _2: T2 = {
			super._1.asInstanceOf[T2]
		}
		
		def this(a: WritableComparable[_ >: T1], b: WritableComparable[_ >: T2]) = {
			this()
			_1 = a
			_2 = b
		}
		
		override def compareTo(o: WritableComparableObj[T1, T2]): Int = {
			var result = _1.compareTo(o._1)
			if (result == 0){
				result = _2.compareTo(o._2)
			}
			result
		}
	}
		
	private class mapper extends mapperType {
		
		/*class Context(conf: Configuration, taskid: TaskAttemptID, reader: RecordReader[mapperInKey,mapperInValue],  writer: RecordWriter[mapperOutKey,mapperOutValue], committer: OutputCommitter, reporter: StatusReporter, split: InputSplit ) 
			extends super.Context(conf, taskid, reader,  writer, committer, reporter, split){
			type TupleType = Tuple2[Text,Text] // don't know why, but otherwise the view bound doesn't work
			
			def write[TupleType <% WritableComparableObj[Text,Text,Text,Text]](key: TupleType, value: mapperOutValue) = {
				logger.info("MYCONTEXT WRITE CALLED")
				super.write(key, value)
			}
		}*/
		
		/*class Context(superwriter: (mapperOutKey, mapperOutValue) => Unit){
			type TupleType = Tuple2[Text,Text] // don't know why, but otherwise the view bound doesn't work
			
			def write[TupleType <% mapperOutKey](key: TupleType, value: mapperOutValue) = {
				logger.info("MYCONTEXT WRITE CALLED")
				superwriter(key, value)
			}
		}
		
		implicit def toInnerContext(context: mapperType#Context) =
			new Context(context.write(_,_))*/
		
		override def map(key: LongWritable, value: Text, context: mapperType#Context) = {
			var line = value.toString().split("\t",2)
			mapper.title.set(line(0))
			var postings: Map[(Text,Text),ArrayBuffer[IntWritable]] = Map()
			var position = 0
			for (w <- line(1).split("\\s")){
				mapper.word.set(w)
				postings((mapper.word, mapper.title)) =
					postings.getOrElse((mapper.word, mapper.title), new ArrayBuffer()) += new IntWritable(position)
				logger.debug("word: "+mapper.word+" title: "+mapper.title+" data: "+postings((mapper.word, mapper.title)))
				position += w.length
			}
			for ( (key, positions) <- postings.iterator){
				mapper.positionArray.set(positions.toArray)
				context.write(
						new WritableComparableObj[BinaryComparable,BinaryComparable](key._1.asInstanceOf[Text],key._2.asInstanceOf[Text]),
						mapper.positionArray
				)
			}
			
			// emit/save total document length
		}
		
	}
	private object mapper extends mapperType {
		private var word = new Text()
		private var title = new Text()
		private val positionArray = new ArrayWritable(classOf[IntWritable])
	}
	
	/*class partitioner extends partitionerType{
		override def getPartition(key: mapperOutKey, value: mapperOutValue, numPartitions: Int): Int = {
			logger.info("PARTITIONER CALLED")
			return (key._1.hashCode())%numPartitions
			//super.getPartition(key._1, value, numPartitions)
		}
	}*/
	
	private class reducer extends reducerType{
		var previousWord: Text = null
		val postings = new ArrayWritable(classOf[WritableObj[Text,mapperOutValue]])
		val postingsBuffer = new ArrayBuffer[WritableObj[Text,mapperOutValue]]()
		val positionsArray = new ArrayWritable(classOf[mapperOutValue])
		var tempArray = new ArrayWritable(classOf[mapperOutValue])
				
		override def reduce(key: mapperOutKey, values: Iterable[mapperOutValue], context: reducerType#Context) = {
			logger.info("REDUCER CALLED")
			if (key._1 != previousWord && previousWord != null){
				postings.set(postingsBuffer.toArray)
				context.write(previousWord, postings)
				postingsBuffer.clear()
			}
			tempArray = new ArrayWritable(classOf[mapperOutValue])
			tempArray.set(values.map(v => v.toArray.asInstanceOf[Array[Writable]]).foldLeft(Array[Writable]())(_++_))
					
			postingsBuffer += ((key._2.asInstanceOf[Text], tempArray))
			previousWord = key._1.asInstanceOf[Text]
			
			/*var iter = values.iterator
			var sum = 0;
			while(iter.hasNext){
				sum += iter.next().get()
			}
			reducer.sumValue.set(sum)
			context.write(key, reducer.sumValue)*/
		}
		
		override def cleanup(context: reducerType#Context) = {
			postings.set(postingsBuffer.toArray)
			context.write(previousWord, postings)
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
		
		job.setMapOutputKeyClass(classOf[mapperOutKey])
		
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[reducerOutValue])

		job.setMapperClass(classOf[mapper])
		job.setCombinerClass(classOf[reducer])
		//job.setPartitionerClass(classOf[partitioner])
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
