import HelperUtils.LogFileUtils

import java.lang.Iterable
import java.util.StringTokenizer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import HelperUtils.{CreateLogger, LogFileUtils, ObtainConfigReference}

import java.time.LocalTime
import scala.collection.JavaConverters.*

class TimeIntervalErrorLogMapReduce

object TimeIntervalErrorLogMapReduce {

  val logger = CreateLogger(classOf[TimeIntervalErrorLogMapReduce])

  val config = ObtainConfigReference("logConfig") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val count = new IntWritable(1)
    val log = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      logger.info("Converting the input string into string Array using Regex Space Split")
      val stringArray = value.toString.split(config.getString("logConfig.regexSpaceSplit"))
      val token = stringArray(2)
      val timeStamp = LocalTime.parse(stringArray(0))
      val startTime = LocalTime.parse(config.getString("startTime"))
      val endTime = LocalTime.parse(config.getString("endTime"))
      val matchString = LogFileUtils.checkRegexPatternMatch("regexPattern", stringArray(5))
      if(! matchString.equals("Not Found") && timeStamp.isAfter(startTime) && timeStamp.isBefore(endTime) && token.contains("ERROR")) {
        log.set(token)
        context.write(log, count)
      }
    }
  }

  class SumReader extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      logger.info("Calculate the sum")
      // calculate the sum
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  class TokenizerMapper2 extends Mapper[Object, Text, Text, Text] {

    val sum = new Text()
    val time = new Text()
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      val k = value.toString.split("\t")(1)
      val v = value.toString.split("\t")(0)
      sum.set(k)
      time.set(v)
      context.write(sum, time)
    }
  }

  class SortComparator extends WritableComparator(classOf[Text], true) {

    override def compare(x: WritableComparable[_], y: WritableComparable[_]): Int = {
      val value1 = x.toString
      val value2 = y.toString

      value2.compareTo(value1)
    }

  }

  class SumReader2 extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      logger.info("Set the key value pair in the reducer")
      // set key value pair in the context for all values .
      values.asScala.foreach((value) => {
        context.write(value, key)
      })
    }
  }


  def main(args: Array[String]): Unit = {
    val configuration1 = new Configuration
    val job1 = Job.getInstance(configuration1, "Error log in descending order")
    //Set TimeIntervalErrorLogMapReduce class to be called by hadoop
    job1.setJarByClass(this.getClass)
    //Set the mapper implementation class
    job1.setMapperClass(classOf[TokenizerMapper])
    //Set the combiner implementation class
    job1.setCombinerClass(classOf[SumReader])
    //Set the reducer implementation class
    job1.setReducerClass(classOf[SumReader])
    //Set the output class
    job1.setOutputKeyClass(classOf[Text])
    // Output class format
    job1.setOutputValueClass(classOf[IntWritable])
    //Input path set as a commandline argument
    FileInputFormat.addInputPath(job1, new Path(args(0)))
    //Output path set as a commandline argument
    FileOutputFormat.setOutputPath(job1, new Path(args(1)))

    job1.waitForCompletion(true)



    val configuration2 = new Configuration
    val job2 = Job.getInstance(configuration2, "Error log in descending order")
    //Set TimeIntervalErrorLogMapReduce class to be called by hadoop
    job2.setJarByClass(this.getClass)
    //Set the mapper implementation class
    job2.setMapperClass(classOf[TokenizerMapper2])
    // Set the comparator implementation class
    job2.setSortComparatorClass(classOf[SortComparator])
    //Set the reducer implementation class
    job2.setReducerClass(classOf[SumReader2])
    //Set the output class
    job2.setOutputKeyClass(classOf[Text])
    // Output class format
    job2.setOutputValueClass(classOf[Text])
    //Input path set as a commandline argument
    FileInputFormat.addInputPath(job2, new Path(args(1)))
    //Output path set as a commandline argument
    FileOutputFormat.setOutputPath(job2, new Path(args(2)))

    job2.waitForCompletion(true)
    //Exit after the job completes
    System.exit(if (job2.waitForCompletion(true)) 0 else 1)
  }

}

//Comparator to sort in descending order
class SortComparator extends WritableComparator(classOf[IntWritable], true) {

  override def compare(x: WritableComparable[_], y: WritableComparable[_]): Int = {
    val value1 = x.asInstanceOf[IntWritable]
    val value2 = y.asInstanceOf[IntWritable]

    value2.compareTo(value1)
  }

}