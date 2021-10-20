import java.lang.Iterable
import java.util.StringTokenizer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConverters.*
import HelperUtils.{CreateLogger, LogFileUtils, ObtainConfigReference}

import java.time.LocalTime

class TimeIntervalLogMapReduce

object TimeIntervalLogMapReduce {

  val logger = CreateLogger(classOf[TimeIntervalLogMapReduce])

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
      if(! matchString.equals("Not Found") && timeStamp.isAfter(startTime) && timeStamp.isBefore(endTime)) {
        log.set(token)
        context.write(log, count)
      }
    }
  }

  class SumReader extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      logger.info("Calculate the sum")
      // calculate the sum
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }


  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    // Set this configuration to convert the output to csv file
    configuration.set("mapred.textoutputformat.separator", ",")
    val job = Job.getInstance(configuration,"log count distribution")
    //Set TimeIntervalLogMapReduce class to be called by hadoop
    job.setJarByClass(this.getClass)
    //Set the mapper implementation class
    job.setMapperClass(classOf[TokenizerMapper])
    //Set the combiner implementation class
    job.setCombinerClass(classOf[SumReader])
    //Set the reducer implementation class
    job.setReducerClass(classOf[SumReader])
    //Set the output class
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    // Output class format
    job.setOutputValueClass(classOf[IntWritable])
    //Input path set as a commandline argument
    FileInputFormat.addInputPath(job, new Path(args(0)))
    //Output path set as a commandline argument
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    //Exit after the job completes
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }

}