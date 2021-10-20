import LongestLogMapReduce.TokenizerMapper

import java.lang.Iterable
import java.util.StringTokenizer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import HelperUtils.{LogFileUtils, CreateLogger, ObtainConfigReference}

import scala.collection.JavaConverters.*

class LongestLogMapReduce

object LongestLogMapReduce {

  val logger = CreateLogger(classOf[LongestLogMapReduce])

  val config = ObtainConfigReference("logConfig") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val log = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      logger.info("Converting the input string into string Array using Regex Space Split")
      val stringArray = value.toString.split(config.getString("logConfig.regexSpaceSplit"))
        val token = stringArray(2)
        val matchString = LogFileUtils.checkRegexPatternMatch("regexPattern", stringArray(5))
        if(! matchString.equals(config.getString("notFound"))) {
          log.set(token)
          val length = stringArray(5).length
          context.write(log, new IntWritable(length))
        }
      }
    }


  class maxReader extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      logger.info("To find the max value")
      val maximumValue = values.asScala.foldLeft(0)(_ max _.get)
      context.write(key, new IntWritable(maximumValue))
    }
  }


  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration,"longest log")
    //Set LongestLogMapReduce class to be called by hadoop
    job.setJarByClass(this.getClass)
    //Set the mapper implementation class
    job.setMapperClass(classOf[TokenizerMapper])
    //Set the combiner implementation class
    job.setCombinerClass(classOf[maxReader])
    //Set the reducer implementation class
    job.setReducerClass(classOf[maxReader])
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