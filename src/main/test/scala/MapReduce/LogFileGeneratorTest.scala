package MapReduce

import Generation.RSGStateMachine.unit
import Generation.RandomStringGenerator
import HelperUtils.LogFileUtils.config
import com.mifmif.common.regex.Generex
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ToolRunner

import scala.io.Source.*

class LogFileGeneratorTest extends AnyFlatSpec with Matchers {

  val minLength = 10
  val maxLength = 10
  val randomSeed = 1
  val lines = fromFile("data/sample.txt").getLines.toString()
  val rsg = RandomStringGenerator((minLength, maxLength), randomSeed)

  behavior of "Configuration Parameters Module"

  // To check whether the timeStamp startTime is same as the one given in config file
  it should "check the timeStamp startTime config" in {
    config.getString(simulationName) shouldBe 01:53:32.479
  }

  // To check whether the timeStamp endTime is same as the one given in config file
  it should "check the timeStamp endTime config" in {
    config.getString(simulationName) shouldBe 01:55:12.339
  }

  // To check whether the timeStamp in the sample input file is same as the one expected
  it should "check file timestamp" in {
    lines.split(" ")(0) shouldBe 01:52:34.402
  }

  // To check whether the log level in the sample input file is same as the one expected
  it should "check file log level" in {
    lines.split(" ")(2) shouldBe INFO
  }

  // To check whether the seperator in the config file is same as the one expected
  def testConfigurationValues(): Unit ={
    assertEquals(",", config.getString("separator"))
  }

  // To locate an instance of the pattern in the randomly generated string
  it should "locate an instance of the pattern in the generated string" in {
    val patternString = "[\\d]+"
    val generex: Generex = new Generex(patternString)
    val genString = generex.random()
    genString should include regex patternString.r
  }

  // check whether a random generated string length is lesser than the minimum length
  it should "generate a random string whose length is lesser than the min length" in {
    val generationStep = init(rsg)
    assert(generationStep._1.length shouldBe < (minLength))
  }


}