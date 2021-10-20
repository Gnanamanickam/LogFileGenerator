package HelperUtils

import scala.util.matching.Regex
import com.typesafe.config.{Config, ConfigFactory}

object LogFileUtils {

  val config = ObtainConfigReference("logConfig") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  /***
   *
   * @param mapperReduceName :  Gets the regex pattern to check with the match string
   * @param matchString : The string which has to checked for regex pattern
   * @return string if found else not found
   */
  def checkRegexPatternMatch(mapperReduceName: String, matchString: String) = {
    val pattern: Regex = config.getString("logConfig." + mapperReduceName).r
    val check: String = pattern.findFirstMatchIn(matchString) match {
      case Some(value) => value.toString()
      case None => "Not Found"
    }
    check
  }

}
