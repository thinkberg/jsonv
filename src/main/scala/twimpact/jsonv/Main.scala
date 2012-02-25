/*
 * Copyright (c) 2012 TWIMPACT UG (haftungsbeschraenkt). All rights reserved.
 */

package twimpact.jsonv

import collection.JavaConverters._
import grizzled.slf4j.Logging
import net.minidev.json.parser.JSONParser
import net.minidev.json.{JSONArray, JSONObject}
import java.io._
import java.util.zip.GZIPInputStream

/**
 * >>Describe Class<<
 *
 * @author Matthias L. Jugel
 */

object Main extends Logging {
  private val jsonParser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE)

  private def usage(message: Option[String] = None) {
    message.foreach(error(_))
    error("usage: jsonv [-i] <dump>")
    error("       jvonv [-csv|-tsv] <fields> <dump>")
    error("")
    error("Start by looking up the fields info from the dump using -i.")
    error("Then dump your information in CSV or TSV format by providing the")
    error("fields (comma separated list with no spaces) and the dump file.")
    System.exit(0)
  }

  private def getFileReader(dump: File): BufferedReader = {
    dump match {
      case f if (f.getName.endsWith("gz")) =>
        new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f))))
      case f =>
        new BufferedReader(new InputStreamReader(new FileInputStream(f)))
    }
  }

  val SEPARATOR = "sep"

  def main(args: Array[String]) {
    info("jsonv - (c) 2012 Matthias L. Jugel")
    if (args.length == 0) usage()
    var printInfo = false
    var settings: Map[String, String] = Map(SEPARATOR -> "\t")
    var input: List[String] = Nil
    args.foreach {
      arg => arg match {
        case "-i" =>
          printInfo = true
        case "-csv" =>
          settings += (SEPARATOR -> ";")
        case "-tsv" =>
          settings += (SEPARATOR -> "\t")
        case i =>
          input = i :: input
      }
    }

    input = input.reverse

    if (input.lastOption.isEmpty) {
      usage(Some("error: please provide a dump file name"))
    }
    val dump = new File(input.last)
    if (!dump.exists()) {
      usage(Some("error: dump file does not exist"))
    }

    if (printInfo) {
      info("Dumping field information from %s".format(dump))
      val r = getFileReader(dump)
      var line = r.readLine

      while (line != null) {
        // skip empty lines
        if (line.trim.length > 0) {
          try {
            val json = jsonParser.parse(line)
            json match {
              case j: JSONObject =>
                println(j.keySet.asScala.mkString(","))
              case j: JSONArray =>
                println("@array")
              case j: Object =>
                info("Only found primitive type in json data: %s".format(j))
            }
            // stop
            line = null
          } catch {
            case e: Exception =>
              debug("line can't be parsed: %s".format(e.getMessage))
              line = r.readLine
          }
        } else {
          line = r.readLine
        }
      }
      r.close()
      System.exit(0)
    } else {
      val fields = input.head.split(",").toList
      info("dumping fields: %s".format(fields))
      val r = getFileReader(dump)
      var line = r.readLine
      while (line != null) {
        val trimmedLine = line.trim
        if (trimmedLine.length > 0)
          try {
            val json = jsonParser.parse(trimmedLine)
            json match {
              case j: JSONArray if (fields.contains("@array")) =>
                println(j.toArray.mkString(settings(SEPARATOR)))
              case j: JSONObject =>
                println(fields.flatMap(f => value(j, f.split("\\."))).map(_.replaceAll("[\\n\\r]+", " "))
                    .mkString(settings(SEPARATOR)))
              case j: Object =>
                info("Only found primitive type in json data: %s".format(j))
            }
          } catch {
            case e: Exception => debug("line can't be parsed: %s".format(e.getMessage))
          }
        line = r.readLine
      }
    }
  }

  private def value(o: Object, keys: Seq[String]): Option[String] = {
    if (keys.length > 1 && o.isInstanceOf[JSONObject])
      value(o.asInstanceOf[JSONObject].get(keys.head), keys.drop(1))
    else {
      val key = keys.head
      o match {
        case j: JSONArray if (key == "@array") =>
          error("%s: sub-arrays not supported".format(key))
          None
        case j: JSONObject =>
          Some(j.get(key).toString)
        case j: Object =>
          Some(j.toString)
      }
    }
  }
}
