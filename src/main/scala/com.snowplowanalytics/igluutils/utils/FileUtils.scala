/*
 * Copyright (c) 2014-2015 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.igluutils
package utils

// Java
import java.io.{
  PrintWriter,
  File
}

// Scalaz
import scalaz._
import Scalaz._

// Jackson
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.core.JsonParseException

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.scalaz.JsonScalaz._

/**
 * Utilities for printing and reading to/from files
 */
object FileUtils {

  /**
   * Creates a new file with the contents of the list
   * inside.
   *
   * @param fileName The name of the new file
   * @param fileDir The directory we want the file to
   *        live in
   * @param list The list of strings to be added to
   *        the new file
   * @return a success or failure string about the process
   */
  def writeListToFile(fileName: String, fileDir: String, list: List[String]): Validation[String, String] =
    try {
      // Attempt to open the file...
      val file = new File(fileDir + fileName)

      // Print the contents of the list to the new file...
      printToFile(file) { p =>
        list.foreach(p.println)
      }
      s"File [${fileDir}${fileName}] was written successfully!".success
    } 
    catch {
      case e: Exception => {
        val exception = e.toString
        s"File [${fileDir}${fileName}] failed to write: [$exception]".fail
      }
    }

  /**
   * Prints a single line to a file
   *
   * @param f The File we are going to print to
   */
  private def printToFile(f: File)(op: PrintWriter => Unit) {
    val p = new PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  /**
   * Returns a validated Array of Json's from the folder it was
   * pointed at.
   *
   * @param dir The directory we are going to get Json's from
   * @param ext The extension of the files we are going to be
   *        attempting to grab
   * @return a validated array of Json's or a failure string
   */
  def getJsonFromDir(dir: String, ext: String = "json"): Array[Validation[String, JValue]] =
    for {
      filePath <- new File(dir).listFiles.filter(_.getName.endsWith("." + ext))
    } yield {
      try {
        val file = scala.io.Source.fromFile(filePath)
        val content = file.mkString
        parse(content).success
      } catch {
        case e: JsonParseException => {
          val exception = e.toString
          s"File [$filePath] contents failed to parse into JSON: [$exception]".fail
        }
        case e: Exception => {
          val exception = e.toString
          s"File [$filePath] fetching and parsing failed: [$exception]".fail
        }
      }
    }
}
