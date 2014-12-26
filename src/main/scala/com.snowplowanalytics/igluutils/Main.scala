/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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

// Generators
import generators.{
  JsonPathGenerator    => JPG,
  RedshiftDdlGenerator => RDG,
  SchemaFlattener      => SF
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

object Main {

  // TODO: remove folder hardcoding
  private val PathToRoot = "//vagrant//Github//iglu-utils//src//test//resources//"

  def main(args: Array[String]) {

    val returned = getSchemaFromFolder(PathToRoot).toList

    val successes = for {
      Success(json) <- returned
    } yield {
      SF.flattenJsonSchema(json)
    }

    println(successes)
  }

  /**
   * Returns a validated List of Json Schemas from the folder it was
   * pointed at.
   *
   * @param dir The directory we are going to get Schemas from
   * @param ext The extension of the files we are going to be
   *        attempting to grab
   * @return an Array with validated JSONs nested inside
   */
  private def getSchemaFromFolder(dir: String, ext: String = "json"): Array[Validation[String, JValue]] =
    for {
      filePath <- new java.io.File(dir).listFiles.filter(_.getName.endsWith("." + ext))
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
