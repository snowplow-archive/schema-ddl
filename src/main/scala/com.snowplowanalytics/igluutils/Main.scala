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

// Generators
import generators.{
  JsonPathGenerator    => JPG,
  RedshiftDdlGenerator => RDG,
  SchemaFlattener      => SF
}

// Utilities
import utils.{
  FileUtils => FU
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

  def main(args: Array[String]) {

    if (args.size == 0) {
      println("No arguments passed to function!")
      exit(1)
    }
    
    // Get the path to the json schema...
    val path = args(0)

    // Fetch and parse the JSON...
    FU.getJsonFromPath(path) match {
      case Success(json) => {
        SF.flattenJsonSchema(json) match {
          case Success(flatSchema) => {
            val jpf = JPG.getJsonPathsFile(flatSchema)
            val rdf = RDG.getRedshiftDdlFile(flatSchema)

            (jpf, rdf) match {
              case (Success(jp), Success(rd)) => {
                jp.foreach {println}
                rd.foreach {println}

                println("Success")
                exit(0)
              }
              case (Failure(a), Failure(b)) => println(a + "," + b); exit(1)
              case (Failure(a), _)        => println(a); exit(1)
              case (_, Failure(b))        => println(b); exit(1)
            }
          }
          case Failure(str) => {
            println(str)
            exit(1)
          }
        }
      }
      case Failure(str) => {
        println(str)
        exit(1)
      }
    }
  }
}
