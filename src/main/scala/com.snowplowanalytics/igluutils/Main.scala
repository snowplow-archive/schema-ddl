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
  FileUtils   => FU,
  StringUtils => SU
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

// Scala
import scala.collection.immutable.ListMap

object Main {

  // TODO: Abstract what this function is doing into many functions
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

        // Flatten the JsonSchema
        SF.flattenJsonSchema(json) match {
          case Success(flatSchema) => {

            // Get the JsonPath and Redshift Files
            val jpf = JPG.getJsonPathsFile(flatSchema)
            val rdf = RDG.getRedshiftDdlFile(flatSchema)

            // Get the vendor/fileName -> com.mailchimp/subscribe_1 
            val combined = getFileName(flatSchema.get("self_elems").get)
            
            (jpf, rdf, combined) match {
              case (Success(jp), Success(rd), Success(name)) => {

                val vendor = name.split("/")(0)
                val file   = name.split("/")(1)

                val jsonPathDir = "/vagrant/iglu-utils-test/jsonpaths/" + vendor + "/"
                val redshiftDir = "/vagrant/iglu-utils-test/sql/" + vendor + "/"

                println(FU.writeListToFile(file + ".json", jsonPathDir, jp))
                println(FU.writeListToFile(file + ".sql", redshiftDir, rd))
                exit(0)
              }
              case (Failure(a), Failure(b), Failure(c)) => println(a + "," + b + "," + c); exit(1)
              case (Failure(a),          _,          _) => println(a); exit(1)
              case (         _, Failure(b),          _) => println(b); exit(1)
              case (         _,          _, Failure(c)) => println(c); exit(1)
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

  // TODO: Move this somewhere more appropriate
  private def getFileName(flatSelfElems: ListMap[String, Map[String, String]]): Validation[String, String] =
    flatSelfElems.get("self") match {
      case Some(values) => {
        val vendor = values.get("vendor")
        val name   = values.get("name")

        (vendor, name) match {
          case (Some(vendor), Some(name)) => {
            // Make the file name
            val file = name.replaceAll("([^A-Z_])([A-Z])", "$1_$2").toLowerCase.concat("_1")

            // Return the vendor and the file name together
            (vendor + "/" + file).success
          }
          case (_, _) => s"Error: Function - `getFileName` - Should never happen; SchemaFlattener is not catching self-desc information missing".fail
        }
      }
      case None => s"Error: Function - `getFileName` - Should never happen; SchemaFlattener is not catching self-desc information missing".fail
    }
}
