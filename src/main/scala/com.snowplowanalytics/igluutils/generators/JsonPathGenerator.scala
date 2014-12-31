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
package generators

// Utils
import utils.{StringUtils => SU}

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

/**
 * Converts lists of keys into a JsonPath file.
 */
object JsonPathGenerator {

  private object JsonPathPrefix {
    val Schema    = "$.schema."
    val Hierarchy = "$.hierarchy."
    val Data      = "$.data."
  }

  private val JsonPathSchemaFields = List(
    "vendor",
    "name",
    "format",
    "version"
  )

  private val JsonPathHierarchyFields = List(
    "rootId",
    "rootTstamp",
    "refRoot",
    "refTree",
    "refParent"
  )

  /**
   * Returns a validated JsonPath file based on the default
   * fields provided and the flattened schema.
   *
   * @param flatSchema A flattened schema containing all
   *        the paths that need to be added for the data
   *        fields
   * @return a JsonPath String containing all of the relevant
   *         fields or a Failure string
   */
  def getJsonPathsFile(flatSchema: Map[String, ListMap[String, Map[String,String]]]): Validation[String, String] = {

    // Append a Prefix to all of the fields
    val schemaFieldList = SU.appendToStrings(JsonPathSchemaFields, JsonPathPrefix.Schema)
    val hierarchyFieldList = SU.appendToStrings(JsonPathHierarchyFields, JsonPathPrefix.Hierarchy)
    val dataFieldList = flatSchema.get("flat_elems") match {
      case Some(elems) => SU.appendToStrings(elems.keys.toList, JsonPathPrefix.Data).success
      case None => s"Error: Function - `getJsonPathFile` - Should never happen; check the key used to store the fields in SchemaFlattener".fail
    }

    // Combine all of the lists into one and return...
    dataFieldList match {
      case (Success(data)) => {
        pretty {
          ("jsonpaths" -> (((schemaFieldList ++ hierarchyFieldList ++ data))) )
        }.success
      }
      case (Failure(str)) => str.fail
    }
  }
}
