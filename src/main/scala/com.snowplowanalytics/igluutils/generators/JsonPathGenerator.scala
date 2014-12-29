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

  private object JsonPathAddOn {
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

  private val JsonPathHierarchyFields = List (
    "rootId",
    "rootTstamp",
    "refRoot",
    "refTree",
    "refParent"
  )

  private val JsonPathDefaultWrapper = parse("""{"jsonpaths":[]}""")

  /**
   * Returns a valid JsonPath file based on the default
   * fields provided and the flattened schema.
   *
   * @param flatSchema A flattened schema containing all
   *        the paths that need to be added for the data
   *        fields
   * @return a JsonPath containing all of the relevant
   *         fields
   */
  def getJsonPath(flatSchema: Map[String, ListMap[String, Map[String,String]]]): JValue = {

    // Convert all of the field lists to lists of JStrings
    val schemaFieldList = transformStringList(JsonPathSchemaFields, JsonPathAddOn.Schema)
    val hierarchyFieldList = transformStringList(JsonPathHierarchyFields, JsonPathAddOn.Hierarchy)
    val dataFieldList = flatSchema.get("flat_elems") match {
      case Some(elems) => transformStringList(elems.keys.toList, JsonPathAddOn.Data)
      case None => List()
    }

    // Combine all of the lists into one
    val fieldList = schemaFieldList ++ hierarchyFieldList ++ dataFieldList

    // Insert the field list into the JsonPath default wrapper and return
    JsonPathDefaultWrapper transformField {
      case ("jsonpaths", JArray(_)) => ("jsonpaths", JArray(fieldList))
    }
  }

  /**
   * Transforms a list of strings into a list of JStrings
   * which can be nested into a JArray JValue.
   *
   * @param strList The list of strings to be converted to
   *        JStrings
   * @param strAdd The string to be appended to the start
   *        of each string
   * @return the List of converted Strings
   */
  private def transformStringList(strList: List[String], strAdd: String): List[JValue] =
    for {
      string <- strList
    } yield {
      JString(strAdd + string)
    }
}





















