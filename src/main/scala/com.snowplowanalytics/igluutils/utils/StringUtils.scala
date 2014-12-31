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

// Scalaz
import scalaz._
import Scalaz._

// Scala
import scala.util.matching.Regex

/**
 * Utilities for manipulating Strings
 */
object StringUtils {

  // Regex used to generate the table name from a schema
  private val schemaPattern = """.+:([a-zA-Z0-9_\.]+)/([a-zA-Z0-9_]+)/[^/]+/(.*)""".r

  /**
   * Will return the longest string in an Iterable
   * string list.
   *
   * @param list The list of strings to analyze
   * @return the longest string in the list
   */
  def getLongest(list: Iterable[String]): String =
    list.reduceLeft((x,y) => if (x.length > y.length) x else y)

  /**
   * Returns a string with N amount of white-space
   *
   * @param count The count of white-space that needs
   *        to be returned
   * @return a string with the amount of white-space
   *         needed
   */
  def getWhiteSpace(count: Int): String =
    ("").padTo(count, ' ')

  /**
   * Appends a Prefix to a String
   *
   * @param strings The list of strings to be converted to
   *        JStrings
   * @param prefix The string to be appended to the start
   *        of each string
   * @return the List of converted Strings
   */
  def appendToStrings(strings: List[String], prefix: String): List[String] =
    for {
      string <- strings
    } yield {
      prefix + string
    }

  /**
   * Create a Redshift Table name from a schema
   *
   * "iglu:com.acme/PascalCase/jsonschema/13-0-0" -> "com_acme_pascal_case_13"
   *
   * @param schema The Schema name
   * @return the Redshift Table name
   */
  def schemaToRedshift(schema: String): Validation[String, String] =
    schema match {
      case schemaPattern(organization, name, schemaVer) => {

        // Split the vendor's reversed domain name using underscores rather than dots
        val snakeCaseOrganization = organization.replaceAll("""\.""", "_").toLowerCase

        // Change the name from PascalCase to snake_case if necessary
        val snakeCaseName = name.replaceAll("([^A-Z_])([A-Z])", "$1_$2").toLowerCase

        // Extract the schemaver version's model
        val model = schemaVer.split("-")(0)

        s"${snakeCaseOrganization}_${snakeCaseName}_${model}".success
      }
      case _ => "Error: Function - `getTableName` - Schema %s does not conform to regular expression %s".format(schema, schemaPattern.toString).fail
    }
}
