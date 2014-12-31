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
   * Prepends a Prefix to a String
   *
   * @param strings The list of strings to be prepended
   * @param prefix The string to be prepended to the start
   *        of each string
   * @return the List of converted Strings
   */
  def prependStrings(strings: List[String], prefix: String): List[String] =
    for {
      string <- strings
    } yield {
      prefix + string
    }

  /**
   * Appends a Suffix to a String
   *
   * @param strings The list of strings to be appended
   * @param suffix The string to be appended to the end
   *        of each string
   * @return the List of converted Strings
   */
  def appendStrings(strings: List[String], suffix: String): List[String] =
    for {
      string <- strings
    } yield {
      string + suffix
    }

  /**
   * Calculates whether or not the string passed is the
   * last string of a list.
   *
   * @param list The list of strings that need to 
   *        be tested
   * @param test The test string which needs to 
   *        be assessed
   * @return a boolean stating whether or not it is
   *         the last string 
   */
  def isLast(list: List[String], test: String): Boolean =
    if (list.last == test) true else false

  /**
   * Builds a Schema name from variables.
   * 
   * @param vendor The vendor of the schema
   * @param name The name of the event the 
   *        schema describes
   * @param version The version of the schema
   * @return a valid schema name
   */
  def getSchemaName(vendor: String, name: String, version: String): String =
    "iglu:"+vendor+"/"+name+"/jsonschema/"+version

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
