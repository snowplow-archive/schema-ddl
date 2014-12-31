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
 * Generates a Redshift DDL File from a Flattened
 * JsonSchema
 */
object RedshiftDdlGenerator {

  // Encoding Options for the different fields
  private object TableEncodeOptions {
    val Array    = "varchar(5000)  encode runlength, -- Holds a JSON array"
    val DateTime = "timestamp      encode raw,"
    val Generic  = "varchar(255)   encode raw,"
  }

  // Settings for the header of the file
  private object HeaderTextSettings {
    val Year   = "2014-2015"
    val Author = "Joshua Beemster"
  }

  // Header Section for a Redshift DDL File
  private val RedshiftDdlDefaultHeader = List(
    "-- Copyright (c) "+HeaderTextSettings.Year+" Snowplow Analytics Ltd. All rights reserved.",
    "--", 
    "-- This program is licensed to you under the Apache License Version 2.0,",
    "-- and you may not use this file except in compliance with the Apache License Version 2.0.",
    "-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.",
    "--",
    "-- Unless required by applicable law or agreed to in writing,",
    "-- software distributed under the Apache License Version 2.0 is distributed on an",
    "-- \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
    "-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.",
    "--",
    "-- Authors:     "+HeaderTextSettings.Author+"",
    "-- Copyright:   Copyright (c) "+HeaderTextSettings.Year+" Snowplow Analytics Ltd",
    "-- License:     Apache License Version 2.0",
    "--"
  )
  
  // Default Tables for the RedShift DDL File
  private val RedshiftDdlDefaultTables = List(
    "    -- Schema of this type",
    "    schema_vendor  varchar(128)   encode runlength not null,",
    "    schema_name    varchar(128)   encode runlength not null,",
    "    schema_format  varchar(128)   encode runlength not null,",
    "    schema_version varchar(128)   encode runlength not null,",
    "    -- Parentage of this type",
    "    root_id        char(36)       encode raw not null,",
    "    root_tstamp    timestamp      encode raw not null,",
    "    ref_root       varchar(255)   encode runlength not null,",
    "    ref_tree       varchar(1500)  encode runlength not null,",
    "    ref_parent     varchar(255)   encode runlength not null,",
    "    -- Properties of this type"
  )

  // Footer Section for a Redshift DDL File
  private val RedshiftDdlDefaultEnd = List(
    ")",
    "DISTSTYLE KEY",
    "-- Optimized join to atomic.events",
    "DISTKEY (root_id)",
    "SORTKEY (root_tstamp);",
    ""
  )

  /**
   * Generates a Redshift DDL File from a flattened 
   * JsonSchema
   *
   * @param flatSchema The Map produced by the Schema
   *        flattening process
   * @return a list of strings which contains every line
   *         in a Redshift DDL file or a Failure String
   */
  def getRedshiftDdlFile(flatSchema: Map[String, ListMap[String, Map[String,String]]]): Validation[String, List[String]] = {

    // Process the data fields of the flattened schema...
    val data = flatSchema.get("flat_elems") match {
      case Some(elems) => {
        formatPropertiesList(processData(elems)).success
      }
      case None => s"Error: Function - `getRedshiftDdlFile` - Should never happen; check the key used to store the fields in SchemaFlattener".fail
    }

    // Process the self describing elements of the flattened schema...
    val selfDesc = flatSchema.get("self_elems") match {
      case Some(elems) => {
        processSelfDesc(elems)
      }
      case None => s"Error: Function - `getRedshiftDdlFile` - Should never happen; check the key used to store the self describing elements in SchemaFlattener".fail
    }

    // Process the new lists...
    (selfDesc, data) match {
      case (Success(a), Success(b)) => (RedshiftDdlDefaultHeader ++ a ++ RedshiftDdlDefaultTables ++ b ++ RedshiftDdlDefaultEnd).success
      case (Failure(a), Failure(b)) => (a + "," + b).fail
      case (Failure(str), _) => str.fail
      case (_, Failure(str)) => str.fail
    }
  }

  /**
   * Creates the Redshift Table Name and compatibility strings
   * for the Redshift DDL File.
   *
   * @param flatSelfElems A Map of Self Describing elements
   *        pulled from the JsonSchema
   * @return a validated list of strings that contain the 
   *         relevant information
   */
  private def processSelfDesc(flatSelfElems: ListMap[String, Map[String, String]]): Validation[String, List[String]] = {
    flatSelfElems.get("self") match {
      case Some(values) => {
        val vendor = values.get("vendor")
        val name = values.get("name")
        val version = values.get("version")

        (vendor, name, version) match {
          case (Some(vendor), Some(name), Some(version)) => {

            // Create variables needed from self-desc information
            val compat = "iglu:"+vendor+"/"+name+"/jsonschema/"+version
            SU.schemaToRedshift(compat) match {
              case Success(str) => {
                List(
                  ("-- Compatibility: "+compat+""),
                  (""),
                  ("CREATE TABLE atomic."+str+" (")
                ).success
              }
              case Failure(str) => str.fail
            }
          }
          case (_, _, _) => s"Error: Function - `processSelfDesc` - Should never happen; Information missing cannot process".fail
        }
      }
      case None => s"Error: Function - `processSelfDesc` - Should never happen; Information missing cannot process".fail
    } 
  }

  /**
   * Processes the Map of Data elements pulled from
   * the JsonSchema and figures out how each one should
   * be processed based on attributes associated with it.
   *
   * eg. ts -> Map("type" -> "string", "format" -> "date-time")
   *     ts timestamp encode raw
   *
   * TODO: Add further analysis here to determine more abstract fields
   *
   * @param flatDataElems The Map of Schema keys -> attributes
   *        which need to be processed
   * @return an itterable list of strings which contain each 
   *         key and an encoding/storage rule for the data that
   *         will come with it
   */
  private def processData(flatDataElems: ListMap[String, Map[String, String]]): Iterable[String] =
    for {
     (key, value) <- flatDataElems
    } yield {
      ("\"" + key + "\"" + " " + (value match {
        case attr => {
          (attr.get("type"), attr.get("format")) match {
            case (Some(types), Some(format)) => {
              if (types.contains("string") && format == "date-time") {
                TableEncodeOptions.DateTime
              }
              else {
                TableEncodeOptions.Generic
              }
            }
            case (Some(types), _) => {
              if (types.contains("array")) {
                TableEncodeOptions.Array
              }
              else {
                TableEncodeOptions.Generic
              }
            }
            case (_, _) => TableEncodeOptions.Generic
          }
        }
      }))
    }

  /**
   * Formats the new fields to have the correct 
   * amount of white-space per line.
   *
   * eg. ts timestamp encode raw,
   *     data.ts timestamp encode raw,
   * ->
   *     ts      timestamp encode raw,
   *     data.ts timestamp encode raw,
   *
   * @param data The list of fields which need
   *        to be formatted
   * @return a list of strings which has the correct
   *         amount of white-space
   */
  private def formatPropertiesList(data: Iterable[String]): Iterable[String] = {

    // Get the longest string in the list to use as a measure
    val maxLen = SU.getLongest(for {
        field <- data
      } yield {
        field.split(" ")(0)
      }).size

    val tab = SU.getWhiteSpace(4)

    // Process each string and add white-space according to calculated maxLen
    for {
      field <- data
    } yield {

      // Get the length of the key...
      val keyLen = field.split(" ")(0).size
      if (keyLen < maxLen) {
        tab.concat(field.replaceFirst(" ", SU.getWhiteSpace(maxLen - (keyLen - 1))))
      }
      else {
        tab.concat(field)
      }
    }
  }
}
