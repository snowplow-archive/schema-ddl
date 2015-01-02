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

// This project
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
  // TODO: Add more encoding options
  private object TableEncodeOptions {
    val Array    = "varchar(5000)  encode runlength"
    val DateTime = "timestamp      encode raw"
    val Generic  = "varchar(255)   encode raw"
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
    "SORTKEY (root_tstamp);"
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
      case Some(elems) => processData(elems).success
      case None        => s"Error: Function - `getRedshiftDdlFile` - Should never happen; check the key used to store the fields in SchemaFlattener".fail
    }

    // Process the self describing fields of the flattened schema...
    val selfDesc = flatSchema.get("self_elems") match {
      case Some(elems) => processSelfDesc(elems)
      case None        => s"Error: Function - `getRedshiftDdlFile` - Should never happen; check the key used to store the self describing elements in SchemaFlattener".fail
    }

    // Process the new lists...
    (selfDesc, data) match {
      case (Success(a), Success(b)) => (RedshiftDdlDefaultHeader ++ a ++ RedshiftDdlDefaultTables ++ b ++ RedshiftDdlDefaultEnd).success
      case (Failure(a),        _)   => a.fail // If data processing failed
      case (_,        Failure(b))   => b.fail // If self-desc processing failed
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
  private[generators] def processSelfDesc(flatSelfElems: ListMap[String, Map[String, String]]): Validation[String, List[String]] =
    flatSelfElems.get("self") match {
      case Some(elems) => {

        val vendor  = elems.get("vendor")
        val name    = elems.get("name")
        val version = elems.get("version")

        (vendor, name, version) match {
          case (Some(a), Some(b), Some(c)) => {

            // Make a schema name from the variables
            val schemaName = SU.getSchemaName(a, b, c)

            // If the schema name -> redshift name is a success...
            SU.schemaToRedshift(schemaName) match {
              case Success(tableName) => {
                List(
                  ("-- Compatibility: "+schemaName+""),
                  (""),
                  ("CREATE TABLE atomic."+tableName+" (")
                ).success
              }
              case Failure(str) => str.fail
            }
          }
          case (_, _, _) => s"Error: Function - `processSelfDesc` - Information missing from self-describing elements".fail
        }
      }
      case None => s"Error: Function - `processSelfDesc` - We are missing the self-describing elements".fail
    }

  /**
   * Processes the Map of Data elements pulled from
   * the JsonSchema and figures out how each one should
   * be processed based on attributes associated with it.
   *
   * eg. ts -> Map("type" -> "string", "format" -> "date-time")
   *     ts timestamp encode raw
   *
   * @param flatDataElems The Map of Schema keys -> attributes
   *        which need to be processed
   * @return an itterable list of strings which contain each 
   *         key and an encoding/storage rule for the data that
   *         will come with it
   */
   // TODO: Add ability to append information after the suffix for 
   //       what type of encoding it is or why we used that type of encoding
  private[generators] def processData(flatDataElems: ListMap[String, Map[String, String]]): Iterable[String] = {

    // Get the size of the longest key in the map
    val keys = flatDataElems.keys.toList
    val maxLen = SU.getLongest(keys).size

    // Process each key pair in the map
    for {
     (k, v) <- flatDataElems
    } yield {
      val prefix = SU.getWhiteSpace(4) // Essentially a tab
      val key    = "\"" + k + "\"" // Wrap the key to prevent odd chars from breaking the table
      val space  = SU.getWhiteSpace(maxLen - k.size + 1) // Get the space needed to neatly format the file
      val encode = getEncodeType(v) // Using the value figure out how the keys vaues should get encoded
      val suffix = if (SU.isLast(keys, k)) "" else "," // If this is the last key we cannot have a comma as a suffix

      // Return compiled string...
      prefix + key + space + encode + suffix
    }
  }

  /**
   * Returns the encode type based on what attrbutes
   * are contained within the providied map.
   *
   * @param attrs The map of attributes to be analyzed
   * @return the encode type as a String
   */
   //TODO: Flesh out definitions for encode types based on attributes
  private[generators] def getEncodeType(attrs: Map[String, String]): String =
    attrs match {
      case map => {
        (map.get("type"), map.get("format")) match {
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
    }
}
