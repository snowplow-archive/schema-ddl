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
package com.snowplowanalytics.schemaddl
package generators
package redshift

// This project
import utils.{ StringUtils => SU }

/**
 * Module containing functions for data type suggestions
 */
object TypeSuggestions {
  import Ddl._

  /**
   * Type alias for function suggesting an encode type based on map of
   * JSON Schema properties
   */
  type DataTypeSuggestion = (Map[String, String], String) => Option[DataType]

  // Suggest VARCHAR(4096) for all product types. Should be first
  val productSuggestion: DataTypeSuggestion = (properties, columnName) =>
    properties.get("type") match {
      case (Some(types)) if (types.split(",").toSet - "null").size > 1 =>
        Some(CustomDataTypes.ProductType(List(s"Product type $types encountered in $columnName")))
      case _ => None
    }

  val timestampSuggestion: DataTypeSuggestion = (properties, columnName) =>
    (properties.get("type"), properties.get("format")) match {
      case (Some(types), Some("date-time")) if types.contains("string") =>
        Some(DataTypes.RedshiftTimestamp)
      case _ => None
    }

  val arraySuggestion: DataTypeSuggestion = (properties, columnName) =>
    properties.get("type") match {
      case Some(types) if types.contains("array") =>
        Some(DataTypes.RedshiftVarchar(5000))
      case _ => None
    }

  val numberSuggestion: DataTypeSuggestion = (properties, columnName) =>
    (properties.get("type"), properties.get("multiplyOf"), properties.get("maximum")) match {
      case (Some(types), Some(multiplyOf), Some(maximum)) if (types.contains("number") && multiplyOf == "0.01") =>
        Some(DataTypes.RedshiftDecimal(Some(36), Some(2)))
      case (Some(types), Some(multiplyOf), _) if (types.contains("number") && multiplyOf == "0.01") =>
        Some(DataTypes.RedshiftDecimal(Some(36), Some(2)))
      case (Some(types), _, _) if types.contains("number") =>
        Some(DataTypes.RedshiftDouble)
      case _ => None
    }

  val integerSuggestion: DataTypeSuggestion = (properties, columnName) => {
    (properties.get("type"), properties.get("maximum"), properties.get("enum")) match {
      case (Some("integer"), Some(maximum), _) =>
        val max = maximum.toLong
        getIntSize(max)
      // Contains only enum
      case (types, _, Some(enum)) if ((!types.isDefined || types.get == "integer") && SU.isIntegerList(enum)) =>
        val max = enum.split(",").toList.map(_.toLong).max
        getIntSize(max)
      case (Some("integer"), _, _) =>
        Some(DataTypes.RedshiftBigInt)
      case _ => None
    }
  }

  val charSuggestion: DataTypeSuggestion = (properties, columnName) => {
    (properties.get("type"), properties.get("minLength"), properties.get("maxLength")) match {
      case (Some("string"), Some(SU.IntegerAsString(minLength)), Some(SU.IntegerAsString(maxLength))) if minLength == maxLength =>
        Some(DataTypes.RedshiftChar(maxLength))
      case _ => None
    }
  }

  val booleanSuggestion: DataTypeSuggestion = (properties, columnName) => {
    properties.get("type") match {
      case Some("boolean") => Some(DataTypes.RedshiftBoolean)
      case _ => None
    }
  }

  val uuidSuggestion: DataTypeSuggestion = (properties, columnName) => {
    (properties.get("type"), properties.get("format")) match {
      case (Some(types), Some("uuid")) if types.contains("string") =>
        Some(DataTypes.RedshiftChar(36))
      case _ => None
    }
  }

  val varcharSuggestion: DataTypeSuggestion = (properties, columnName) => {
    (properties.get("type"), properties.get("maxLength"), properties.get("enum"), properties.get("format")) match {
      case (Some(types),     _,                           _,                      Some("ipv6")) if types.contains("string") =>
        Some(DataTypes.RedshiftVarchar(39))
      case (Some(types),     _,                           _,                      Some("ipv4")) if types.contains("string") =>
        Some(DataTypes.RedshiftVarchar(15))
      case (Some(types),     Some(SU.IntegerAsString(maxLength)), _,              _) if types.contains("string") =>
        Some(DataTypes.RedshiftVarchar(maxLength))
      case (_,              _,                            Some(enum),             _) => {
        val enumItems = enum.split(",")
        val maxLength = enumItems.toList.reduceLeft((a, b) => if (a.length > b.length) a else b).length
        if (enumItems.length == 1) {
          Some(DataTypes.RedshiftChar(maxLength))
        } else {
          Some(DataTypes.RedshiftVarchar(maxLength))
        }
      }
      case _ => None
    }
  }

  /**
   * Helper function to get size of Integer
   *
   * @param max upper bound
   * @return Long representing biggest possible value or None if it's not Int
   */
  private def getIntSize(max: Long): Option[DataType] =
    if (max <= Short.MaxValue) Some(DataTypes.RedshiftSmallInt)
    else if (max <= Int.MaxValue) Some(DataTypes.RedshiftInteger)
    else if (max <= Long.MaxValue) Some(DataTypes.RedshiftBigInt)
    else None
}
