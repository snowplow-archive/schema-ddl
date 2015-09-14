/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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

// Scala
import scala.io.Source

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

// specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

class SchemaFlattenerSpec extends Specification with ValidationMatchers { def is = s2"""
  Check SchemaFlattener
    split product types                                 $checkSplitProductTypes
    stringify JSON array                                $checkStringifyArray
    fail to stringify object in JSON array              $failOnObjectInStringifyArray
    get required properties                             $checkGetRequiredProperties
    process properties                                  $checkProcessProperties
    process properties with nested nullable             $checkProcessPropertiesWithNestedNullable
  """

  def checkSplitProductTypes = {
    val property = Map("test_key" -> Map("type" -> "string,null,integer", "maxLength" -> "32", "maximum" -> "1024"))
    val result = Map(
      "test_key_string" -> Map("type" -> "string,null",  "maxLength" -> "32", "maximum" -> "1024"),
      "test_key_integer" -> Map("type" -> "integer,null",  "maxLength" -> "32", "maximum" -> "1024")
    )

    SchemaFlattener.splitProductTypes(property) must beEqualTo(result)
  }

  def checkStringifyArray = {
    val jValues = List(JInt(3), JNull, JString("str"), JDecimal(3.3), JDouble(3.13), JString("another_str"))
    SchemaFlattener.stringifyArray(jValues) must beSuccessful("3,null,str,3.3,3.13,another_str")
  }

  def failOnObjectInStringifyArray = {
    val jValues = List(JInt(3), JNull, JString("str"), JDecimal(3.3), JDouble(3.13), JString("another_str"), JObject(List(("keyOfFatum", JInt(42)))))
    SchemaFlattener.stringifyArray(jValues) must beFailing
  }

  def checkGetRequiredProperties = {
    implicit val formats = DefaultFormats
    val json: JObject = parse(Source.fromURL(getClass.getResource("/schema_with_required_properties.json")).mkString).asInstanceOf[JObject]
    val map = json.extract[Map[String, JValue]]

    SchemaFlattener.getRequiredProperties(map) must beSuccessful(List("anotherRequired", "requiredKey"))
                                                         // getRequiredProperties reverses values ^^^
  }

  def checkProcessProperties = {
    val json: JObject = parse(Source.fromURL(getClass.getResource("/schema_with_required_properties.json")).mkString).asInstanceOf[JObject]
    val root = List(("root", json))
    val resultMap = Map(
      "root.oneKey" -> Map("type" -> "string"),
      "root.requiredKey" -> Map("type" -> "integer"),
      "root.objectKey.skippedRequired" -> Map("type" -> "string"),
      "root.anotherRequired" -> Map("type" -> "string")
    )

    SchemaFlattener.processProperties(root) must beSuccessful(SchemaFlattener.SubSchema(resultMap, Set()))
  }

  def checkProcessPropertiesWithNestedNullable = {
    val json: JObject = parse(Source.fromURL(getClass.getResource("/schema_with_required_properties.json")).mkString).asInstanceOf[JObject]
    val root = List(("root", json))
    val resultMap = Map(
      "root.oneKey" -> Map("type" -> "string"),
      "root.requiredKey" -> Map("type" -> "integer"),
      "root.objectKey.skippedRequired" -> Map("type" -> "string"),
      "root.anotherRequired" -> Map("type" -> "string")
    )

    SchemaFlattener.processProperties(root, requiredAccum = Set("root")) must beSuccessful(SchemaFlattener.SubSchema(resultMap, Set("root.requiredKey", "root.anotherRequired")))
  }
}
