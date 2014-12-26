/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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

/**
 * Flattens a JsonSchema into Strings representing the path
 * to a field. This will be linked with a Map of attributes
 * that the field possesses.
 */
object SchemaFlattener {

  /**
   * Flattens a JsonSchema into a useable Map of Strings and Attributes.
   * - Will extract the self-desccribing elements of the JsonSchema out --- TODO
   * - Will then grab the first properties list and begin the recursive
   *   function
   * - Will then return the validated map of string paths and attributes
   *
   * @param jSchema The JsonSchema which we will process
   * @return a validated map of keys and attributes or a 
   *         failure string
   */
  def flattenJsonSchema(jSchema: JValue): Validation[String, Map[String, Map[String,String]]] =
    jSchema match {
      case JObject(list) => {
        attrElemInfo(list) match {
          case Success(elemProps) => {
            elemProps.get("object") match {
              case Some(props) => processProperties(props)
              case _           => s"Error: Function - 'flattenJsonSchema' - First level of JsonSchema does not contain any properties".fail
            }
          }
          case Failure(str) => str.fail
        }
      }
      case _ => s"Error: Function - 'flattenJsonSchema' - Invalid Schema passed to flattener".fail
    }

  /**
   * Returns information about a single list element:
   * - What 'core' type the element is (object,array,other)
   * - If it is an 'object' returns the properties list for 
   *   processing
   *
   * TODO: Flesh out defining cases for different types of elements
   *
   * @param maybeAttrList The list of attributes which need
   *        to be analysed to determine what to do with them
   * @return a map which contains a string illustrating what
   *         what needs to be done with the element
   */
  private def attrElemInfo(maybeAttrList: List[(String, JValue)]): Validation[String, Map[String, List[(String, JValue)]]] =
    maybeAttrList.toMap.get("type") match {
      case Some(types) => {
        processType(types) match {
          case (Success(types)) => {
            if (types.contains("object")) {
              maybeAttrList.toMap.get("properties") match {
                case Some(JObject(props)) => Map("object" -> props).success
                case _                    => s"Error: Function - 'attrElemInfo' - Object does not have any properties".fail
              }
            }
            else if (types.contains("array")) {
              Map("array" -> List()).success
            }
            else {
              Map("null" -> List()).success
            }
          }
          case Failure(str) => str.fail
        }
      }
      case _ => s"Error: Function - 'attrElemInfo' - List does not contain 'Type' field".fail
    }

  /**
   * Processes the properties of an object.  This can be for 
   * 'n' amount of attributes. The list of properties can also 
   * contain other objects with nested properties.
   *
   * @param propertyList The list of properties of a 
   *        JsonSchema 'object'
   * @param accum The accumulated map of key's and 
   *        attributes
   * @param accumKey The key that is added to for each 
   *        nested 'object' level
   * @return a validated map of keys and attributes or a 
   *         failure string
   */
  private def processProperties(propertyList: List[(String, JValue)], accum: Map[String, Map[String, String]] = Map(), accumKey: String = ""): 
    Validation[String, Map[String, Map[String, String]]] = {

    propertyList match {
      case x :: xs => {
        val res = x match {
          case (key, JObject(list)) => {
            attrElemInfo(list) match {
              case Success(elemProps) => {
                if (elemProps.contains("object")) {
                  processProperties(elemProps.get("object").get, Map(), accumKey + key + ".")
                }
                else if (elemProps.contains("array")) {
                  Map(key -> Map("type" -> "array")).success
                }
                else {
                  processAttributes(list) match {
                    case Success(attr) => Map(key -> attr).success
                    case Failure(str)  => str.fail
                  }
                }
              }
              case Failure(str) => str.fail
            }
          }
          case _ => s"Error: Function - 'processProperties' - Invalid List Tuple2 Encountered".fail
        }

        res match {
          case Success(goodRes) => processProperties(xs, (accum ++ goodRes), accumKey)
          case Failure(badRes) => badRes.fail
        }
        
      }
      case Nil => accum.success
    }
  }

  /**
   * Processes the attributes of an objects list element.
   * This function is aimed at 'final' elements: i.e. cannot
   * be of type 'object' or 'array'.
   *
   * TODO: Add more cases for different internal attributes to pull (type, format, maxLength etc...)
   *
   * @param attributes The list of attributes that an element has
   * @param accum The acuumulated Map of String -> String attributes
   * @return a validated map of attributes or a failure string
   */
  private def processAttributes(attributes: List[(String, JValue)], accum: Map[String, String] = Map()): Validation[String, Map[String, String]] =
    attributes match {
      case x :: xs => {
        x match {
          case (key, JObject(value))       => s"Error: Function - 'processAttributes' - Invalid JValue found".fail
          case ("type", typeOpt)           => {
            processType(typeOpt) match {
              case Success(types) => processAttributes(xs, (accum ++ Map("type" -> types)))
              case Failure(str)   => str.fail
            }
          }
          case ("format", JString(format)) => processAttributes(xs, (accum ++ Map("format" -> format)))
          case _                           => processAttributes(xs, (accum ++ Map()))
        }
      }
      case Nil => accum.success
    }

  /**
   * Process the type field of an element; can be either a String or
   * an array of Strings.
   *
   * @param typeOpt The JValue from the "type" field of an element
   * @return A validated String containing all of the Type Strings
   *         combined into one or a failure string
   */
  private def processType(typeOpt: JValue): Validation[String, String] =
    typeOpt match {
      case (JString(typeOpt)) => typeOpt.success
      case (JArray(list))     => processTypeList(list)
      case _                  => s"Error: Function - 'processTypes' - Type Field is invalid".fail
    }

  /**
   * Takes a list of values and converts them into a single string
   * deliminated by a comma.
   *
   * TODO: Add validation into Nil Case for odd groupings of
   *       types eg. (object,string)
   *
   * Example: List((JString("string")),(JString("null")))
   *          "string,null"
   *
   * @param list The list of values to be combined
   * @param accum The accumulated String from the list
   * @param delim The deliminator to be used between
   *        strings
   * @return A validated String containing all entities of the
   *         list that was passed or a failure string
   */ 
  private def processTypeList(list: List[JValue], accum: String = "", delim: String = ","): Validation[String, String] =
    list match {
      case x :: xs => {
        x match {
          case JString(typeOpt) => processTypeList(xs, (accum + delim + typeOpt))
          case _                => s"Error: Function - 'processTypeList' - Invalid JValue in list".fail
        }
      }
      case Nil => accum.drop(1).success
    }

  /*
  /**
    * Will remove any extraneous fields from the schema
     * which will not be needed in the flattening process.
    */
  def stripJsonSchema(jSchema: JValue): Any = {

  }

  /**
   * Will organise the paths returned from the flattening
   * process into the most logical order.
   * 1. The amount of jumps in the path:
   *    - Lowest to highest amount
   * 2. Alphabetical sorting per tier of jumps
   */
  def orderFlattenedPaths(paths: List[String]): Any = {

  }
  */
}

