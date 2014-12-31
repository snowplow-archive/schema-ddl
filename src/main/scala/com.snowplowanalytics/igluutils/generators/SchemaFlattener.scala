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
import utils.{MapUtils => MU}

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
 * Flattens a JsonSchema into Strings representing the path
 * to a field. This will be linked with a Map of attributes
 * that the field possesses.
 */
object SchemaFlattener {

  // Needed for json4s default extraction formats
  implicit val formats = DefaultFormats

  /**
   * Flattens a JsonSchema into a useable Map of Strings and Attributes.
   * - Will extract the self-describing elements of the JsonSchema out
   * - Will then grab the first properties list and begin the recursive
   *   function
   * - Will then return the validated map of string paths and attributes
   *
   * @param jSchema The JsonSchema which we will process
   * @return a validated map of keys and attributes or a 
   *         failure string
   */
  def flattenJsonSchema(jSchema: JValue): Validation[String, Map[String, ListMap[String, Map[String,String]]]] =
    // Grab the self-desc elements from the Schema
    getSelfDescElems(jSchema) match {
      case Success(selfElem) => {

        // Match against the Schema and check that it is properly formed: i.e. wrapped in { ... }
        jSchema match {
          case JObject(list) => {

            // Analyze the base level of the schema
            getElemInfo(list) match {
              case Success(elemProps) => {

                // Assume that the first level will be type object with a valid list of properties
                elemProps.get("object") match {
                  case Some(props) => {

                    // If we do have a list of properties then begin the recursuve processing of the list
                    processProperties(props) match {
                      case Success(flatElem) => Map("self_elems" -> selfElem, "flat_elems" -> MU.getOrderedMap(flatElem)).success
                      case Failure(str) => str.fail
                    }
                  }
                  case _ => s"Error: Function - 'flattenJsonSchema' - JsonSchema does not begin with an 'object' & 'properties'".fail
                }
              }
              case Failure(str) => str.fail
            }
          }
          case _ => s"Error: Function - 'flattenJsonSchema' - Invalid Schema passed to flattener".fail
        }
      }
      case Failure(str) => str.fail
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

            getElemInfo(list) match {
              case Success(elemProps) => {

                if (elemProps.contains("object")) {
                  elemProps.get("object") match {
                    case Some(props) => processProperties(props, Map(), accumKey + key + ".")
                    case _           => s"Error: Function - 'processProperties' - JsonSchema 'object' does not contain any properties, 'getElemInfo' is not catching this anymore".fail
                  }
                }
                else if (elemProps.contains("array")) {
                  Map(accumKey + key -> Map("type" -> "array")).success
                }
                else {
                  processAttributes(list) match {
                    case Success(attr) => Map(accumKey + key -> attr).success
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
   * @param attributes The list of attributes that an element has
   * @param accum The acuumulated Map of String -> String attributes
   * @return a validated map of attributes or a failure string
   */
  private def processAttributes(attributes: List[(String, JValue)], accum: Map[String, String] = Map()): Validation[String, Map[String, String]] =
    attributes match {
      case x :: xs => {
        x match {
          case (key, JArray(value))  => {
            processList(value) match {
              case Success(strs) => processAttributes(xs, (accum ++ Map(key -> strs)))
              case Failure(str) => str.fail
            }
          }
          case (key, JBool(value))    => processAttributes(xs, (accum ++ Map(key -> value.toString)))
          case (key, JInt(value))     => processAttributes(xs, (accum ++ Map(key -> value.toString)))
          case (key, JDecimal(value)) => processAttributes(xs, (accum ++ Map(key -> value.toString)))
          case (key, JDouble(value))  => processAttributes(xs, (accum ++ Map(key -> value.toString)))
          case (key, JNull)           => processAttributes(xs, (accum ++ Map(key -> "null")))
          case (key, JString(value))  => processAttributes(xs, (accum ++ Map(key -> value)))
          case _                      => s"Error: Function - 'processAttributes' - Invalid JValue found".fail
        }
      }
      case Nil => accum.success
    }

  /**
   * Takes a list of values and converts them into a single string
   * deliminated by a comma.
   *
   * TODO: Check if lists of attributes are only strings?
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
  private def processList(list: List[JValue], accum: String = "", delim: String = ","): Validation[String, String] =
    list match {
      case x :: xs => {
        x match {
          case JString(str) => processList(xs, (accum + delim + str))
          case _            => s"Error: Function - 'processList' - Invalid JValue in list".fail
        }
      }
      case Nil => accum.drop(1).success
    }

  /**
   * Attempts to extract the self describing elements of the 
   * JsonSchema that we are processing.
   *
   * @param jSchema the self describing json schema that needs
   *        to be processed
   * @return a validated map containing the needed self describing
   *         elements
   */
  private def getSelfDescElems(jSchema: JValue): Validation[String, ListMap[String, Map[String, String]]] = {
    val vendor  = (jSchema \ "self" \ "vendor").extractOpt[String]
    val name    = (jSchema \ "self" \ "name").extractOpt[String]
    val version = (jSchema \ "self" \ "version").extractOpt[String]

    (vendor, name, version) match {
      case (Some(vendor), Some(name), Some(version)) => ListMap("self" -> Map("vendor" -> vendor, "name" -> name, "version" -> version)).success
      case (_, _, _) => s"Error: Function - 'getSelfDescElems' - Schema does not contain all needed self describing elements".fail
    }
  }

  /**
   * Returns information about a single list element:
   * - What 'core' type the element is (object,array,other)
   * - If it is an 'object' returns the properties list for 
   *   processing
   *
   * @param maybeAttrList The list of attributes which need
   *        to be analysed to determine what to do with them
   * @return a map which contains a string illustrating what
   *         needs to be done with the element
   */
  private def getElemInfo(maybeAttrList: List[(String, JValue)]): Validation[String, Map[String, List[(String, JValue)]]] =
    maybeAttrList.toMap.get("type") match {
      case Some(types) => {
        getElemType(types) match {
          case (Success(elemType)) => {
            elemType match {
              case "object" => {
                maybeAttrList.toMap.get("properties") match {
                  case Some(JObject(props)) => Map("object" -> props).success
                  case _ => s"Error: Function - 'getElemInfo' - JsonSchema 'object' does not have any properties".fail
                }
              }
              case "array"  => Map("array" -> List()).success
              case _        => Map("" -> List()).success // Pass back a successful empty Map for a normal entry (Should come up with something better...)
            }
          }
          case Failure(str) => str.fail
        }
      }
      case _ => s"Error: Function - 'getElemInfo' - List does not contain 'Type' field".fail
    }

  /**
   * Process the type field of an element; can be either a String or
   * an array of Strings.
   *
   * TODO: Add validation for odd groupings of
   *       types eg. (object,string)
   *
   * @param types The JValue from the "type" field of an element
   * @return A validated String which determines what type the element is
   */
  private def getElemType(types: JValue): Validation[String, String] = {
    val maybeTypes = types match {
      case JString(value) => value.success
      case JArray(list) => processList(list)
      case _ => s"Error: Function - 'getElemType' - Type List contains invalid JValue".fail
    }

    maybeTypes match {
      case Success(str) => {
        if (str.contains("object")) {
          "object".success
        }
        else if (str.contains("array")) {
          "array".success
        }
        else {
          "".success
        }
      }
      case Failure(str) => str.fail
    }
  }
}
