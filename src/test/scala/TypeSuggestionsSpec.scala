/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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

// specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

// This library
import redshift.RedshiftDdlGenerator
import redshift.Ddl.DataTypes._


class TypeSuggestionsSpec extends Specification with ValidationMatchers { def is = s2"""
  Check type suggestions
    suggest decimal for multipleOf == 0.01  $e1
  """

  def e1 = {
    val props = Map("type" -> "number", "multipleOf" -> "0.01")
    RedshiftDdlGenerator.getDataType(props, 16,"somecolumn") must beEqualTo(RedshiftDecimal(Some(36), Some(2)))
  }
}
