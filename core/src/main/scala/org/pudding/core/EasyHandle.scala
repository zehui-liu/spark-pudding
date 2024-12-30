/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pudding.core

import java.sql.{Date, Timestamp}
import java.util.Locale

import scala.collection.immutable._

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Assist in parsing configuration, using interfaces to enable SPI implementation classes to directly use.
 */
trait EasyHandle {

  def getMapValueThrow[T](cf: Map[String, Any], k: String): T = {
    cf.get(k) match {
      case Some(v) =>
          v.asInstanceOf[T]
      case None =>
        throw new PuddingException(s"can not find param: $k, $k must be exist!")
    }
  }

  def checkMapValueNull(cf: Map[String, Any]): Unit = {
    val nullValueMap = cf.filter { case (k, v) => v == null }
    if (nullValueMap.nonEmpty) {
      throw new PuddingException(s"${nullValueMap.keySet.mkString(",")} value must exist !")
    }
  }

  /**
   * get boolean param from optional param
   *
   * @param v Option[String]
   * @return Boolean
   */
  def checkBoolean(v: Option[String]): Boolean = {
    v match {
      case Some(bool) =>
        bool match {
          case "true" => true
          case "false" => false
          case _ => throw new PuddingException("unsupported param, only set true or false")
        }
      case None => false
    }
  }

  /**
   * get StructType from schema params
   * complex type eg: "struct<name:string,age:int>"
   *
   * @param schemas Map[String, String]
   * @return StructType
   */
  def createStructType(schemas: Map[String, String]): StructType =
    StructType(schemas.map { case (k, v) => StructField(k, DataType.fromDDL(v)) }.toSeq)

  /**
   * get column seq form schemas
   *
   * @param schemas Map[String, String]
   * @return Seq[Column]
   */
  def getColumns(schemas: Map[String, String]): Seq[Column] = schemas.map { case (k, v) => col(k).cast(v) }.toList

  /**
   * build row from string and schema type
   *
   * @param str String
   * @param v String
   * @return Any
   */
  def buildRow(str: String, v: String): Any = {
    v.toLowerCase(Locale.ROOT) match {
      case "boolean" => str.toBoolean
      case "byte" => str.toByte
      case "short" => str.toShort
      case "integer" | "int" => str.toInt
      case "long" => str.toLong
      case "float" => str.toFloat
      case "double" => str.toDouble
      case "string" => str
      case "date" => Date.valueOf(str)
      case "timestamp" => Timestamp.valueOf(str)
      case _ => throw new UnsupportedOperationException(s"Unsupported type: $v")
    }
  }

  def retry[T](n: Int)(fn: => T): T = {
    try {
      fn
    } catch {
      case e if n > 1 => retry(n - 1)(fn)
    }
  }

}
