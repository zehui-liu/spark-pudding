/*
 * Copyright 2024-2026 the spark-pudding authors
 *
 * Licensed under the Apache License, Version 2.0
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

import scala.collection.JavaConverters._
import scala.collection.immutable._

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import org.pudding.core.configure.ExecutionMode

/**
 * Assist in parsing configuration, using interfaces to enable SPI implementation classes to directly use.
 */
trait EasyHandle {

  /** Get a required parameter, failing when the key is missing or null. */
  def getMapValueThrow[T](cf: Map[String, Any], k: String): T = {
    cf.get(k).filter(_ != null) match {
      case Some(v) =>
        v.asInstanceOf[T]
      case None =>
        throw new PuddingException(s"can not find param: $k, $k must be exist!")
    }
  }

  /**
   * Get an optional parameter, treating a missing or null value as absent.
   *
   * @param cf configuration map
   * @param k  parameter key
   * @return the value, or `None` when the key is absent or null
   */
  def getMapValue[T](cf: Map[String, Any], k: String): Option[T] = {
    cf.get(k).filter(_ != null).map(_.asInstanceOf[T])
  }

  /** Reject a configuration map that contains a null value. */
  def checkMapValueNull(cf: Map[String, Any]): Unit = {
    val nullValueMap = cf.filter { case (_, v) => v == null }
    if (nullValueMap.nonEmpty) {
      throw new PuddingException(s"${nullValueMap.keySet.mkString(",")} value must exist !")
    }
  }

  /**
   * An explicit execution mode is authoritative over connector-level streaming flags. None is
   * used only when a connector is called directly outside a job; BuildJob always supplies a mode.
   */
  def effectiveStreaming(
      configured: Boolean,
      executionMode: Option[ExecutionMode]): Boolean = {
    executionMode.map(_ == ExecutionMode.Streaming).getOrElse(configured)
  }

  /**
   * Normalize a Spark reader/writer option map. YAML and JSON may produce Boolean or numeric
   * values, while Spark options are strings; keys and values must not be null. HOCON treats
   * unquoted dots as nesting, so nested maps are flattened back to dotted option names.
   */
  def getStringOptions(cf: Map[String, Any], key: String): Map[String, String] = {
    def join(prefix: String, name: String): String = {
      if (prefix.isEmpty) name else s"$prefix.$name"
    }

    def flatten(prefix: String, value: Any): Seq[(String, String)] = value match {
      case options: Map[_, _] =>
        flattenMap(prefix, options)
      case options: java.util.Map[_, _] =>
        flattenMap(prefix, options.asScala)
      case null =>
        throw new PuddingException(s"$key can not contain a null key or value!")
      case other =>
        Seq(prefix -> other.toString)
    }

    def flattenMap(
        prefix: String,
        options: scala.collection.Map[_, _]): Seq[(String, String)] = {
      if (options.isEmpty) {
        if (prefix.isEmpty) {
          Seq.empty
        } else {
          throw new PuddingException(s"$key can not contain an empty map!")
        }
      } else {
        options.toSeq.flatMap { case (optionKey, value) =>
          if (optionKey == null || value == null) {
            throw new PuddingException(s"$key can not contain a null key or value!")
          }
          flatten(join(prefix, optionKey.toString), value)
        }.toVector
      }
    }

    cf.get(key).filter(_ != null) match {
      case Some(options: Map[_, _]) =>
        flattenMap("", options).toMap
      case Some(options: java.util.Map[_, _]) =>
        flattenMap("", options.asScala).toMap
      case Some(_) => throw new PuddingException(s"$key must be a map!")
      case None => Map.empty[String, String]
    }
  }

  /**
   * Build a Spark schema from column name to SQL type entries. Complex types use Spark DDL,
   * for example `struct<name:string,age:int>`.
   *
   * @param schemas column name to Spark SQL type
   * @return the corresponding struct type
   */
  def createStructType(schemas: Map[String, String]): StructType =
    StructType(schemas.map { case (k, v) => StructField(k, DataType.fromDDL(v)) }.toSeq)

  /**
   * Project and cast columns according to a prepared schema map.
   *
   * @param schemas column name to Spark SQL type
   * @return the projected columns
   */
  def getColumns(schemas: Map[String, String]): Seq[Column] =
    schemas.map { case (k, v) => col(k).cast(v) }.toList

  /**
   * Convert a string value to the Java/Scala type named by the schema type.
   *
   * @param str the source value
   * @param v   the Spark SQL type
   * @return the converted value
   */
  def buildRow(str: String, v: String): Any = {
    if (str == null) {
      throw new PuddingException("row value can not be null!")
    }
    if (v == null) {
      throw new PuddingException("row type can not be null!")
    }
    try {
      v.trim.toLowerCase(Locale.ROOT) match {
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
        case _ => throw new PuddingException(s"Unsupported type: $v")
      }
    } catch {
      case error: IllegalArgumentException =>
        throw new PuddingException(s"value '$str' is not a valid $v", error)
    }
  }

}
