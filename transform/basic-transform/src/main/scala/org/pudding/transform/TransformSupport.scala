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

package org.pudding.transform

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame

import org.pudding.core.PuddingException

/**
 * Shared configuration parsing for the DataFrame transform operators. Values may come from YAML,
 * JSON or HOCON, so scalar types are normalized here and errors only name the offending key.
 */
private[transform] object TransformSupport {

  private val NumPartitionsKey = "numPartitions"

  def requiredConfig(config: Option[Map[String, Any]]): Map[String, Any] = {
    config.getOrElse(throw new PuddingException("config can not be null!"))
  }

  def single(dataFrames: Map[String, DataFrame], transform: String): DataFrame = {
    if (dataFrames == null || dataFrames.isEmpty) {
      throw new PuddingException(s"$transform dataFrames can not be empty!")
    }
    if (dataFrames.size != 1) {
      throw new PuddingException(
        s"$transform requires exactly one dependency, but got: " +
          dataFrames.keys.toSeq.sorted.mkString(", "))
    }
    dataFrames.values.head
  }

  def requireAtLeast(
      dataFrames: Map[String, DataFrame],
      count: Int,
      transform: String): Unit = {
    if (dataFrames == null || dataFrames.size < count) {
      throw new PuddingException(
        s"$transform requires at least $count dependencies, but got: " +
          Option(dataFrames).map(_.size).getOrElse(0))
    }
  }

  def requiredString(cfg: Map[String, Any], key: String): String = {
    cfg.get(key) match {
      case Some(value: String) if value.trim.nonEmpty => value.trim
      case Some(_: String) => throw new PuddingException(s"$key can not be empty!")
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => throw new PuddingException(s"can not find param: $key, $key must be exist!")
    }
  }

  def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) => Option(value.trim).filter(_.nonEmpty)
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => None
    }
  }

  /**
   * Parse a list of names. A comma-separated string is accepted for convenience; lists must
   * contain non-empty strings. Duplicate values are rejected because they usually indicate a
   * configuration mistake and produce ambiguous Spark plans.
   */
  def stringSeq(cfg: Map[String, Any], key: String): Option[Seq[String]] = {
    cfg.get(key).filter(_ != null).map { value =>
      val values = value match {
        case text: String =>
          text.split(",").map(_.trim).filter(_.nonEmpty).toSeq
        case seq: Seq[_] =>
          seq.map(item => requiredText(item, key)).toSeq
        case array: Array[_] =>
          array.map(item => requiredText(item, key)).toSeq
        case list: java.util.List[_] =>
          list.asScala.map(item => requiredText(item, key)).toSeq
        case _ =>
          throw new PuddingException(s"$key must be a string or a list of strings!")
      }

      if (values.isEmpty) {
        throw new PuddingException(s"$key can not be empty!")
      }
      val duplicates = this.duplicates(values)
      if (duplicates.nonEmpty) {
        throw new PuddingException(
          s"$key contains duplicate values: ${duplicates.mkString(", ")}")
      }
      values
    }
  }

  /**
   * Parse aggregate projections while preserving declaration order. The map form is concise and
   * the list form should be used when output column order matters:
   *
   * {{{
   * aggregations:
   *   - name: order_count
   *     expr: count(*)
   * }}}
   */
  def namedExpressions(
      cfg: Map[String, Any],
      key: String): Seq[(String, String)] = {
    val value = cfg.get(key).filter(_ != null).getOrElse {
      throw new PuddingException(s"can not find param: $key, $key must be exist!")
    }
    val entries = value match {
      case map: Map[_, _] =>
        map.toSeq.map { case (mapKey, mapValue) => this.textEntry(key, mapKey, mapValue) }
      case map: java.util.Map[_, _] =>
        map.asScala.toSeq.map {
          case (mapKey, mapValue) => this.textEntry(key, mapKey, mapValue)
        }
      case seq: Seq[_] =>
        seq.map(item => this.namedExpressionEntry(item, key))
      case list: java.util.List[_] =>
        list.asScala.toSeq.map(item => this.namedExpressionEntry(item, key))
      case _ =>
        throw new PuddingException(
          s"$key must be a map or a list of name/expr entries!")
    }

    if (entries.isEmpty) {
      throw new PuddingException(s"$key can not be empty!")
    }
    val duplicates = this.duplicates(entries.map(_._1))
    if (duplicates.nonEmpty) {
      throw new PuddingException(
        s"$key contains duplicate output names: ${duplicates.mkString(", ")}")
    }
    entries
  }

  def requiredInt(cfg: Map[String, Any], key: String, minValue: Int): Int = {
    this.optionalInt(cfg, key, minValue).getOrElse {
      throw new PuddingException(s"can not find param: $key, $key must be exist!")
    }
  }

  def optionalInt(
      cfg: Map[String, Any],
      key: String,
      minValue: Int): Option[Int] = {
    cfg.get(key).filter(_ != null).map(value => this.intValue(value, key, minValue))
  }

  def optionalBoolean(cfg: Map[String, Any], key: String): Option[Boolean] = {
    cfg.get(key).filter(_ != null).map {
      case value: Boolean => value
      case value: String =>
        value.trim.toLowerCase(Locale.ROOT) match {
          case "true" => true
          case "false" => false
          case _ => throw new PuddingException(s"$key must be true or false, but got: $value")
        }
      case value =>
        throw new PuddingException(s"$key must be a boolean, but got: $value")
    }
  }

  def applyNumPartitions(
      dataFrame: DataFrame,
      cfg: Map[String, Any]): DataFrame = {
    this.optionalInt(cfg, NumPartitionsKey, 1) match {
      case Some(numPartitions) => dataFrame.repartition(numPartitions)
      case None => dataFrame
    }
  }

  def normalize(value: String): String = {
    Option(value).getOrElse("").toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "")
  }

  private def namedExpressionEntry(value: Any, key: String): (String, String) = {
    val values = value match {
      case map: Map[_, _] => map.map { case (mapKey, mapValue) => mapKey.toString -> mapValue }
      case map: java.util.Map[_, _] =>
        map.asScala.map { case (mapKey, mapValue) => mapKey.toString -> mapValue }.toMap
      case _ =>
        throw new PuddingException(
          s"$key entries must be maps with name and expr, but got: $value")
    }
    val name = values.get("name").map(item => this.requiredText(item, s"$key name")).getOrElse {
      throw new PuddingException(s"$key entries must contain name!")
    }
    val expression = values.get("expr").orElse(values.get("expression"))
      .map(item => this.requiredText(item, s"$key expr"))
      .getOrElse(throw new PuddingException(s"$key entries must contain expr!"))
    name -> expression
  }

  private def textEntry(key: String, mapKey: Any, mapValue: Any): (String, String) = {
    val name = this.requiredText(mapKey, s"$key key")
    val value = this.requiredText(mapValue, s"$key value")
    name -> value
  }

  private def requiredText(value: Any, key: String): String = value match {
    case text: String if text.trim.nonEmpty => text.trim
    case _: String => throw new PuddingException(s"$key can not contain an empty value!")
    case _ => throw new PuddingException(s"$key must contain strings!")
  }

  private def duplicates(values: Seq[String]): Seq[String] = {
    values.groupBy(identity).collect {
      case (value, occurrences) if occurrences.size > 1 => value
    }.toSeq.sorted
  }

  private def intValue(value: Any, key: String, minValue: Int): Int = {
    val parsed = value match {
      case item: Int => item
      case item: Long => item.toInt
      case item: BigInt => item.toInt
      case item: java.math.BigInteger => item.intValue()
      case item: BigDecimal if item.isValidInt => item.toInt
      case item: java.math.BigDecimal if item.stripTrailingZeros().scale() <= 0 =>
        item.intValue()
      case item: Double if item == item.floor => item.toInt
      case item: String =>
        try {
          item.trim.toInt
        } catch {
          case _: NumberFormatException =>
            throw new PuddingException(s"$key must be an integer, but got: $item")
        }
      case _ =>
        throw new PuddingException(s"$key must be an integer, but got: $value")
    }
    if (parsed < minValue) {
      throw new PuddingException(s"$key must be >= $minValue, but got: $parsed")
    }
    parsed
  }
}
