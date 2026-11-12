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

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/**
 * Resolves the job configuration from exactly one of an inline argument, an environment
 * variable or a file. Error messages never include configuration values.
 */
object JobConfigSource {

  private val ENV_NAME = "^[A-Za-z_][A-Za-z0-9_]*$".r

  /**
   * Resolve a job configuration using the process environment.
   *
   * @param jobText inline configuration text
   * @param jobEnv  environment variable containing the configuration
   * @param jobFile path to a UTF-8 configuration file
   * @return configuration text
   */
  def resolve(
      jobText: Option[String],
      jobEnv: Option[String],
      jobFile: Option[String]): String = {
    resolve(jobText, jobEnv, jobFile, sys.env)
  }

  /**
   * Resolve a job configuration from exactly one source.
   *
   * @param jobText     inline configuration text
   * @param jobEnv      environment variable containing the configuration
   * @param jobFile     path to a UTF-8 configuration file
   * @param environment environment variables
   * @return configuration text
   */
  def resolve(
      jobText: Option[String],
      jobEnv: Option[String],
      jobFile: Option[String],
      environment: Map[String, String]): String = {
    val text = nonBlank(jobText)
    val envName = nonBlank(jobEnv)
    val fileName = nonBlank(jobFile)
    val sources = Seq(text, envName, fileName).flatten

    if (sources.size > 1) {
      throw new PuddingException(
        "specify exactly one of --jobText, --jobEnv or --jobFile, not several")
    }
    sources.headOption match {
      case None =>
        throw new PuddingException("one of --jobText, --jobEnv or --jobFile is required")
      case Some(_) if text.isDefined =>
        text.get
      case Some(_) if envName.isDefined =>
        readEnvironment(envName.get, environment)
      case Some(_) =>
        readFile(fileName.get)
    }
  }

  private def readFile(fileName: String): String = {
    val path = Paths.get(fileName)
    if (!Files.isRegularFile(path)) {
      throw new PuddingException(s"job configuration file does not exist: $path")
    }
    if (!Files.isReadable(path)) {
      throw new PuddingException(s"job configuration file is not readable: $path")
    }
    val text =
      try {
        new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
      } catch {
        case e: IOException =>
          throw new PuddingException(
            s"failed to read job configuration file: $path (${e.getClass.getSimpleName})", e)
      }
    if (text.trim.isEmpty) {
      throw new PuddingException(s"job configuration file is empty: $path")
    }
    text
  }

  private def readEnvironment(name: String, environment: Map[String, String]): String = {
    name match {
      case ENV_NAME() =>
      case _ => throw new PuddingException(
        s"invalid job environment variable name: $name")
    }

    environment.get(name) match {
      case None =>
        throw new PuddingException(s"job environment variable is not set: $name")
      case Some(value) if value.trim.isEmpty =>
        throw new PuddingException(s"job environment variable is empty: $name")
      case Some(value) =>
        value
    }
  }

  private def nonBlank(value: Option[String]): Option[String] = {
    value match {
      case Some(text) if text.trim.nonEmpty => Some(text)
      case _ => None
    }
  }
}
