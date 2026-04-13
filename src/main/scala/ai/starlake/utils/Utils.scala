/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.job.Main
import ai.starlake.schema.model.Severity.*
import ai.starlake.schema.model.{TableAttribute, ValidationMessage, WriteMode}
import better.files.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.hubspot.jinjava.Jinjava
import com.typesafe.scalalogging.{LazyLogging, Logger}

import java.io.{PrintWriter, StringWriter}
import java.net.{HttpURLConnection, URL}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Facade object that delegates to focused utility modules.
  *
  * All methods are re-exported here for backward compatibility. New code should import the specific
  * utility objects directly: JinjaUtils, ProcessRunner, MapperFactory, GraphVizUtils, etc.
  */
object Utils extends LazyLogging {

  // --- Type aliases for backward compatibility ---
  type CommandOutput = ProcessRunner.CommandOutput
  val CommandOutput = ProcessRunner.CommandOutput
  type Closeable = { def close(): Unit }

  // --- Exception handling & resource management ---

  def withResources[T <: Closeable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      f(resource)
    } catch {
      case e: Throwable =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  def loadInstance[T](objectName: String): T = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(objectName)
    val obj: universe.ModuleMirror = runtimeMirror.reflectModule(module)
    obj.instance.asInstanceOf[T]
  }

  private def closeAndAddSuppressed(e: Throwable, resource: Closeable): Unit = {
    import scala.language.reflectiveCalls
    if (e != null) {
      e.printStackTrace()
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }

  def logFailure[T](attempt: Try[T], logger: Logger): Try[T] =
    attempt match {
      case success @ Success(_) => success
      case failure @ Failure(exception) =>
        logException(logger, exception)
        failure
    }

  def throwFailure[T](attempt: Try[T], logger: Logger): Boolean =
    attempt match {
      case Success(_) => true
      case Failure(exception) =>
        logException(logger, exception)
        throw exception
    }

  def logException(logger: Logger, exception: Throwable): Unit = {
    logger.error(exceptionAsString(exception))
  }

  @annotation.tailrec
  def hasCauseInStack[T <: Throwable](
    ex: Throwable
  )(implicit ct: reflect.ClassTag[T]): Boolean = {
    if (ct.runtimeClass.isInstance(ex)) true
    else if (ex.getCause() == null) false
    else hasCauseInStack(ex.getCause())
  }

  def exceptionMessagesChainAsString(exception: Throwable): String = {
    val messages = mutable.ListBuffer.empty[String]
    var currentException = exception
    var count = 1
    while (currentException != null) {
      val prefix = ">" * count
      val msg = Option(currentException.getMessage).getOrElse("").trim
      val msgPrefix = msg.indexOf("Exception:")
      if (msgPrefix > 0) {
        messages += prefix + " " + msg.substring(msgPrefix + "Exception:".length).trim
      } else {
        messages += prefix + " " + msg.trim
      }
      currentException = currentException.getCause
      count = count + 1
    }
    messages.mkString("\n")
  }

  def exceptionAsString(exception: Throwable): String = {
    val sw = new StringWriter
    exception.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  // --- Data mapping & validation ---

  def getDBDisposition(writeMode: WriteMode): (String, String) = {
    val (createDisposition, writeDisposition) =
      writeMode match {
        case WriteMode.OVERWRITE => ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
        case WriteMode.APPEND    => ("CREATE_IF_NEEDED", "WRITE_APPEND")
        case _                   => ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      }
    (createDisposition, writeDisposition)
  }

  def subst(value: String, params: Map[String, String]): String = {
    val paramsList = params.toList
    val paramKeys = paramsList.map { case (name, _) => name }
    val paramValues = paramsList.map { case (_, value) => value }
    val allParams = paramKeys.zip(paramValues)
    allParams.foldLeft(value) { case (acc, (p, v)) =>
      s"\\b($p)\\b".r.replaceAllIn(acc, v)
    }
  }

  def toMap(attributes: List[TableAttribute]): Map[String, Any] = {
    attributes.map { attribute =>
      attribute.attributes match {
        case Nil        => attribute.name -> attribute
        case attributes => attribute.name -> toMap(attributes)
      }
    }.toMap
  }

  def duplicates(
    target: String,
    values: List[String],
    errorMessage: String
  ): Either[List[ValidationMessage], Boolean] = {
    val errorList: mutable.ListBuffer[ValidationMessage] = mutable.ListBuffer.empty
    val duplicates = values.groupBy(identity).view.mapValues(_.size).filter { case (_, size) =>
      size > 1
    }
    duplicates.foreach { case (key, size) =>
      errorList += ValidationMessage(Error, target, errorMessage.format(key, size))
    }
    if (errorList.nonEmpty) Left(errorList.toList) else Right(true)
  }

  def combine(
    errors1: Either[List[ValidationMessage], Boolean],
    errors2: Either[List[ValidationMessage], Boolean]*
  ): Either[List[ValidationMessage], Boolean] = {
    val allErrors = errors1 :: List(errors2: _*)
    val errors = allErrors.collect { case Left(err) => err }.flatten
    if (errors.isEmpty) Right(true) else Left(errors)
  }

  def extractTags(tags: Set[String]): Set[(String, String)] = {
    tags.map { tag =>
      val hasValue = tag.indexOf('=') > 0
      val keyValuePAir = if (hasValue) tag.split('=') else Array(tag, "")
      keyValuePAir(0) -> keyValuePAir(1)
    }
  }

  // --- Environment detection ---

  def isIcebergAvailable(): Boolean =
    try { Class.forName("org.apache.iceberg.spark.SparkCatalog"); true }
    catch { case _: ClassNotFoundException => false }

  def isDeltaAvailable(): Boolean =
    try { Class.forName("io.delta.tables.DeltaTable"); true }
    catch { case _: ClassNotFoundException => false }

  def isRunningInDatabricks(): Boolean = sys.env.contains("DATABRICKS_RUNTIME_VERSION")

  def labels(tags: Set[String]): Map[String, String] =
    tags.map { tag =>
      val labelValue = tag.split("=")
      if (labelValue.size == 1) (labelValue(0), "") else (labelValue(0), labelValue(1))
    }.toMap

  def isRunningOnGcp(): Boolean = {
    val metadataUrl = "http://metadata.google.internal"
    try {
      val url = new URL(metadataUrl)
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      try {
        connection.setConnectTimeout(500)
        connection.setReadTimeout(500)
        connection.setRequestProperty("Metadata-Flavor", "Google")
        connection.getResponseCode == 200
      } finally {
        connection.disconnect()
      }
    } catch {
      case _: Exception => false
    }
  }

  // --- Delegated to JinjaUtils ---
  def jinjava(implicit settings: Settings): Jinjava = JinjaUtils.jinjava
  def resetJinjaClassLoader(): Unit = JinjaUtils.resetJinjaClassLoader()
  def renderJinja(str: String, params: Map[String, Any] = Map.empty)(implicit
    settings: Settings
  ): String = JinjaUtils.renderJinja(str, params)
  def parseJinja(str: String, params: Map[String, Any])(implicit settings: Settings): String =
    JinjaUtils.parseJinja(str, params)
  def parseJinja(str: List[String], params: Map[String, Any])(implicit
    settings: Settings
  ): List[String] = JinjaUtils.parseJinja(str, params)
  def parseJinjaTpl(templateContent: String, params: Map[String, Object])(implicit
    settings: Settings
  ): String = JinjaUtils.parseJinjaTpl(templateContent, params)

  // --- Delegated to MapperFactory ---
  def newYamlMapper(): ObjectMapper = MapperFactory.newYamlMapper()
  def newJsonMapper(): ObjectMapper = MapperFactory.newJsonMapper()
  def setMapperProperties(mapper: ObjectMapper): ObjectMapper =
    MapperFactory.setMapperProperties(mapper)

  // --- Delegated to GraphVizUtils ---
  def dot2Svg(outputFile: Option[File], str: String): Unit = GraphVizUtils.dot2Svg(outputFile, str)
  def dot2Png(outputFile: Option[File], str: String): Unit = GraphVizUtils.dot2Png(outputFile, str)
  def save(outputFile: Option[File], result: String): Unit = GraphVizUtils.save(outputFile, result)

  // --- Delegated to ProcessRunner ---
  def runCommand(cmd: Seq[String], extraEnv: Map[String, String]): Try[CommandOutput] =
    ProcessRunner.runCommand(cmd, extraEnv)
  def runCommand(cmd: Seq[String]): Try[CommandOutput] = ProcessRunner.runCommand(cmd)
  def runCommand(cmd: String, extraEnv: Map[String, String]): Try[CommandOutput] =
    ProcessRunner.runCommand(cmd, extraEnv)
  def runCommand(cmd: String): Try[CommandOutput] = ProcessRunner.runCommand(cmd)
  def runCommand(
    cmd: Seq[String],
    extraEnv: Map[String, String],
    outFile: File,
    errFile: File
  ): Try[CommandOutput] = ProcessRunner.runCommand(cmd, extraEnv, outFile, errFile)
  def runCommand(
    cmd: String,
    extraEnv: Map[String, String],
    outFile: File,
    errFile: File
  ): Try[CommandOutput] = ProcessRunner.runCommand(cmd, extraEnv, outFile, errFile)

  // --- Output & redaction ---

  def printOut(s: String): Unit = {
    if (Main.cliMode) println(s)
  }

  private val obfuscateKeys: Set[String] = Set("pass", "token", "access", "aes")

  def redact(options: Map[String, String]): Map[String, String] = {
    options.map { case (key, value) =>
      if (key.toLowerCase.contains("password")) key -> "********" else key -> value
    }
  }

  def obfuscate(map: Map[String, String]): Map[String, String] = {
    map.map { case (k, v) =>
      if (obfuscateKeys.exists(k.toLowerCase.contains)) k -> "********" else k -> v
    }
  }

  def obfuscate(cmd: Seq[String]): Seq[String] = {
    var result = ListBuffer[String]()
    var isSensitiveKey = false
    cmd.foreach { c =>
      if (isSensitiveKey) {
        result.append("********")
        isSensitiveKey = false
      } else if (obfuscateKeys.exists(c.toLowerCase.contains)) {
        isSensitiveKey = true
        result.append(c)
      } else
        result.append(c)
    }
    result.toList
  }
}
