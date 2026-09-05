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

package ai.starlake.schema.handlers

import ai.starlake.utils.Utils
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang.SystemUtils
import org.apache.hadoop.fs._

import java.io.{InputStream, InputStreamReader, OutputStream}
import java.nio.charset.Charset.defaultCharset
import java.nio.charset.{Charset, StandardCharsets}
import java.time.LocalDateTime
import java.util.regex.Pattern
import scala.util.Try

/** Interface required by any filesystem manager
  */
trait StorageHandler extends LazyLogging {
  val starApiIsActive: Boolean = {
    Try(Utils.loadInstance("ai.starlake.api.Application")).isSuccess

  }
  protected def pathSecurityCheck(path: Path): Unit = {
    if (starApiIsActive && path.toString.contains("..")) {
      throw new Exception(s"Security check: Path cannot contain '..'. File $path")
    }
  }

  def move(src: Path, dst: Path): Boolean

  def copy(src: Path, dst: Path): Boolean

  def delete(path: Path): Boolean

  def exists(path: Path): Boolean

  def mkdirs(path: Path): Boolean

  def copyFromLocal(source: Path, dest: Path): Unit

  def copyToLocal(source: Path, dest: Path): Unit

  def moveFromLocal(source: Path, dest: Path): Unit

  def moveSparkPartFile(sparkFolder: Path, extension: String): Option[Path] =
    collapseSparkFolder(
      sparkFolder,
      list(sparkFolder, extension = extension, recursive = false).headOption.map(_.path)
    )

  /** Spark names its single part file after the output format (.csv, .txt, sometimes no extension
    * at all), so the part file is picked by its `part-` prefix rather than by extension.
    */
  def moveSparkPartFile(sparkFolder: Path): Option[Path] =
    collapseSparkFolder(
      sparkFolder,
      list(sparkFolder, extension = "", recursive = false)
        .map(_.path)
        .find(_.getName.startsWith("part-"))
    )

  private def collapseSparkFolder(sparkFolder: Path, partFile: Option[Path]): Option[Path] = {
    partFile.map { f =>
      val tmpFile = new Path(sparkFolder.getParent, sparkFolder.getName + ".tmp")
      move(f, tmpFile)
      delete(sparkFolder)
      move(tmpFile, sparkFolder)
      sparkFolder
    }
  }

  def read(path: Path, charset: Charset = StandardCharsets.UTF_8): String

  def readAndExecute[T](path: Path, charset: Charset = StandardCharsets.UTF_8)(
    action: InputStreamReader => T
  ): T

  def readAndExecuteIS[T](path: Path)(
    action: InputStream => T
  ): T

  def write(data: String, path: Path)(implicit charset: Charset = defaultCharset): Unit

  def writeBinary(data: Array[Byte], path: Path): Unit

  def listDirectories(path: Path): List[Path]

  def list(
    path: Path,
    extension: String = "",
    since: LocalDateTime = LocalDateTime.MIN,
    recursive: Boolean,
    exclude: Option[Pattern] = None,
    sortByName: Boolean = false // sort by time by default
  ): List[FileInfo]

  def stat(
    path: Path
  ): FileInfo

  def blockSize(path: Path): Long

  def lastModified(path: Path): Long

  def spaceConsumed(path: Path): Long

  def touchz(path: Path): Try[Unit]

  def touch(path: Path): Try[Unit]

  def lockAcquisitionPollTime: Long
  def lockRefreshPollTime: Long

  def getScheme(): String

  def loadExtraConf(options: Map[String, String]): Unit = {}
  def extraConf(opts: Map[String, String]): Map[String, String] = Map.empty

  def copyMerge(header: Option[String], srcDir: Path, dstFile: Path, deleteSource: Boolean): Boolean

  def open(path: Path): Option[InputStream]

  def output(path: Path): OutputStream

  def initFS(options: Map[String, String]): Unit
}

object StorageHandler {
  private val HAS_DRIVE_LETTER_SPECIFIER = Pattern.compile("^/?[a-zA-Z]:")

  def localFile(path: Path): File = {
    val pathAsString = path.toUri.getPath
    val isWindowsFile =
      SystemUtils.IS_OS_WINDOWS && HAS_DRIVE_LETTER_SPECIFIER.matcher(pathAsString).find()
    if (isWindowsFile)
      File(pathAsString.substring(1))
    else
      File(pathAsString)
  }

}
