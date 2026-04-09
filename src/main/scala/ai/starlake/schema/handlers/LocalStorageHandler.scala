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

import ai.starlake.config.Settings
import better.files.File
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import java.io.{IOException, InputStream, InputStreamReader, OutputStream}
import java.nio.charset.{Charset, StandardCharsets}
import java.time.{LocalDateTime, ZoneId}
import java.util.regex.Pattern
import scala.util.{Failure, Success, Try}

/** HDFS Filesystem Handler
  */
class LocalStorageHandler(implicit
  settings: Settings
) extends StorageHandler {

  import StorageHandler._

  def lockAcquisitionPollTime: Long = settings.appConfig.lock.pollTime

  def lockRefreshPollTime: Long = settings.appConfig.lock.refreshTime

  /** Gets the outputstream given a path
    *
    * @param path
    *   : path
    * @return
    *   FSDataOutputStream
    */
  private def getOutputStream(path: Path): OutputStream = {
    pathSecurityCheck(path)
    val file = localFile(path)
    file.delete(true)
    file.newOutputStream()
  }

  /** Read a UTF-8 text file into a string used to load yml configuration files
    *
    * @param path
    *   : Absolute file path
    * @return
    *   file content as a string
    */
  def read(path: Path, charset: Charset = StandardCharsets.UTF_8): String = {
    pathSecurityCheck(path)
    readAndExecute(path, charset) { is =>
      IOUtils.toString(is)
    }
  }

  /** read input stream and do something with input
    *
    * @param path
    *   : Absolute file path
    * @return
    *   file content as a string
    */
  def readAndExecute[T](path: Path, charset: Charset = StandardCharsets.UTF_8)(
    action: InputStreamReader => T
  ): T = {
    readAndExecuteIS(path) { is =>
      val codecFactory = new CompressionCodecFactory(new Configuration())
      val decompressedIS =
        Option(codecFactory.getCodec(path)).map(_.createInputStream(is)).getOrElse(is)
      action(new InputStreamReader(decompressedIS, charset))
    }
  }

  override def readAndExecuteIS[T](path: Path)(action: InputStream => T): T = {
    pathSecurityCheck(path)
    val file = localFile(path)
    file.fileInputStream
      .map(action)
      .get()
  }

  /** Write a string to a UTF-8 text file. Used for yml configuration files.
    *
    * @param data
    *   file content as a string
    * @param path
    *   : Absolute file path
    */
  def write(data: String, path: Path)(implicit charset: Charset): Unit = {
    pathSecurityCheck(path)
    val file = localFile(path)
    file.parent.createDirectories()
    file.overwrite(data)
  }

  /** Write bytes to binary file. Used for zip / gzip input test files.
    *
    * @param data
    *   file content as a string
    * @param path
    *   : Absolute file path
    */
  def writeBinary(data: Array[Byte], path: Path): Unit = {
    pathSecurityCheck(path)
    val file = localFile(path)
    file.parent.createDirectories()
    file.writeByteArray(data)
  }

  def listDirectories(path: Path): List[Path] = {
    pathSecurityCheck(path)
    val file = localFile(path)
    file.list.filter(_.isDirectory).map(f => new Path(f.pathAsString)).toList
  }

  def stat(path: Path): FileInfo = {
    pathSecurityCheck(path)
    val file = localFile(path)
    FileInfo(path, file.size, file.lastModifiedTime)
  }

  /** List all files in folder
    *
    * @param path
    *   Absolute folder path
    * @param extension
    *   : Files should end with this string. To list all files, simply provide an empty string
    * @param since
    *   Minimum modification time of liste files. To list all files, simply provide the beginning of
    *   all times
    * @param recursive
    *   : List all files recursively ?
    * @return
    *   List of Path
    */
  def list(
    path: Path,
    extension: String,
    since: LocalDateTime,
    recursive: Boolean,
    exclude: Option[Pattern],
    sortByName: Boolean = false // sort by time by default
  ): List[FileInfo] = {
    pathSecurityCheck(path)
    logger.info(s"list($path, $extension, $since)")
    Try {
      if (exists(path)) {
        val file = localFile(path)
        val fileList =
          if (recursive)
            file.listRecursively()
          else
            file.list

        val iterator = fileList.filter(_.isRegularFile)
        val files = iterator.filter { f =>
          logger.info(s"found file=$f")
          val time = LocalDateTime.ofInstant(f.lastModifiedTime, ZoneId.systemDefault)
          val excludeFile =
            exclude.exists(_.matcher(f.name).matches())
          !excludeFile && time.isAfter(since) && f.name.endsWith(extension)
        }.toList
        val sorted =
          if (sortByName)
            files.sortBy(_.name)
          else
            files.sortBy(f => (f.lastModifiedTime, f.name))

        sorted.map(f => FileInfo(new Path(f.pathAsString), f.size, f.lastModifiedTime))
      } else
        Nil
    } match {
      case Success(list) => list
      case Failure(e) =>
        logger.warn(s"Ignoring folder $path", e)
        Nil
    }
  }

  /** Copy file
    *
    * @param src
    *   source path
    * @param dest
    *   destination path
    * @return
    */
  override def copy(src: Path, dest: Path): Boolean = {
    pathSecurityCheck(src)
    pathSecurityCheck(dest)
    val fsrc = localFile(src)
    val fdest = localFile(dest)
    mkdirs(dest.getParent)
    fsrc.copyTo(fdest)
    true
  }

  /** Move file
    *
    * @param src
    *   source path (file or folder)
    * @param dest
    *   destination path (file or folder)
    * @return
    */
  def move(src: Path, dest: Path): Boolean = {
    pathSecurityCheck(src)
    pathSecurityCheck(dest)
    val fsrc = localFile(src)
    val fdest = localFile(dest)
    fdest.delete(true)
    mkdirs(dest.getParent)
    fsrc.moveTo(fdest)
    true
  }

  /** delete file (skip trash)
    *
    * @param path
    *   : Absolute path of file to delete
    */
  def delete(path: Path): Boolean = {
    pathSecurityCheck(path)
    val file = localFile(path)
    file.delete(true)
    true
  }

  /** Create folder if it does not exsit including any intermediary non existent folder
    *
    * @param path
    *   Absolute path of folder to create
    */
  def mkdirs(path: Path): Boolean = {
    pathSecurityCheck(path)
    val file = localFile(path)
    file.createDirectories()
    true
  }

  /** Copy file from local filesystem to target file system
    *
    * @param source
    *   Local file path
    * @param dest
    *   destination file path
    */
  def copyFromLocal(src: Path, dest: Path): Unit = {
    pathSecurityCheck(src)
    pathSecurityCheck(dest)
    val fsrc = localFile(src)
    val fdest = localFile(dest)
    fdest.delete(true)
    mkdirs(dest.getParent)
    fsrc.copyTo(fdest)

  }

  /** Copy file to local filesystem from remote file system
    *
    * @param source
    *   Remote file path
    * @param dest
    *   Local file path
    */
  def copyToLocal(src: Path, dest: Path): Unit = copyFromLocal(src, dest)

  /** Move file from local filesystem to target file system If source FS Scheme is not "file" then
    * issue a regular move
    *
    * @param source
    *   Local file path
    * @param dest
    *   destination file path
    */
  def moveFromLocal(source: Path, dest: Path): Unit = {
    pathSecurityCheck(source)
    pathSecurityCheck(dest)
    this.move(source, dest)
  }

  def exists(path: Path): Boolean = {
    pathSecurityCheck(path)
    val file = localFile(path)
    file.exists
  }

  def blockSize(path: Path): Long = {
    64 * 1024 * 1024 // 64 MB
  }

  def spaceConsumed(path: Path): Long = {
    pathSecurityCheck(path)
    val file = localFile(path)
    file.size()
  }

  def lastModified(path: Path): Timestamp = {
    pathSecurityCheck(path)
    val file = localFile(path)
    file.lastModifiedTime.toEpochMilli
  }

  def touchz(path: Path): Try[Unit] = {
    pathSecurityCheck(path)
    val file = localFile(path)
    Try(file.touch())
  }

  def touch(path: Path): Try[Unit] = {
    pathSecurityCheck(path)
    touchz(path)
  }

  def getScheme(): String = "file"

  override def copyMerge(
    header: Option[String],
    srcDir: Path,
    dstFile: Path,
    deleteSource: Boolean
  ): Boolean = {
    pathSecurityCheck(srcDir)
    pathSecurityCheck(dstFile)
    val sourceDir = StorageHandler.localFile(srcDir)
    val destFile = StorageHandler.localFile(dstFile)

    if (destFile.exists()) {
      throw new IOException(s"Target $dstFile already exists")
    }

    // Source path is expected to be a directory:
    if (sourceDir.isDirectory()) {
      val parts = sourceDir.list(file => file.isRegularFile).toList
      header.foreach { header =>
        val headerWithNL = if (header.endsWith("\n")) header else header + "\n"
        destFile.append(headerWithNL)
      }
      parts
        .filter(part => part.name.startsWith("part-"))
        .sortBy(_.name)
        .collect { case part =>
          destFile.append(part.contentAsString)
          if (deleteSource) part.delete(swallowIOExceptions = true)
        }
      true
    } else
      false
  }

  override def open(path: Path): Option[InputStream] = {
    pathSecurityCheck(path)
    val file = localFile(path)
    Try(file.fileInputStream.get()) match {
      case Success(is) => Some(is)
      case Failure(f) =>
        logger.error(f.getMessage)
        None
    }
  }

  override def output(path: Path): OutputStream = {
    pathSecurityCheck(path)
    localFile(path).newOutputStream
  }

  def initFS(options: Map[String, String]): Unit = {}
}
