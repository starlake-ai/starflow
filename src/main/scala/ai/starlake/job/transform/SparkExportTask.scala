package ai.starlake.job.transform

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AutoTaskInfo, FsSink, WriteStrategyType}
import ai.starlake.utils.{JobResult, SparkJobResult}
import org.apache.hadoop.fs.Path
import org.apache.poi.ss.usermodel.{
  Cell,
  CellStyle,
  Row => XlsRow,
  Sheet,
  Workbook,
  WorkbookFactory
}
import org.apache.poi.ss.util.CellReference
import org.apache.spark.sql.{DataFrame, Row}

import scala.annotation.tailrec
import scala.util.{Success, Try}
import scala.util.matching.Regex

class SparkExportTask(
  appId: Option[String],
  taskDesc: AutoTaskInfo,
  commandParameters: Map[String, String],
  interactive: Option[String],
  truncate: Boolean,
  test: Boolean,
  logExecution: Boolean,
  accessToken: Option[String] = None,
  resultPageSize: Int,
  resultPageNumber: Int,
  scheduledDate: Option[String]
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends SparkAutoTask(
      appId = appId,
      taskDesc = taskDesc,
      commandParameters = commandParameters,
      interactive = interactive,
      truncate = truncate,
      test = test,
      logExecution = logExecution,
      accessToken = accessToken,
      resultPageSize = resultPageSize,
      resultPageNumber = resultPageNumber,
      scheduledDate = scheduledDate,
      syncSchema = false
    ) {

  private lazy val fsSink: FsSink = sinkConfig match {
    case fs: FsSink => fs
    case other =>
      throw new IllegalStateException(
        s"SparkExportTask requires FsSink but got ${other.getClass.getSimpleName}"
      )
  }

  override protected def effectiveSinkToFile(dataset: DataFrame): Try[JobResult] = {
    val location = getExportFilePath(taskDesc.domain, taskDesc.table)
    (isCSV, isXls) match {
      case (true, _) =>
        dataset.write
          .format(fsSink.getStorageFormat())
          .mode(this.taskDesc.getWriteMode().toSaveMode)
          .options(fsSink.sparkStorageOptions())
          .save(location.toString)
        exportToCSV(
          taskDesc.domain,
          taskDesc.table,
          if (fsSink.withHeader.getOrElse(false)) Some(dataset.columns.toList) else None,
          fsSink.delimiter
        )
      case (_, true) =>
        exportToXls(taskDesc.domain, taskDesc.table, Some(dataset))
      case _ => // should never be the case because isExport is true
        throw new Exception(
          s"Unsupported format ${fsSink.format.getOrElse("")} for export ${taskDesc.name}"
        )
    }
    Success(SparkJobResult(Some(dataset), None))
  }

  private def isCSV = {
    (settings.appConfig.csvOutput || sinkConfig
      .asInstanceOf[FsSink]
      .format
      .getOrElse(
        ""
      ) == "csv") && !writeStrategy
      .isMerge()
  }

  private def isXls: Boolean = {
    sinkConfig
      .asInstanceOf[FsSink]
      .format
      .getOrElse(
        ""
      )
      .toLowerCase == "xls" && !writeStrategy
      .isMerge()
  }

  /** This function is called only if csvOutput is true This means we are sure that sink is an
    * FsSink
    *
    * @return
    */
  private def csvOutputExtension(): String =
    fsSink.extension.getOrElse(settings.appConfig.csvOutputExt)

  def exportToCSV(
    domainName: String,
    tableName: String,
    header: Option[List[String]],
    separator: Option[String]
  ): Boolean = {
    val location = getExportFilePath(domainName, tableName)

    val extension =
      if (csvOutputExtension().nonEmpty) {
        val ext = csvOutputExtension()
        if (ext.startsWith("."))
          ext
        else
          s".$ext"
      } else {
        ".csv"
      }
    val finalCsvPath: Path = fsSink.finalPath
      .map { p =>
        val parsed = parseJinja(List(p), allVars).head
        if (parsed.contains("://"))
          new Path(parsed)
        else
          new Path(settings.appConfig.datasets, parsed)
      }
      .getOrElse(getExportFilePath(domainName, tableName + extension))
    storageHandler.delete(finalCsvPath)

    val withHeader = header.isDefined
    val delimiter = separator.getOrElse("µ")
    val headerString =
      if (withHeader)
        Some(
          header
            .getOrElse(throw new IllegalStateException("should never happen"))
            .mkString(delimiter)
        )
      else
        None
    storageHandler.copyMerge(headerString, location, finalCsvPath, deleteSource = true)
  }

  private def getExportFilePath(domainName: String, tableName: String) = {
    val file = DatasetArea.export(domainName, tableName)
    storageHandler.mkdirs(file.getParent)
    file
  }

  private def outputExtension(): Option[String] = fsSink.extension

  private val startCellRegex: Regex = """([a-zA-Z]+)(\d+)""".r

  def exportToXls(
    domainName: String,
    tableName: String,
    result: Option[DataFrame]
  ): Boolean = {
    result match {
      case Some(df) =>
        // retrieve the domain export root path
        val domainDir = DatasetArea.`export`(domainName)
        storageHandler.mkdirs(domainDir)

        // retrieve the xls file extension
        val extension = {
          outputExtension() match {
            case Some(ext) if ext.nonEmpty =>
              if (ext.startsWith("."))
                ext
              else
                s".$ext"
            case _ => ".xlsx"
          }
        }

        // define the full path to the xls file
        val finalXlsPath =
          fsSink.finalPath
            .map { p =>
              val parsed = parseJinja(List(p), allVars).head
              if (parsed.contains("://"))
                new Path(parsed)
              else
                new Path(settings.appConfig.datasets, parsed)
            }
            .getOrElse(
              new Path(
                domainDir,
                tableName + extension
              )
            )

        // retrieve the schema of the dataset
        // create an iterator that will contain all the dataframe rows to sink to the xls file
        val rows = df.toLocalIterator()

        // create an input stream to the targeted workbook or the optional template
        val inputStream = storageHandler.open(finalXlsPath).orElse {
          fsSink.template.map { p =>
            if (p.contains("://"))
              new Path(p)
            else
              new Path(settings.appConfig.datasets, p)
          } match {
            case Some(value) => storageHandler.open(value)
            case _           => None
          }
        }

        // create the targeted workbook
        val workbook: Workbook = inputStream match {
          case Some(is) => WorkbookFactory.create(is)
          case _        => WorkbookFactory.create(true)
        }

        /** retrieve the workbook sheet name to which all the dataframe rows will be added - table
          * name by default
          */
        val sheetName = fsSink.sheetName.getOrElse(tableName)

        /** retrieve the workbook sheet and the last row added to the latter
          */
        val (sheet, lastRow) =
          Option(workbook.getSheet(sheetName)) match {
            case Some(sheet) =>
              writeStrategy.`type`.getOrElse(WriteStrategyType.APPEND) match {
                case WriteStrategyType.OVERWRITE =>
                  // TODO if a template has been defined, we don't want to simply create a new sheet
                  workbook.removeSheetAt(workbook.getSheetIndex(sheet))
                  (workbook.createSheet(sheetName), 0) // OVERWRITE
                case _ => (sheet, sheet.getLastRowNum + 1) // APPEND
              }
            case _ => (workbook.createSheet(sheetName), 0)
          }

        /** if xls:startCell sink option has been defined, start to write at the specified row
          * number and column index
          */
        val (rowNum, colIndex) = fsSink.startCell match {
          case Some(cell) =>
            val matcher = startCellRegex.pattern.matcher(cell)
            if (matcher.matches()) {
              val tempRowNum = matcher.group(2).toInt - 1
              val cellNum = CellReference.convertColStringToIndex(matcher.group(1))
              // if the write strategy type has been defined to APPEND,
              // then we will start writing to the row specified if the later is larger than the last row previously added
              // or if the start cell is empty
              if (
                tempRowNum > lastRow || Option(
                  sheet
                    .getRow(tempRowNum)
                ).flatMap(r =>
                  Option(r.getCell(cellNum, XlsRow.MissingCellPolicy.RETURN_BLANK_AS_NULL))
                ).isEmpty
              )
                (tempRowNum, cellNum)
              else
                (lastRow, cellNum)
            } else {
              (lastRow, 0)
            }
          case _ => (lastRow, 0)
        }

        /** add each dataframe row to the workbook sheet, starting at the computed row number and
          * column index
          */
        writeXlsRow(sheet, rows, rowNum, colIndex)

        /** close the input stream
          */
        inputStream.foreach(_.close())

        /** create an output stream to the workbook
          */
        val outputStream = storageHandler.output(finalXlsPath)

        /** write the workbook
          */
        workbook.write(outputStream)

        /** close the workbook
          */
        workbook.close()

        /** close the output stream
          */
        outputStream.close()

        true

      case _ =>
        logger.warn(s"No dataframe has been provided for $domainName.$fullTableName xls sink")
        false
    }
  }

  @tailrec
  private def writeXlsRow(
    sheet: Sheet,
    rows: java.util.Iterator[Row],
    rowNum: Int,
    colIndex: Int
  ): Unit = {
    if (rows.hasNext) {
      val row = rows.next()
      val sheetRow = Option(sheet.getRow(rowNum)).getOrElse(sheet.createRow(rowNum))
      val fields = row.toSeq.map(Option(_)).toList
      for ((field, idx) <- fields.zipWithIndex) {
        val cell = Option(sheetRow.getCell(colIndex + idx)) match {
          case Some(cell) => cell
          case None       => sheetRow.createCell(colIndex + idx)
        }
        cell.setCellStyle(getPreferredCellStyle(cell))
        field match {
          case Some(value) =>
            value match {
              case b: Boolean             => cell.setCellValue(b)
              case d: Double              => cell.setCellValue(d)
              case f: Float               => cell.setCellValue(f)
              case t: java.sql.Date       => cell.setCellValue(t)
              case t: java.sql.Timestamp  => cell.setCellValue(t)
              case t: java.time.Instant   => cell.setCellValue(java.sql.Timestamp.from(t))
              case t: java.time.LocalDate => cell.setCellValue(t)
              case other                  => cell.setCellValue(other.toString)
            }
          case None => cell.setCellValue("")
        }
      }
      writeXlsRow(sheet, rows, rowNum + 1, colIndex)
    }
  }

  private def getPreferredCellStyle(cell: Cell): CellStyle = {
    var cellStyle = Option(cell.getCellStyle)
    if (cellStyle.map(_.getIndex).getOrElse(0) == 0) cellStyle = Option(cell.getRow.getRowStyle)
    if (cellStyle.isEmpty) cellStyle = Option(cell.getSheet.getColumnStyle(cell.getColumnIndex))
    cellStyle.getOrElse(cell.getCellStyle)
  }

  override def buildRLSQueries(): List[String] = throw new Exception("RLS not supported for export")
}
