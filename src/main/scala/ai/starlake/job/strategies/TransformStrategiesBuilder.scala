package ai.starlake.job.strategies

import ai.starlake.config.Settings
import ai.starlake.config.Settings.JdbcEngine
import ai.starlake.job.strategies.TransformStrategiesBuilder.TableComponents
import ai.starlake.schema.generator.WriteStrategyTemplateLoader
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.LazyLogging

class TransformStrategiesBuilder extends LazyLogging {

  def buildSqlWithJ2(
    strategy: WriteStrategy,
    selectStatementInput: String,
    tableComponents: TableComponents,
    targetTableExists: Boolean,
    truncate: Boolean,
    materializedView: Materialization,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink,
    action: String,
    templateVariables: Map[String, Any] = Map.empty
  )(implicit settings: Settings): String = {
    val selectStatement = {
      val trimmed = selectStatementInput.trim
      if (trimmed.endsWith(";")) trimmed.dropRight(1) else trimmed
    }
    val context = TransformStrategiesBuilder.StrategiesGenerationContext(
      strategy,
      selectStatement,
      tableComponents,
      targetTableExists,
      truncate,
      materializedView,
      jdbcEngine,
      sinkConfig
    )
    val templateLoader = new WriteStrategyTemplateLoader()
    val paramMap = (context.asMap() ++ templateVariables).asInstanceOf[Map[String, Object]]
    val contentTemplate = templateLoader.loadTemplate(
      s"${jdbcEngine.strategyBuilder.toLowerCase()}/${action.toLowerCase()}.j2"
    )

    // because jinjava does not like # chars and treat them as comments :( we need to replace them before putting them back
    val (contentTemplateUpdated, isSlIncomingRedshiftTempView) =
      if (contentTemplate.contains("#SL_INCOMING")) {
        (contentTemplate.replaceAll("#SL_INCOMING", "__SL_INCOMING__"), true)
      } else {
        (contentTemplate, false)
      }

    val macrosContent = templateLoader.loadMacros().toOption.getOrElse("")
    val jinjaOutput = Utils.parseJinjaTpl(macrosContent + "\n" + contentTemplateUpdated, paramMap)

    val jinjaOutputUpdated =
      if (isSlIncomingRedshiftTempView) {
        jinjaOutput.replaceAll("__SL_INCOMING__", "#SL_INCOMING")
      } else {
        jinjaOutput
      }
    jinjaOutputUpdated
  }

  def buildTransform(
    strategy: WriteStrategy,
    selectStatement: String,
    tableComponents: TransformStrategiesBuilder.TableComponents,
    targetTableExists: Boolean,
    truncate: Boolean,
    materializedView: Materialization,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink,
    templateVariables: Map[String, Any] = Map.empty
  )(implicit settings: Settings): String = {
    logger.info(
      s"Running Write strategy: ${strategy.`type`} for table ${tableComponents.getFullTableName()} with options: truncate: $truncate, materializedView: $materializedView, targetTableExists: $targetTableExists"
    )
    if (materializedView == Materialization.VIEW) {
      buildSqlWithJ2(
        strategy,
        selectStatement,
        tableComponents,
        targetTableExists,
        truncate,
        materializedView,
        jdbcEngine,
        sinkConfig,
        "VIEW",
        templateVariables
      )

    } else if (targetTableExists) {
      if (strategy.getEffectiveType() == WriteStrategyType.OVERWRITE_BY_PARTITION)
        TransformStrategiesBuilder.requireSelectedPartition(sinkConfig, tableComponents)
      buildSqlWithJ2(
        strategy,
        selectStatement,
        tableComponents,
        targetTableExists,
        truncate,
        materializedView,
        jdbcEngine,
        sinkConfig,
        strategy.getEffectiveType().toString,
        templateVariables
      )
    } else {
      buildSqlWithJ2(
        strategy,
        selectStatement,
        tableComponents,
        targetTableExists,
        truncate,
        materializedView,
        jdbcEngine,
        sinkConfig,
        "CREATE",
        templateVariables
      )
    }
  }
}

object TransformStrategiesBuilder {
  def apply(): TransformStrategiesBuilder = {
    new TransformStrategiesBuilder()
    /*
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol: ru.ClassSymbol =
      mirror.staticClass(className)
    val classMirror = mirror.reflectClass(classSymbol)

    val consMethodSymbol = classSymbol.primaryConstructor.asMethod
    val consMethodMirror = classMirror.reflectConstructor(consMethodSymbol)

    val strategyBuilder = consMethodMirror.apply().asInstanceOf[StrategiesBuilder]
    strategyBuilder
     */
  }

  /** Fails when the partition of an OVERWRITE_BY_PARTITION strategy is a column that the SELECT
    * does not return: the DELETE of the templates would read the partition of each row of the
    * target, and delete them all (#1875). A partition given as an expression is not checked, nor a
    * SELECT whose columns are not known.
    */
  def requireSelectedPartition(sinkConfig: Sink, tableComponents: TableComponents): Unit = {
    val columns = SQLUtils.unquoteAgressive(tableComponents.columnNames)
    sinkConfig.toAllSinks().partition.flatMap(_.headOption).map(SQLUtils.unquoteAgressive).foreach {
      partition =>
        if (
          partition.matches("[A-Za-z_][A-Za-z0-9_]*") && columns.nonEmpty &&
          !columns.exists(column => column.endsWith("*") || column.equalsIgnoreCase(partition))
        )
          throw new IllegalArgumentException(
            s"OVERWRITE_BY_PARTITION on ${tableComponents.getFullTableName()}: the SELECT does not return the partition column $partition"
          )
    }
  }

  def asJavaList[T](l: List[T]): java.util.ArrayList[T] = {
    val res = new java.util.ArrayList[T]()
    l.foreach(key => res.add(key))
    res
  }
  def asJavaMap[K, V](m: Map[K, V]): java.util.Map[K, V] = {
    val res = new java.util.HashMap[K, V]()
    m.foreach { case (k, v) =>
      res.put(k, v)
    }
    res
  }

  implicit class JavaWriteStrategy(writeStrategy: WriteStrategy) {

    def asMap(jdbcEngine: JdbcEngine): Map[String, Any] = {
      Map(
        "strategyType"  -> writeStrategy.`type`.getOrElse(WriteStrategyType.APPEND).toString,
        "strategyTypes" -> asJavaMap(writeStrategy.types.getOrElse(Map.empty[String, String])),
        "strategyKey"   -> asJavaList(writeStrategy.key),
        "quotedStrategyKey" -> asJavaList(
          SQLUtils
            .quoteCols(SQLUtils.unquoteCols(writeStrategy.key, jdbcEngine.quote), jdbcEngine.quote)
        ),
        "strategyTimestamp"   -> writeStrategy.timestamp.getOrElse(""),
        "strategyQueryFilter" -> writeStrategy.queryFilter.getOrElse(""),
        "strategyOn"          -> writeStrategy.on.getOrElse(MergeOn.TARGET).toString,
        "strategyStartTs"     -> writeStrategy.startTs.getOrElse(""),
        "strategyEndTs"       -> writeStrategy.endTs.getOrElse(""),
        "strategyKeyCsv"      -> writeStrategy.keyCsv(jdbcEngine.quote),
        "strategyKeyJoinCondition" -> writeStrategy.keyJoinCondition(
          jdbcEngine.quote,
          "SL_INCOMING",
          "SL_EXISTING"
        )
      )
    }
  }

  implicit class JavaJdbcEngine(jdbcEngine: JdbcEngine) {
    def asMap(): Map[String, Any] = {
      Map(
        "engineQuote"           -> jdbcEngine.quote,
        "engineViewPrefix"      -> jdbcEngine.viewPrefix.getOrElse(""),
        "enginePreActions"      -> jdbcEngine.preActions.getOrElse(""),
        "engineStrategyBuilder" -> jdbcEngine.strategyBuilder
      )
    }
  }
  case class TableComponents(
    database: String,
    domain: String,
    name: String,
    columnNames: List[String]
  ) {
    def getFullTableName(): String = {
      (database, domain, name) match {
        case ("", "", _) => name
        case ("", _, _)  => s"$domain.$name"
        case (_, _, _)   => s"$database.$domain.$name"
      }
    }
    def paramsForInsertSql(quote: String): String = {
      val targetColumns = SQLUtils.targetColumnsForSelectSql(columnNames, quote)
      val tableIncomingColumnsCsv =
        SQLUtils.incomingColumnsForSelectSql("SL_INCOMING", columnNames, quote)
      s"""($targetColumns) VALUES ($tableIncomingColumnsCsv)"""
    }

    def asMap(jdbcEngine: JdbcEngine): Map[String, Any] = {
      val tableIncomingColumnsCsv =
        SQLUtils.incomingColumnsForSelectSql("SL_INCOMING", columnNames, jdbcEngine.quote)
      val tableInsert = "INSERT " + paramsForInsertSql(jdbcEngine.quote)
      val tableUpdateSetExpression =
        SQLUtils.setForUpdateSql("SL_INCOMING", columnNames, jdbcEngine.quote)

      Map(
        "tableDatabase"    -> database,
        "tableDomain"      -> domain,
        "tableName"        -> name,
        "tableColumnNames" -> asJavaList(columnNames),
        "quotedTableColumnNames" -> asJavaList(
          columnNames.map(col => s"${jdbcEngine.quote}$col${jdbcEngine.quote}")
        ),
        "tableFullName"           -> getFullTableName(),
        "tableParamsForInsertSql" -> paramsForInsertSql(jdbcEngine.quote),
        "tableParamsForUpdateSql" -> SQLUtils
          .setForUpdateSql("SL_INCOMING", columnNames, jdbcEngine.quote),
        "tableInsert"              -> tableInsert,
        "tableUpdateSetExpression" -> tableUpdateSetExpression,
        "tableColumnsCsv" -> SQLUtils.targetColumnsForSelectSql(columnNames, jdbcEngine.quote),
        "tableIncomingColumnsCsv" -> tableIncomingColumnsCsv,
        "quote"                   -> jdbcEngine.quote
      )
    }
  }

  case class StrategiesGenerationContext(
    strategy: WriteStrategy,
    selectStatement: String,
    tableComponents: TransformStrategiesBuilder.TableComponents,
    targetTableExists: Boolean,
    truncate: Boolean,
    materializedView: Materialization,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink
  ) {

    def asMap()(implicit settings: Settings): Map[String, Any] = {
      val (partitionPruningKey, quotedPartitionPruningKey) =
        if (settings.appConfig.prunePartitionOnMerge && strategy.key.nonEmpty) {
          sinkConfig
            .toAllSinks()
            .partition
            .flatMap(partitions => partitions.find(p => strategy.key.contains(p)))
            .map(k => k -> jdbcEngine.quoteIdentifier(k))
            .getOrElse("" -> "")
        } else {
          "" -> ""
        }
      val tableFormat = sinkConfig
        .toAllSinks()
        .format
        .getOrElse(settings.appConfig.defaultWriteFormat)
      strategy.asMap(jdbcEngine) ++ tableComponents.asMap(jdbcEngine) ++ Map(
        "selectStatement"           -> selectStatement,
        "tableExists"               -> targetTableExists,
        "tableTruncate"             -> truncate,
        "materializedView"          -> materializedView.toString,
        "tableFormat"               -> tableFormat,
        "partitionPruningKey"       -> partitionPruningKey,
        "quotedPartitionPruningKey" -> quotedPartitionPruningKey
      ) ++ jdbcEngine.asMap() ++ sinkConfig.asMap(jdbcEngine)

    }
  }
}
