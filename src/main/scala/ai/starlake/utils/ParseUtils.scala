package ai.starlake.utils

object ParseUtils {

  /** Parse the sql string and return the tables and select statement
    * @param str
    * @return
    *   tables and select statements tables: Array[(String, Array[(String, String)])]
    *
    * select: String
    */
  def parse(
    str: String,
    markers: List[String]
  ): (Array[(String, Array[(String, String)])], String) = {
    val regex = ";\\R"
    val statements =
      str
        .split('\n')
        .filter(!_.startsWith("--"))
        .mkString("\n")
        .split(regex)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(_.replaceAll(" +", " ").replaceAll("\\R", "\n"))

    val createTables = statements.filter {
      _.toUpperCase().startsWith("CREATE ")
    }

    val selectStatement = statements.filter { statement =>
      markers.exists(statement.toUpperCase().startsWith(_))
    }

    if (selectStatement.length > 1)
      throw new Exception(
        s"Only one FROM/SELECT statement always found ${selectStatement.length} in $selectStatement"
      )

    if (selectStatement.isEmpty)
      throw new Exception(
        "No FROM/SELECT statement found ! Did you forget any ';' separator between the statements"
      )

    val tableAndColumns =
      createTables.flatMap { createTable =>
        val isCreateTable =
          createTable
            .replaceAll("\\R", "")
            .split('(')
            .headOption
            .exists(_.toUpperCase().contains(" TABLE "))
        if (isCreateTable) {
          val parIndex = createTable.indexOf('(')
          var index = parIndex - 1
          while (createTable.charAt(index).isSpaceChar && index > 0) {
            index = index - 1
          }
          val tableName =
            if (index > 0) {
              val endTableName = index
              while (!createTable.charAt(index).isSpaceChar && index > 0) {
                index = index - 1
              }
              if (index > 0) {
                val tableName = createTable.substring(index + 1, endTableName + 1)
                tableName
              } else {
                throw new Exception(s"Invalid create table expression $createTable")
              }

            } else {
              throw new Exception(s"Table name not found in $createTable")
            }

          val withoutTableName = createTable.substring(parIndex + 1)
          val columns =
            withoutTableName.split(',').map(_.trim)
          val colNamesAndTypes =
            columns.map { column =>
              val colNameAndType = column.split(' ')
              require(
                colNameAndType.length >= 2,
                s"Invalid syntax $column in $createTable"
              )
              val colName = colNameAndType(0)
              val colType = colNameAndType(1)
              var index = 0
              while (
                index < colType.length && (Character.isLetter(
                  colType(index)
                ) || Character
                  .isDigit(colType(index)))
              ) {
                index = index + 1
              }
              val finalColType = colType.substring(0, index)
              (colName, finalColType)
            }
          Some((tableName, colNamesAndTypes))
        } else {
          None
        }
      }
    (tableAndColumns, selectStatement.head)
  }
}
