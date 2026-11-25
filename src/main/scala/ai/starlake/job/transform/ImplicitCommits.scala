package ai.starlake.job.transform

import java.util.Locale

/** Whether the rollback of a JDBC write undoes it, which the expectations checked before its commit
  * need (#1976). A statement that commits the transaction on its own, as `TRUNCATE` on MySQL or a
  * DDL statement on Snowflake, commits every statement that precedes it.
  *
  * On an engine whose statements may commit on their own, the SQL that the project writes (presql,
  * postsql, and the SQL of a task whose write strategy does not generate it) is not analysed: such
  * a task checks after its write. The statements of the write, which its write strategy generates,
  * are read with the quote and comment rules of the engine. None of them may commit on its own,
  * except, before the first statement that stays in the transaction:
  *   - the creation of a temporary table, which commits only what precedes the write;
  *   - on a first run, the `CREATE TABLE` of the new table and the `ALTER TABLE ... ADD` statements
  *     that complete it, since a failed check drops the table.
  *
  * On an engine whose statements stay in the transaction, as PostgreSQL and DuckDB, only the
  * statements that end it (`COMMIT`, `ROLLBACK`...) commit: none may be in the write or in its
  * postsql.
  */
object ImplicitCommits {

  /** The write can check its expectations before its commit
    *
    * @param dropsCreatedTable
    *   the run created the table, and its creation committed: a failed check drops the table
    * @param presqlCommitted
    *   the presql statements of the task commit before the write: a rollback leaves them
    * @param altersCommitted
    *   the ALTER statements that align the schema of the table commit before the write: a rollback
    *   leaves the new schema
    */
  final case class Checkable(
    dropsCreatedTable: Boolean,
    presqlCommitted: Boolean,
    altersCommitted: Boolean
  ) {
    private[ImplicitCommits] def or(other: Checkable): Checkable =
      Checkable(
        dropsCreatedTable || other.dropsCreatedTable,
        presqlCommitted || other.presqlCommitted,
        altersCommitted || other.altersCommitted
      )
  }

  /** How an engine reads quotes and comments
    *
    * @param backslashEscapes
    *   a backslash escapes the next character of a string
    * @param escapeStrings
    *   a backslash escapes the next character of an `E'...'` string, as on PostgreSQL
    * @param doubleQuotedStrings
    *   `"..."` is a string, whose backslashes escape, as on MySQL
    * @param backtickQuotes
    *   `` `...` `` is an identifier, as on MySQL
    * @param bracketQuotes
    *   `[...]` is an identifier, as on SQL Server
    * @param dollarQuotes
    *   `$$...$$` is a string
    * @param dollarTags
    *   `$tag$...$tag$` is a string, as on PostgreSQL
    * @param hashComments
    *   `#` starts a comment, as on MySQL
    * @param slashComments
    *   `//` starts a comment, as on Snowflake and H2
    * @param dashNeedsSpace
    *   `--` starts a comment only before a space, as on MySQL
    * @param crEndsComments
    *   a carriage return ends a line comment, as on PostgreSQL
    * @param nestedComments
    *   block comments nest, as on PostgreSQL and SQL Server
    * @param executableComments
    *   the engine runs the content of `/*! ... */`, as MySQL does
    */
  private final case class Dialect(
    name: String,
    backslashEscapes: Boolean = false,
    escapeStrings: Boolean = false,
    doubleQuotedStrings: Boolean = false,
    backtickQuotes: Boolean = false,
    bracketQuotes: Boolean = false,
    dollarQuotes: Boolean = false,
    dollarTags: Boolean = false,
    hashComments: Boolean = false,
    slashComments: Boolean = false,
    dashNeedsSpace: Boolean = false,
    crEndsComments: Boolean = false,
    nestedComments: Boolean = false,
    executableComments: Boolean = false
  )

  private val PostgreSQL = Dialect(
    "PostgreSQL",
    escapeStrings = true,
    dollarQuotes = true,
    dollarTags = true,
    crEndsComments = true,
    nestedComments = true
  )

  /** The readings of each engine: its variants and modes */
  private val EngineDialects: Map[String, List[Dialect]] = {
    val redshift = PostgreSQL.copy(name = "Redshift", backslashEscapes = true)
    val mysql = Dialect(
      "MySQL",
      backslashEscapes = true,
      doubleQuotedStrings = true,
      backtickQuotes = true,
      hashComments = true,
      dashNeedsSpace = true,
      executableComments = true
    )
    val mysqlDialects = List(
      mysql,
      mysql.copy(name = "MySQL with NO_BACKSLASH_ESCAPES", backslashEscapes = false),
      mysql.copy(name = "MySQL with ANSI_QUOTES", doubleQuotedStrings = false)
    )
    val snowflake =
      Dialect("Snowflake", backslashEscapes = true, slashComments = true, dollarQuotes = true)
    val sqlServer = Dialect("SQL Server", bracketQuotes = true, nestedComments = true)
    val h2 = Dialect("H2", slashComments = true, dollarQuotes = true)
    Map(
      // DuckDB parses SQL with the parser of PostgreSQL
      "postgresql" -> List(
        PostgreSQL,
        PostgreSQL
          .copy(name = "PostgreSQL without standard_conforming_strings", backslashEscapes = true)
      ),
      "duckdb" -> List(PostgreSQL.copy(name = "DuckDB")),
      // A PostgreSQL connection taken for Redshift is read as PostgreSQL too
      "redshift"  -> List(redshift, redshift.copy(nestedComments = false), PostgreSQL),
      "mysql"     -> mysqlDialects,
      "snowflake" -> List(snowflake, snowflake.copy(crEndsComments = true)),
      "sqlserver" -> List(sqlServer, sqlServer.copy(crEndsComments = true)),
      "h2" -> List(
        h2,
        h2.copy(nestedComments = true, crEndsComments = true),
        h2.copy(name = "H2 in a compatibility mode", bracketQuotes = true, backtickQuotes = true)
      )
    )
  }

  /** The ways the engine reads SQL text, those of every engine when it is not known */
  private def dialectsOf(engine: String): List[Dialect] =
    EngineDialects.getOrElse(engine, EngineDialects.values.flatten.toList.distinct)

  /** How the engine folds the case of a name that is not quoted: none when its rules vary */
  private def foldOf(engine: String): Option[String => String] =
    engine match {
      case "snowflake" | "h2"                   => Some(_.toUpperCase(Locale.ROOT))
      case "postgresql" | "duckdb" | "redshift" => Some(_.toLowerCase(Locale.ROOT))
      case _                                    => None
    }

  /** A statement of SQL text, without its comments, its whitespace collapsed */
  private final case class Statement(text: String) {
    val normalized: String = text.toUpperCase(Locale.ROOT)
  }

  /** The statements that end a transaction on every engine */
  private val TransactionControl =
    List("COMMIT", "ROLLBACK", "ABORT", "BEGIN", "START TRANSACTION", "END", "PREPARE TRANSACTION")

  /** A SET statement that turns autocommit on commits the transaction, on MySQL among others */
  private val AutocommitSetting = """SET\b.*\bAUTOCOMMIT\b.*""".r

  private val TemporaryTable =
    """(?i)CREATE (OR REPLACE )?((LOCAL|GLOBAL) )?TEMP(ORARY)? TABLE .*""".r

  /** The statements that create a new table, or complete it, followed by its name */
  private val CreateTable = """(?i)CREATE (?:TRANSIENT |HYBRID )?TABLE """.r
  private val AlterTable = """(?i)ALTER TABLE """.r

  private val DollarTag = """\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$""".r

  /** The JDBC escape of a procedure call: `{call p()}` or `{? = call f()}` */
  private val CallEscape = """^\{\s*(\?\s*=\s*)?""".r

  /** @return
    *   the statements of the SQL text, split on its semicolons outside quotes and comments, or None
    *   when the dialect finds a quote or a comment that does not end
    */
  private def statements(sql: String, dialect: Dialect): Option[List[Statement]] = {
    val result = List.newBuilder[Statement]
    val current = new StringBuilder
    val length = sql.length
    var index = 0
    var executableComment = false
    var readable = true
    def at(offset: Int): Char =
      if (index + offset < length) sql.charAt(index + offset) else '\u0000'
    def flush(): Unit = {
      val text = CallEscape.replaceFirstIn(current.toString.trim.replaceAll("\\s+", " "), "")
      if (text.nonEmpty) result += Statement(text)
      current.clear()
    }
    def identifierPart(char: Char): Boolean =
      Character.isLetterOrDigit(char) || char == '_' || char == '$'
    def lineComment: Boolean =
      at(0) == '-' && at(1) == '-' &&
      (!dialect.dashNeedsSpace || at(2).isWhitespace || at(2) == '\u0000') ||
      dialect.hashComments && at(0) == '#' ||
      dialect.slashComments && at(0) == '/' && at(1) == '/'
    def endsComment(char: Char): Boolean = char == '\n' || dialect.crEndsComments && char == '\r'

    /** Appends the quoted text that starts at the index, up to its closing character */
    def quoted(close: Char, escapes: Boolean): Unit = {
      var end = index + 1
      var closed = false
      while (!closed && end < length) {
        val next = sql.charAt(end)
        if (escapes && next == '\\') end += 2
        else if (next == close) {
          // A doubled closing character stands for itself
          if (end + 1 < length && sql.charAt(end + 1) == close) end += 2
          else {
            closed = true
            end += 1
          }
        } else end += 1
      }
      if (closed) {
        current.append(sql.substring(index, end))
        index = end
      } else readable = false
    }
    while (readable && index < length) {
      val char = sql.charAt(index)
      val previous = if (index > 0) sql.charAt(index - 1) else ' '
      lazy val dollarTag =
        if (dialect.dollarQuotes && char == '$' && !identifierPart(previous)) {
          if (at(1) == '$') Some("$$")
          else if (dialect.dollarTags) {
            val matcher = DollarTag.pattern.matcher(sql).region(index, length)
            if (matcher.lookingAt()) Some(matcher.group()) else None
          } else None
        } else None
      if (char == '\'') {
        // E'...' on PostgreSQL
        val escapeString = dialect.escapeStrings && (previous == 'E' || previous == 'e') &&
          (index < 2 || !identifierPart(sql.charAt(index - 2)))
        quoted('\'', dialect.backslashEscapes || escapeString)
      } else if (char == '"') quoted('"', dialect.doubleQuotedStrings && dialect.backslashEscapes)
      else if (dialect.backtickQuotes && char == '`') quoted('`', escapes = false)
      else if (dialect.bracketQuotes && char == '[') quoted(']', escapes = false)
      else if (dollarTag.isDefined) {
        val tag = dollarTag.get
        val end = sql.indexOf(tag, index + tag.length)
        if (end < 0) readable = false
        else {
          current.append(sql.substring(index, end + tag.length))
          index = end + tag.length
        }
      } else if (lineComment) {
        while (index < length && !endsComment(sql.charAt(index))) index += 1
      } else if (dialect.executableComments && char == '/' && at(1) == '*' && at(2) == '!') {
        // MySQL runs the content of an executable comment: only its marker is removed
        index += 3
        while (index < length && sql.charAt(index).isDigit) index += 1
        executableComment = true
        current.append(' ')
      } else if (char == '/' && at(1) == '*') {
        var depth = 1
        var end = index + 2
        while (depth > 0 && end < length) {
          if (sql.startsWith("*/", end)) {
            depth -= 1
            end += 2
          } else if (dialect.nestedComments && sql.startsWith("/*", end)) {
            depth += 1
            end += 2
          } else end += 1
        }
        if (depth > 0) readable = false
        else {
          index = end
          current.append(' ')
        }
      } else if (char == '*' && at(1) == '/' && executableComment) {
        executableComment = false
        index += 2
        current.append(' ')
      } else if (char == ';') {
        flush()
        index += 1
      } else {
        current.append(char)
        index += 1
      }
    }
    if (readable && !executableComment) {
      flush()
      Some(result.result())
    } else None
  }

  /** @return
    *   the statements of the SQL texts, or the first text that the dialect cannot read
    */
  private def statementsOf(sqls: List[String], dialect: Dialect): Either[String, List[Statement]] =
    sqls.foldLeft[Either[String, List[Statement]]](Right(Nil)) { (read, sql) =>
      read.flatMap { previous =>
        statements(sql, dialect).map(previous ++ _).toRight(sql)
      }
    }

  private def keywordsOf(implicitCommits: List[String]): List[String] =
    (implicitCommits ++ TransactionControl)
      .map(_.trim.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT))
      .filter(_.nonEmpty)

  private def commits(statement: Statement, keywords: List[String]): Boolean = {
    val text = statement.normalized
    AutocommitSetting.pattern.matcher(text).matches() ||
    keywords.exists { keyword =>
      text == keyword || text.startsWith(keyword) && {
        val next = text.charAt(keyword.length)
        !Character.isLetterOrDigit(next) && next != '_'
      }
    }
  }

  /** A part of a qualified name, quoted or not */
  private final case class NamePart(text: String, quoted: Boolean)

  /** @return
    *   the parts of the qualified name that starts the text, and the text that follows it
    */
  private def qualifiedName(text: String): Option[(List[NamePart], String)] = {
    val parts = List.newBuilder[NamePart]
    var index = 0
    var more = true
    var valid = true
    while (more && valid) {
      val close = text.lift(index) match {
        case Some('"') => Some('"')
        case Some('`') => Some('`')
        case Some('[') => Some(']')
        case _         => None
      }
      close match {
        case Some(closing) =>
          val end = text.indexOf(closing, index + 1)
          if (end < 0) valid = false
          else {
            parts += NamePart(text.substring(index + 1, end), quoted = true)
            index = end + 1
          }
        case None =>
          val start = index
          while (
            index < text.length &&
            (Character.isLetterOrDigit(text.charAt(index)) || "_$@#".contains(text.charAt(index)))
          ) index += 1
          if (index == start) valid = false
          else parts += NamePart(text.substring(start, index), quoted = false)
      }
      if (valid && text.lift(index).contains('.')) index += 1 else more = false
    }
    if (valid) Some((parts.result(), text.substring(index))) else None
  }

  /** @return
    *   true when the name is the table: named with its schema, without more parts than the table,
    *   its parts the same once the engine folds the case of those that are not quoted. An engine
    *   whose folding is not known compares them as they are written.
    */
  private def sameTable(name: List[NamePart], table: String, engine: String): Boolean = {
    val fold = foldOf(engine).getOrElse((text: String) => text)
    def folded(part: NamePart): String = if (part.quoted) part.text else fold(part.text)
    qualifiedName(table).exists { case (tableParts, rest) =>
      rest.isEmpty && name.size >= 2 && name.size <= tableParts.size &&
      name.reverse.zip(tableParts.reverse).forall { case (part, tablePart) =>
        folded(part) == folded(tablePart)
      }
    }
  }

  private def named(
    prefix: scala.util.matching.Regex,
    statement: Statement,
    table: String,
    engine: String
  )(
    rest: String => Boolean
  ): Boolean =
    prefix.findPrefixMatchOf(statement.text).exists { matched =>
      qualifiedName(statement.text.substring(matched.end)).exists { case (name, after) =>
        sameTable(name, table, engine) && rest(after)
      }
    }

  /** @return
    *   true when the statement creates the new table with a `CREATE TABLE`, which fails on an
    *   existing table
    */
  private def createsNewTable(statement: Statement, table: String, engine: String): Boolean =
    named(CreateTable, statement, table, engine) { after =>
      after.isEmpty || after.startsWith(" ") || after.startsWith("(")
    }

  /** @return
    *   true when the statement adds columns to the new table, and does nothing else
    */
  private def addsToNewTable(statement: Statement, table: String, engine: String): Boolean =
    named(AlterTable, statement, table, engine) { after =>
      after.toUpperCase(Locale.ROOT).startsWith(" ADD ") && !changesMore(after)
    }

  /** @return
    *   true when an ALTER statement holds a comma outside parentheses and strings: one more change
    */
  private def changesMore(alteration: String): Boolean = {
    val withoutStrings = alteration.replaceAll("'(?:[^']|'')*'", "''")
    var depth = 0
    withoutStrings.exists {
      case '(' => depth += 1; false
      case ')' => depth -= 1; false
      case ',' => depth == 0
      case _   => false
    }
  }

  /** The beginning of a statement, without what a string may hold */
  private def abbreviated(text: String): String = {
    val beginning = text.trim.takeWhile(char => char != '\'' && char != '"' && char != '$')
    if (beginning.length > 60) beginning.take(60).trim + "..."
    else if (beginning.length < text.trim.length) beginning.trim + "..."
    else beginning.trim
  }

  /** @param implicitCommits
    *   the leading keywords of the statements that commit on their own on the engine
    * @param engine
    *   the engine, whose rules read the SQL text; those of every engine when empty
    * @return
    *   true when one of the statements of the SQL text commits the transaction on its own, or when
    *   the text cannot be read
    */
  def commitsOnItsOwn(sql: String, implicitCommits: List[String], engine: String = ""): Boolean = {
    val keywords = keywordsOf(implicitCommits)
    dialectsOf(engine).exists(statements(sql, _).forall(_.exists(commits(_, keywords))))
  }

  /** @param engine
    *   the name of the engine, whose rules read the SQL text
    * @param implicitCommits
    *   the statements that commit on their own on the engine, None when it does not declare them
    * @param autocommitBeforeMain
    *   true when the presql and ALTER statements run before the transaction, as on DuckDB
    * @param presql
    *   the presql statements of the task
    * @param beforeMain
    *   the statements that run between the presql and the main ones: the ALTER statements that
    *   align the schema of the table
    * @param main
    *   the statements of the write
    * @param postsql
    *   the postsql statements of the task, which run before the check
    * @param table
    *   the table of the write, with its schema
    * @param createsTable
    *   true when the main statements create the table
    * @param createdBeforeWrite
    *   true when the run created the table before its write, and committed it: the empty table that
    *   the presql statements of a first run read
    * @param generated
    *   false when the main statements are the SQL of the task, which its write strategy does not
    *   generate (`parseSQL: false`)
    * @return
    *   why the rollback would not undo the write, or how it undoes it
    */
  def plan(
    engine: String,
    implicitCommits: Option[List[String]],
    autocommitBeforeMain: Boolean,
    presql: List[String],
    beforeMain: List[String],
    main: List[String],
    postsql: List[String],
    table: String,
    createsTable: Boolean,
    createdBeforeWrite: Boolean = false,
    generated: Boolean = true
  ): Either[String, Checkable] =
    implicitCommits match {
      case None =>
        Left(
          s"the engine $engine does not declare the statements that commit on their own " +
          s"(jdbcEngines.$engine.implicitCommits)"
        )
      case Some(declared) if declared.nonEmpty && !generated =>
        Left(
          s"its SQL, which its write strategy does not generate (parseSQL: false), is not analysed " +
          s"on $engine, whose statements may commit on their own"
        )
      case Some(declared) =>
        val keywords = keywordsOf(declared)
        val plans = dialectsOf(engine).map { dialect =>
          val read = for {
            pre    <- statementsOf(presql, dialect)
            alters <- statementsOf(beforeMain, dialect)
            write  <- statementsOf(main, dialect)
            post   <- statementsOf(postsql, dialect)
          } yield (pre, alters, write, post)
          read match {
            case Left(unread) =>
              Left(
                s"the entry that starts with `${abbreviated(unread)}` holds a quote or a comment " +
                s"that does not end, as ${dialect.name} reads it"
              )
            case Right((pre, _, _, post)) if declared.nonEmpty && (pre.nonEmpty || post.nonEmpty) =>
              Left(
                s"its presql or postsql statements are not analysed on $engine, whose statements " +
                "may commit on their own"
              )
            case Right((pre, alters, write, post)) =>
              planOf(engine, keywords, autocommitBeforeMain, pre, alters, write, post, table)(
                createsTable,
                createdBeforeWrite
              )
          }
        }
        plans.collectFirst { case Left(reason) => Left(reason) }.getOrElse {
          Right(plans.collect { case Right(checkable) => checkable }.reduce(_ or _))
        }
    }

  private def planOf(
    engine: String,
    keywords: List[String],
    autocommitBeforeMain: Boolean,
    presql: List[Statement],
    alters: List[Statement],
    write: List[Statement],
    postsql: List[Statement],
    table: String
  )(createsTable: Boolean, createdBeforeWrite: Boolean): Either[String, Checkable] = {
    def committing(statement: Statement): Boolean = commits(statement, keywords)
    val firstInTransaction = write.indexWhere(!committing(_))
    def beforeWrite(index: Int): Boolean = firstInTransaction < 0 || index < firstInTransaction
    def allowed(statement: Statement, index: Int): Boolean =
      beforeWrite(index) && (
        TemporaryTable.pattern.matcher(statement.text).matches() ||
        // A table that the run created is dropped when the check fails
        createsTable && (
          createsNewTable(statement, table, engine) || addsToNewTable(statement, table, engine)
        )
      )
    val committingWrite =
      write.zipWithIndex.collectFirst {
        case (statement, index) if committing(statement) && !allowed(statement, index) =>
          s"its statement `${abbreviated(statement.text)}` commits on its own on $engine"
      }
    val committingPostsql =
      postsql.find(committing).map { statement =>
        s"its postsql statement `${abbreviated(statement.text)}` commits on its own on $engine"
      }
    committingWrite.orElse(committingPostsql) match {
      case Some(reason) => Left(reason)
      case None =>
        Right(
          Checkable(
            // Only a CREATE TABLE that committed, and that fails on an existing table, tells that
            // the run created the table
            dropsCreatedTable = createdBeforeWrite ||
              createsTable && write.exists { statement =>
                committing(statement) && createsNewTable(statement, table, engine)
              },
            presqlCommitted = presql.nonEmpty &&
              (autocommitBeforeMain || (presql ++ alters ++ write).exists(committing)),
            altersCommitted =
              alters.nonEmpty && (autocommitBeforeMain || (alters ++ write).exists(committing))
          )
        )
    }
  }
}
