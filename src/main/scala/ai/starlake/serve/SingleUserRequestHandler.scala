package ai.starlake.serve

import ai.starlake.utils.Utils
import better.files.File

import java.io.IOException
import javax.servlet.ServletException
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

class SingleUserRequestHandler extends HttpServlet {
  @throws[ServletException]
  @throws[IOException]
  override protected def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val params = req.getParameter("PARAMS").split(" ")
    val root = Option(req.getParameter("ROOT")).getOrElse(File.temp.pathAsString)
    val env = Option(req.getParameter("ENV"))
    val metadata = Option(req.getParameter("METADATA"))
    val duckDbMode =
      Option(req.getParameter("SL_DUCKDB_MODE"))
        .orElse(Option(req.getParameter("SL_DUAL_MODE")))
        .getOrElse("false")
        .toBoolean
    val gcpProject =
      Option(req.getParameter("SL_DATABASE"))
        .filter(_.nonEmpty)
    val queryIndex = params.indexOf("--query")
    val paramsWithSpaces =
      if (queryIndex > 0) {
        require(queryIndex + 1 < params.length)
        var query = params(queryIndex + 1)
        query = query.replaceAll("__SL__", " ")
        params.dropRight(1) :+ query
      } else
        params
    System.out.println(s"PARAMS=${paramsWithSpaces.toList}")

    System.out.println(s"ROOT=$root")
    System.out.println(s"METADATA=$metadata")
    System.out.println(s"ENV=$env")
    System.out.println(s"SL_DATABASE=$gcpProject")
    System.out.println(s"SL_DUCKDB_MODE=$duckDbMode")
    System.out.println(s"SL_DUAL_MODE=$duckDbMode")
    try {
      val response =
        SingleUserMainServer.run(root, metadata, paramsWithSpaces, env, gcpProject, duckDbMode)
      resp.setStatus(HttpServletResponse.SC_OK)
      resp.getWriter.println(response)
      println(response)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        resp.getWriter.println(Utils.exceptionAsString(e))
    }
  }
  @throws[ServletException]
  @throws[IOException]
  override protected def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val payload = new StringBuilder
    val reader = req.getReader
    try {
      var line: String = reader.readLine
      while (line != null) {
        payload.append(line)
        line = reader.readLine
      }
    } finally {
      if (reader != null)
        reader.close()
    }
    val map = SingleUserMainServer.mapper
      .readValue[Map[String, String]](payload.toString(), classOf[Map[String, String]])
    val params = map.getOrElse("PARAMS", "").split(" ")
    val root = map.getOrElse("ROOT", File.temp.pathAsString)
    val env = map.get("ENV")
    val metadata = map.get("METADATA")
    val duckDbMode =
      map.getOrElse("SL_DUCKDB_MODE", map.getOrElse("SL_DUAL_MODE", "false")).toBoolean
    val gcpProject = map.get("SL_DATABASE").filter(_.nonEmpty)

    System.out.println(s"PARAMS=${params.toList}")
    System.out.println(s"ROOT=$root")
    System.out.println(s"METADATA=$metadata")
    System.out.println(s"ENV=$env")
    System.out.println(s"SL_DATABASE=$gcpProject")
    System.out.println(s"SL_DUCKDB_MODE=$duckDbMode")
    System.out.println(s"SL_DUAL_MODE=$duckDbMode")

    try {
      val response = SingleUserMainServer.run(root, metadata, params, env, gcpProject, duckDbMode)
      resp.setStatus(HttpServletResponse.SC_OK)
      resp.getWriter.println(response)
      println(response)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        resp.getWriter.println(Utils.exceptionAsString(e))
    }
  }
}
