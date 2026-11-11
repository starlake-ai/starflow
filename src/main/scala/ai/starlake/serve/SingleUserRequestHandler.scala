package ai.starlake.serve

import ai.starlake.utils.Utils
import better.files.File

import java.io.IOException
import jakarta.servlet.ServletException
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

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
    val map = RequestParams.parsePayload(payload.toString())
    val root = RequestParams.string(map, "ROOT").getOrElse(File.temp.pathAsString)
    val env = RequestParams.string(map, "ENV")
    val metadata = RequestParams.string(map, "METADATA")
    val duckDbMode =
      RequestParams
        .string(map, "SL_DUCKDB_MODE")
        .orElse(RequestParams.string(map, "SL_DUAL_MODE"))
        .getOrElse("false")
        .toBoolean
    val gcpProject = RequestParams.string(map, "SL_DATABASE").filter(_.nonEmpty)

    System.out.println(s"ROOT=$root")
    System.out.println(s"METADATA=$metadata")
    System.out.println(s"ENV=$env")
    System.out.println(s"SL_DATABASE=$gcpProject")
    System.out.println(s"SL_DUCKDB_MODE=$duckDbMode")
    System.out.println(s"SL_DUAL_MODE=$duckDbMode")

    try {
      val params = RequestParams.args(map)
      System.out.println(s"PARAMS=${params.toList}")
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
