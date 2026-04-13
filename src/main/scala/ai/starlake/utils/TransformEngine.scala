package ai.starlake.utils

import java.security.SecureRandom

/** Several encryption methods used in privacy management
  */
object TransformEngine {

  def algo(alg: String, data: String): String = {
    val m = java.security.MessageDigest.getInstance(alg)
    val b = data.getBytes("UTF-8")
    m.update(b, 0, b.length)
    val bytes: Array[Byte] = m.digest()
    bytes.map("%02x" format _).mkString
  }

  def parse(maskingAlgo: String): (String, List[String]) = {
    def parseParams(params: List[String]): List[String] =
      params.map { param =>
        if (param.startsWith("\"") && param.endsWith("\""))
          param.substring(1, param.length - 1)
        else if (param.startsWith("'") && param.endsWith("'"))
          param.substring(1, 2)
        else
          param
      }

    val hasParam = maskingAlgo.indexOf('(')
    if (hasParam > 0) {
      require(maskingAlgo.indexOf(')') > hasParam)
      (
        maskingAlgo.substring(0, hasParam),
        parseParams(
          maskingAlgo.substring(hasParam + 1, maskingAlgo.length - 1).split(',').map(_.trim).toList
        )
      )
    } else {
      (maskingAlgo, Nil)
    }
  }
}

trait TransformEngine extends Serializable {

  /** @param s:
    *   String => Input string to encrypt
    * @param colMap
    *   : Map[String, Option[String]] => Map of all the attributes and their corresponding values
    * @param params:
    *   List[Any] => Parameters passed to the algorithm as defined in the conf file. Parameter
    *   starting with '"' is converted to a string Parameter containing a '.' is converted to a
    *   double Parameter equals to true of false is converted a boolean Anything else is converted
    *   to an int
    * @return
    *   The encrypted string
    */
  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String
}

object Md5 extends TransformEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String =
    TransformEngine.algo("MD5", s)
}

object Sha1 extends TransformEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String =
    TransformEngine.algo("SHA-1", s)
}

object Sha256 extends TransformEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String =
    TransformEngine.algo("SHA-256", s)
}

object Sha512 extends TransformEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String =
    TransformEngine.algo("SHA-512", s)
}

object Hide extends TransformEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String = {
    if (params.isEmpty)
      ""
    else {
      require(params.length == 2)
      val c = params.head
      val i = params(1).toInt
      c * i
    }
  }
}

object No extends TransformEngine {
  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String = s
}

object Initials extends TransformEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String = {
    s.split("\\s+").map(_.substring(0, 1)).mkString("", ".", ".")
  }
}

object Email extends TransformEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String = {
    require(params.length == 1)
    val split = s.split('@')
    TransformEngine.algo(params.head.toString, split(0)) + "@" + split(1)
  }
}

trait IP extends TransformEngine {
  def separator: Char

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String = {
    require(params.length == 1)
    crypt(s, params.head.toInt)
  }

  def crypt(s: String, maskBytes: Int): String = {
    val ip = s.split(separator)
    (ip.dropRight(maskBytes) ++ List.fill(maskBytes)(0)).mkString(separator.toString)
  }
}

object IPv4 extends IP {
  override val separator: Char = '.'
}

object IPv6 extends IP {
  override val separator: Char = ':'
}

trait NumericRandomTransform extends TransformEngine {
  val rnd: SecureRandom

  final def gen(low: Double, up: Double): Double = low + (up - low) * rnd.nextDouble()

  def genUnbounded(): Double

  final def crypt(params: List[String]): Double = {
    require(params.length == 2 || params.isEmpty)
    params match {
      case Nil =>
        genUnbounded()
      case lowerBound :: upperBound :: Nil =>
        val low = lowerBound.toDouble
        val up = upperBound.toDouble
        gen(low, up)
      case _ => throw new Exception("Should never happen!")
    }
  }
}

object RandomDouble extends NumericRandomTransform {
  val rnd = new SecureRandom()

  def genUnbounded(): Double = rnd.nextDouble()

  override def crypt(
    s: String,
    colMap: => Map[String, Option[String]],
    params: List[String]
  ): String = {
    crypt(params).toString
  }
}

object RandomLong extends NumericRandomTransform {
  val rnd = new SecureRandom()

  override def genUnbounded(): Double = rnd.nextLong().toDouble

  override def crypt(
    s: String,
    colMap: => Map[String, Option[String]],
    params: List[String]
  ): String =
    (crypt(params) % Long.MaxValue).toLong.toString
}

object RandomInt extends NumericRandomTransform {
  val rnd = new SecureRandom()

  override def genUnbounded(): Double = rnd.nextInt().toDouble

  override def crypt(
    s: String,
    colMap: => Map[String, Option[String]],
    params: List[String]
  ): String =
    (crypt(params) % Int.MaxValue).toInt.toString
}

class ApproxDouble extends TransformEngine {
  val rnd = new SecureRandom()

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String = {
    require(params.length == 1)
    crypt(s.toDouble, params.head.toInt).toString
  }

  def crypt(value: Double, percent: Int): Double = {
    val rndBool = rnd.nextBoolean()
    val distance = (value * percent * rnd.nextDouble()) / 100
    if (rndBool)
      value - distance
    else
      value + distance
  }
}

object ApproxDouble extends ApproxDouble

object ApproxLong extends ApproxDouble {

  override def crypt(
    s: String,
    colMap: => Map[String, Option[String]],
    params: List[String]
  ): String = {
    require(params.length == 1)
    crypt(s.toDouble, params.head.toInt).toLong.toString
  }

}

object Mask extends TransformEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[String]): String = {
    require(params.length == 4)
    val maskingChar = params(0).charAt(0)
    val numberOfChars = params(1).toInt
    val leftSide = params(2).toInt
    val rightSide = params(3).toInt
    crypt(s, maskingChar, numberOfChars, leftSide, rightSide)
  }

  def crypt(
    s: String,
    maskingChar: Char,
    numberOfChars: Int,
    leftSide: Int,
    rightSide: Int
  ): String = {
    s match {
      case input if input.length <= leftSide =>
        "%s%s".format(input, maskingChar.toString * numberOfChars)
      case _ =>
        "%s%s%s".format(
          s.take(leftSide),
          maskingChar.toString * numberOfChars,
          s.takeRight(rightSide)
        )
    }
  }
}
