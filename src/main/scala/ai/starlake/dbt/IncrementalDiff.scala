package ai.starlake.dbt

/** Merges the two renders of an incremental model, `is_incremental()` false and true, into one SQL
  * text whose differences are `sl_incremental` blocks. A line diff (longest common subsequence)
  * finds the hunks:
  *   - lines only in the incremental render: `{% if sl_incremental %}...{% endif %}`
  *   - lines only in the full render: `{% if not sl_incremental %}...{% endif %}`
  *   - lines replaced: `{% if sl_incremental %}<incremental>{% else %}<full>{% endif %}`
  */
object IncrementalDiff {

  def merge(full: String, incremental: String): String =
    if (full == incremental) full
    else {
      val fullLines = full.split("\n", -1).toVector
      val incLines = incremental.split("\n", -1).toVector
      // the lines common at the start and at the end are kept out of the LCS table, so a long
      // model with a small difference costs memory in the size of the difference only
      val maxCommon = math.min(fullLines.length, incLines.length)
      val prefixLen = fullLines.iterator.zip(incLines.iterator).takeWhile(p => p._1 == p._2).size
      val suffixLen = fullLines.reverseIterator
        .zip(incLines.reverseIterator)
        .takeWhile(p => p._1 == p._2)
        .size
        .min(maxCommon - prefixLen)
      val a = fullLines.slice(prefixLen, fullLines.length - suffixLen)
      val b = incLines.slice(prefixLen, incLines.length - suffixLen)
      // lcs(i)(j): length of the longest common subsequence of a.drop(i) and b.drop(j)
      val lcs = Array.ofDim[Int](a.length + 1, b.length + 1)
      for (i <- a.indices.reverse; j <- b.indices.reverse)
        lcs(i)(j) =
          if (a(i) == b(j)) lcs(i + 1)(j + 1) + 1 else math.max(lcs(i + 1)(j), lcs(i)(j + 1))

      val out = Vector.newBuilder[String]
      out ++= fullLines.take(prefixLen)
      val onlyFull = Vector.newBuilder[String]
      val onlyIncremental = Vector.newBuilder[String]
      def flush(): Unit = {
        val f = onlyFull.result()
        val inc = onlyIncremental.result()
        onlyFull.clear()
        onlyIncremental.clear()
        if (f.nonEmpty && inc.nonEmpty)
          out ++= ("{% if sl_incremental %}" +: inc) ++ ("{% else %}" +: f) :+ "{% endif %}"
        else if (inc.nonEmpty)
          out ++= ("{% if sl_incremental %}" +: inc) :+ "{% endif %}"
        else if (f.nonEmpty)
          out ++= ("{% if not sl_incremental %}" +: f) :+ "{% endif %}"
      }
      var i = 0
      var j = 0
      while (i < a.length || j < b.length) {
        if (i < a.length && j < b.length && a(i) == b(j)) {
          flush()
          out += a(i)
          i += 1
          j += 1
        } else if (j < b.length && (i == a.length || lcs(i)(j + 1) >= lcs(i + 1)(j))) {
          onlyIncremental += b(j)
          j += 1
        } else {
          onlyFull += a(i)
          i += 1
        }
      }
      flush()
      out ++= fullLines.takeRight(suffixLen)
      out.result().mkString("\n")
    }
}
