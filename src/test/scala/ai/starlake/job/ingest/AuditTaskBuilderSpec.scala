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

package ai.starlake.job.ingest

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class AuditTaskBuilderSpec extends AnyFlatSpec {

  "escapeLiteral with a custom quote replacement" should
  "replace the quote with the given replacement while leaving the other passes untouched" in {
    val escaped = AuditTaskBuilder.escapeLiteral("it's a 'test'", "\"")
    escaped shouldBe "it\"s a \"test\""
  }

  it should "still drop every brace, including one that would synthesize a delimiter" in {
    // pairwise stripping of {{ }} / {% %} / {# #} is not a fixpoint: it can join what is left
    // of two different pairs into a live delimiter. Removing every brace, one at a time,
    // cannot do that, whatever quoteReplacement is.
    val escaped = AuditTaskBuilder.escapeLiteral("{}}%ANUM%#}}", "\"")
    escaped shouldBe "%ANUM%#"
    escaped should not include "{"
    escaped should not include "}"
  }

  it should "still neutralize backslashes and newlines" in {
    val escaped = AuditTaskBuilder.escapeLiteral("a\\b\nc", "\"")
    escaped shouldBe "a-b c"
  }

  it should "insert the replacement literally, not as a regex replacement string" in {
    // replaceAll's second argument treats $ and \ as metacharacters; Matcher.quoteReplacement
    // makes the parameter literal so a $ bearing replacement cannot throw from the audit path.
    AuditTaskBuilder.escapeLiteral("a'b", "$1") shouldBe "a$1b"
    AuditTaskBuilder.escapeLiteral("a'b", "$") shouldBe "a$b"
  }

  "escapeLiteral called with one argument" should
  "produce exactly what it produced before the second parameter was added" in {
    // pinned example: quote becomes a dash, backslash becomes a dash, newline becomes a
    // space, every brace is gone.
    AuditTaskBuilder.escapeLiteral("it's a {{value}} with a \\ and a\nnewline") shouldBe
    "it-s a value with a - and a newline"
  }
}
