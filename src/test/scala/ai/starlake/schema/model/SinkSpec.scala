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

package ai.starlake.schema.model

import ai.starlake.TestHelper

class SinkSpec extends TestHelper {
  new WithSettings() {
    "parsing BQ sink" should "succeed" in {
      mapper.readValue(
        """
          |connectionRef: "sink"
          |partition: ["timestamp"]
          |""".stripMargin,
        classOf[BigQuerySink]
      ) shouldBe BigQuerySink(connectionRef = Some("sink"), partition = Some(List("timestamp")))

    }

    "parsing FS sink" should "succeed" in {
      mapper.readValue(
        """
          |connectionRef: "sink"
          |extension: "extension"
          |options:
          |  anyOption: "true"
          |""".stripMargin,
        classOf[FsSink]
      ) shouldBe FsSink(
        connectionRef = Some("sink"),
        extension = Some("extension"),
        options = Some(Map("anyOption" -> "true"))
      )
    }

    "parsing ES sink" should "succeed" in {
      mapper.readValue(
        """
          |connectionRef: "sink"
          |timestamp: "timestamp"
          |options:
          |  anyOption: "true"
          |""".stripMargin,
        classOf[EsSink]
      ) shouldBe EsSink(
        connectionRef = Some("sink"),
        timestamp = Some("timestamp"),
        options = Some(Map("anyOption" -> "true"))
      )
    }

    "The BigQuery and JDBC sinks of a task" should "carry its options, not those of the connection (#1877)" in {
      val options = Some(Map("my_option" -> "hello"))
      AllSinks(connectionRef = Some("bigquery"), options = options).getSink() shouldBe BigQuerySink(
        connectionRef = Some("bigquery"),
        options = options
      )
      AllSinks(connectionRef = Some("test-pg"), options = options).getSink() shouldBe JdbcSink(
        connectionRef = Some("test-pg"),
        options = options
      )
      // The write strategy templates receive them, not as table options of the engine
      val variables = AllSinks(connectionRef = Some("test-pg"), options = options)
        .getSink()
        .asMap(settings.appConfig.jdbcEngines("spark"))
      variables.get("sinkOptions") shouldBe Some(java.util.Map.of("my_option", "hello"))
      variables.get("sinkTableOptionsClause") shouldBe Some("")
      AllSinks(connectionRef = Some("bigquery"), options = options)
        .getSink()
        .asMap(settings.appConfig.jdbcEngines("bigquery"))
        .get("sinkTableOptionsClause") shouldBe Some("")
      // File sinks still render them
      AllSinks(connectionRef = Some("spark"), options = options)
        .getSink()
        .asMap(settings.appConfig.jdbcEngines("spark"))
        .get("sinkTableOptionsClause") shouldBe Some("OPTIONS('my_option'='hello')")
      // Never the URL or the credentials of the connection
      AllSinks(connectionRef = Some("bigquery")).getSink().toAllSinks().options shouldBe None
      AllSinks(connectionRef = Some("test-pg")).getSink().toAllSinks().options shouldBe None
      // A file sink still takes the options of its connection
      AllSinks(connectionRef = Some("spark")).getSink().toAllSinks().options shouldBe Some(
        settings.appConfig.connections("spark").options
      )
    }

    "parsing JDBC sink" should "succeed" in {
      mapper.readValue(
        """
          |connectionRef: "sink"
          |""".stripMargin,
        classOf[JdbcSink]
      ) shouldBe JdbcSink(connectionRef = Some("sink"))
    }

    "parsing KAFKA sink" should "fail" in {
      assertThrows[Exception] {
        mapper.readValue(
          """
          |name: "sink"
          |type: "KAFKA"
          |options:
          |  anyOption: "true"
          |""".stripMargin,
          classOf[Sink]
        )
      }
    }
  }
}
