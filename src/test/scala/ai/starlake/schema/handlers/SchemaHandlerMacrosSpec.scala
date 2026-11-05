package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import org.apache.hadoop.fs.Path

class SchemaHandlerMacrosSpec extends TestHelper {

  "allMacros" should "read the macro files once per schema handler" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/shared-directory/domain1.sl.yml",
        datasetDomainName = "SHARED1",
        sourceDatasetPathName = ""
      ) {
        cleanMetadata
        val macroFile = new Path(DatasetArea.expectations, "read_once.j2")
        storageHandler.write("first version", macroFile)

        val schemaHandler = settings.schemaHandler(reload = true)
        schemaHandler.allMacros should include("first version")

        storageHandler.write("second version", macroFile)
        schemaHandler.allMacros should include("first version")

        settings.schemaHandler(reload = true).allMacros should include("second version")
        storageHandler.delete(macroFile)
      }
    }
  }
}
