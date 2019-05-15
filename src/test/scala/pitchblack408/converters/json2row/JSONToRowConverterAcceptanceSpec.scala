package pitchblack408.converters.json2row

import java.io.{File, FileOutputStream}
import java.nio.charset.Charset

import com.github.agourlay.json_converters.Json2Row
import org.apache.commons.io.FileUtils
import org.scalatest.{Matchers, WordSpec}

class JSONToRowConverterAcceptanceSpec extends WordSpec with Matchers {


  "The converter" must {
    "transform properly the nominal case" in {
      val inputJSONString :String = """[{"userId": 1,"id": 1,"title": "delectus aut autem","completed": false}"""
      val outputName = "delete.csv"
      val resultOutputStream = new FileOutputStream(outputName)
      Json2Row.convert(inputJSONString, resultOutputStream)



      val resultFile = new File(outputName)
      val resultFileContent = FileUtils.readFileToString(resultFile, Charset.defaultCharset)
      FileUtils.forceDelete(resultFile)

      val referenceResultFile = new File(getClass.getResource("/test-json.csv").getPath)
      resultFileContent shouldEqual FileUtils.readFileToString(referenceResultFile, Charset.defaultCharset)
    }
  }
}