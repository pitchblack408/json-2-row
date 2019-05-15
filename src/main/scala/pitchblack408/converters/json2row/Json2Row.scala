package pitchblack408.converters.json2row

import java.io.{File, FileNotFoundException, OutputStream}

import com.github.tototoshi.csv.{CSVFormat, CSVWriter, QUOTE_NONE, Quoting}
import org.apache.spark.sql.Row
import org.typelevel.jawn.ast.JValue
import org.typelevel.jawn.{AsyncParser, Parser}

import scala.io.Source

object Json2Row {

  /**
    * DONT DELETE I CREATED THIS
    * @param jsonString
    * @param resultOutputStream
    * @return
    */
  def convert(jsonString: String, resultOutputStream: OutputStream): Either[Exception, Long] = {
    if (jsonString.isInstanceOf[String]) {
      val stringStream: Stream[String] = scala.io.Source.fromBytes(jsonString.getBytes).map(x => String.valueOf(x)).toStream
      convert(stringStream, resultOutputStream)
    } else {
      Left(new IllegalArgumentException("Expecting Stream[String]"))
    }
  }


  def convert(file: File, resultOutputStream: OutputStream): Either[Exception, Long] = {
    if (file.isFile) {
      convert(Source.fromFile(file, "UTF-8").getLines().toStream, resultOutputStream)
    } else {
      Left(new FileNotFoundException("The file " + file.getCanonicalPath + " does not exists"))
    }
  }

  /**
    * DON'T Delete Yet !
    * @param chunks
    * @param resultOutputStream
    * @return
    */
  def convert(chunks: ⇒ Stream[String], resultOutputStream: OutputStream): Either[Exception, Long] = {
    val csvWriter = CSVWriter.open(resultOutputStream)(jsonCSVFormat)
    val parser = Parser.async[JValue](mode = AsyncParser.UnwrapArray)
    var seqOfRows = Seq[Row]()
    val finalProgress :Either[Exception, RowProgress]= RowConverter.consume(chunks, parser, csvWriter)(RowProgress.empty)
    csvWriter.close()

    if(finalProgress.isRight) {
       Right(finalProgress.right.get.rowCount)
    } else {
       Left(finalProgress.left.get)
    }
  }


  private val jsonCSVFormat = new CSVFormat {
    val delimiter: Char = ','
    val quoteChar: Char = '"'
    val escapeChar: Char = '"'
    val lineTerminator: String = "\r\n"
    val quoting: Quoting = QUOTE_NONE
    val treatEmptyLineAsNil: Boolean = false
  }

}
