package io.gzet.oldapi

import java.io.File

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, IndexedRecord}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.io.gzet.gdelt.gkg.v21.Specification

object createParquet {
  def main(args: Array[String]): Unit = {

    val inputFile = new File("/Users/uktpmhallett/Downloads/20160101020000.gkg.avro")
    val outputFile = new Path("file:///Users/uktpmhallett/Downloads/gkg.parquet")

    val schema = Specification.getClassSchema
    val reader =  new GenericDatumReader[IndexedRecord](schema)
    val dataFileReader = DataFileReader.openReader(inputFile, reader)
    val parquetWriter = new AvroParquetWriter[IndexedRecord](outputFile, schema)

    while(dataFileReader.hasNext)  {
      parquetWriter.write(dataFileReader.next())
    }

    dataFileReader.close()
    parquetWriter.close();
  }
}
