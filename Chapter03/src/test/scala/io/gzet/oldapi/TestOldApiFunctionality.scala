/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gzet.oldapi

import java.io.{BufferedReader, FileReader, File}
import java.nio.file.{Files, Paths}

import io.gzet.test.SparkFunSuite
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{IndexedRecord, GenericDatumReader}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.io.gzet.gdelt.gkg.v2.SourceCollectionIdentifier
import org.io.gzet.gdelt.gkg.v21.Specification

import scala.io.Source

class TestOldApiFunctionality extends SparkFunSuite {

  val inputFile = new File(getClass.getResource("/20160101020000.gkg.csv").getFile)
  val avroFile = new File("target/20160101020000.gkg.avdl.avro")
  val parquetFile = new File("target/20160101020000.gkg.parquet")

  localTest("Test createAvro creates valid avro file") { sc =>

    Files.deleteIfExists(Paths.get(avroFile.getPath))
    Files.createFile(Paths.get(avroFile.getPath))

    val userDatumWriter = new SpecificDatumWriter[Specification](classOf[Specification])
    val dataFileWriter = new DataFileWriter[Specification](userDatumWriter)

    dataFileWriter.create(Specification.getClassSchema, avroFile)

    for(line <- Source.fromFile(inputFile).getLines)
      dataFileWriter.append(CreateAvroWithIDL.generateAvro(line))

    dataFileWriter.close()

    val br = new BufferedReader(new FileReader(avroFile))

    assertResult(true) (Files.exists(Paths.get(avroFile.getPath)))
    assertResult(true) (br.readLine().contains("avro.schema"))

    br.close
  }

  localTest("Test createParquet creates valid Parquet file") { sc =>

    Files.deleteIfExists(Paths.get(parquetFile.getPath))

    val schema = Specification.getClassSchema
    val reader =  new GenericDatumReader[IndexedRecord](schema)
    val dataFileReader = DataFileReader.openReader(avroFile, reader)
    val parquetWriter = new AvroParquetWriter[IndexedRecord](new Path(parquetFile.toURI), schema)

    while(dataFileReader.hasNext)  {
      parquetWriter.write(dataFileReader.next())
    }

    dataFileReader.close()
    parquetWriter.close()

    val br = new BufferedReader(new FileReader(parquetFile))

    assertResult(true) (Files.exists(Paths.get(parquetFile.getPath)))
    assertResult(true) (br.readLine().contains("PAR1"))

    br.close
  }
}
