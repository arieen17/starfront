/*
 * Copyright 2020 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.ucrstar

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import edu.ucr.cs.bdlab.beast.io.{CSVFeatureReader, FeatureReader, SpatialReaderMetadata}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, LineRecordReader}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptID}
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.types._

@SpatialReaderMetadata(description = "Parses Porto taxi data", shortName = "porto", extension = ".txt")
class PortoReader extends FeatureReader {

  /** An underlying reader for the text file */
  val lineReader = new LineRecordReader()

  /** The returned feature */
  var feature: Feature = _

  /** Cache the file path to report errors */
  var filePath: Path = _

  var geometryFactory = FeatureReader.DefaultGeometryFactory

  override def initialize(inputSplit: InputSplit, opts: BeastOptions): Unit = {
    lineReader.initialize(inputSplit, new TaskAttemptContextImpl(opts.loadIntoHadoopConf(null), new TaskAttemptID()))
    this.filePath = inputSplit.asInstanceOf[FileSplit].getPath
  }

  override def nextKeyValue(): Boolean = {
    try {
      if (!lineReader.nextKeyValue) return false
      if (lineReader.getCurrentKey.get() == 0) {
        // Skip first line
        if (!lineReader.nextKeyValue())
          return false
      }
      val textLine = lineReader.getCurrentValue

      val values = new Array[Any](PortoReader.Schema.length)
      for (i <- values.indices)
        values(i) = CSVFeatureReader.deleteAttribute(textLine, ',', 0, "\"\"")
      values(0) = values(0).asInstanceOf[String].toLong
      values(4) = values(4).asInstanceOf[String].toInt
      values(5) = new java.sql.Timestamp(values(5).asInstanceOf[String].toLong)
      values(7) = values(7).asInstanceOf[String].equalsIgnoreCase("true")
      // Parse the geometry string in a poor way
      val strcoords: Array[String] = values(8).asInstanceOf[String].stripPrefix("[").stripSuffix("]").split(',')
        .map(x => x.stripPrefix("[").stripSuffix("]"))
      values(8) = if (strcoords.length == 1 && strcoords(0).isEmpty) {
        // Special case. Empty list of coordinates
        geometryFactory.createMultiPoint()
      } else {
        assert(strcoords.length % 2 == 0, "Must have even number of coordinates")
        val numPoints = strcoords.length / 2
        val coordinateSequence = geometryFactory.getCoordinateSequenceFactory.create(numPoints, 2)
        for (iPoint <- 0 until numPoints) {
          coordinateSequence.setOrdinate(iPoint, 0, strcoords(iPoint * 2).toDouble)
          coordinateSequence.setOrdinate(iPoint, 1, strcoords(iPoint * 2 + 1).toDouble)
        }
        geometryFactory.createMultiPoint(coordinateSequence)
      }
      feature = new Feature(values, PortoReader.Schema)
      true
    } catch {
      case e: Exception => throw new RuntimeException(s"Error parsing file '$filePath' at position ${lineReader.getCurrentKey}", e)
    }
  }

  override def getCurrentValue: IFeature = feature

  override def getProgress: Float = lineReader.getProgress

  override def close(): Unit = lineReader.close()
}

object PortoReader {

  val Schema = StructType(Array(
    StructField("TRIP_ID", LongType),
    StructField("CALL_TYPE", StringType),
    StructField("ORIGIN_CALL", StringType),
    StructField("ORIGIN_STAND", StringType),
    StructField("TAXI_ID", IntegerType),
    StructField("TIMESTAMP", TimestampType),
    StructField("DAY_TYPE", StringType),
    StructField("MISSING_DATA", BooleanType),
    StructField("GPS_POINT", GeometryDataType),
  ))
}