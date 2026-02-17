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

import java.util.Calendar
import edu.ucr.cs.bdlab.beast.io.FeatureReader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PortoReaderTest extends FunSuite with ScalaSparkTest {

  test("ReadSample") {
    val inputPath = new Path(makeFileCopy("/porto_sample.txt").getPath)
    val portoReader: FeatureReader = new PortoReader()
    val fileSystem = inputPath.getFileSystem(sparkContext.hadoopConfiguration)
    portoReader.initialize(new FileSplit(inputPath, 0, fileSystem.getFileStatus(inputPath).getLen, null),
      new BeastOptions(sparkContext.getConf))
    var count = 0
    while (portoReader.nextKeyValue()) {
      count += 1
      val feature = portoReader.getCurrentValue
      assert(feature.getGeometry.getGeometryType == "MultiPoint")
      if (count == 2) {
        // Check the attribute values for the second element
        assert(feature.getAs[Long]("TRIP_ID") == 1372637303620000596L)
        assert(feature.getAs[String]("CALL_TYPE") == "B")
        assert(feature.getAs[String]("ORIGIN_CALL") == "")
        assert(feature.getAs[String]("ORIGIN_STAND") == "7")
        assert(feature.getAs[Int]("TAXI_ID") == 20000596)
        assert(feature.getAs[java.sql.Timestamp]("TIMESTAMP").getTime == 1372637303)
        assert(!feature.getAs[Boolean]("MISSING_DATA"))
      }
    }
    portoReader.close
    assert(count == 6)
  }
}
