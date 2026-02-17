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
package edu.ucr.cs.bdlab.ucrstar;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.beast.io.FeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialReaderMetadata;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.spark.beast.sql.GeometryDataType$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Reads GeoLife .mplot files as a sequence of points. Similar to the CSVFeatureReader but it skips the first six
 * lines per the GeoLife format.
 */
@SpatialReaderMetadata(
    description = "Parses GeoLife .mplot files",
    shortName = "geolife",
    extension = ".plt",
    filter = "*.plt"
)
public class GeoLifeReader extends FeatureReader {
  private static final Log LOG = LogFactory.getLog(GeoLifeReader.class);

  /**An underlying reader for the text file*/
  protected final LineRecordReader lineReader = new LineRecordReader();

  /**The returned feature*/
  protected Feature feature;

  /**Cache the file path to report errors*/
  protected Path filePath;

  protected GeometryFactory geometryFactory = FeatureReader.DefaultGeometryFactory;

  /**
   * The fixed schema of the GeoLife file
   */
  protected static StructType Schema = new StructType(new StructField[] {
      new StructField("GPSPoint", GeometryDataType$.MODULE$, true, null),
      new StructField("Unused", StringType, true, null),
      new StructField("Altitude", DoubleType, true, null),
      new StructField("Date_Days", DoubleType, true, null),
      new StructField("Date", StringType, true, null),
      new StructField("Time", StringType, true, null),
  });

  @Override
  public void initialize(InputSplit split, BeastOptions opts) throws IOException {
    lineReader.initialize(split, new TaskAttemptContextImpl(opts.loadIntoHadoopConf(null), new TaskAttemptID()));
    this.filePath = ((FileSplit)split).getPath();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (!lineReader.nextKeyValue())
      return false;
    if (lineReader.getCurrentKey().get() == 0) {
      // Skip the first six lines
      for (int $i = 0; $i < 6; $i++) {
        if (!lineReader.nextKeyValue())
          return false;
      }
    }
    feature = Feature.create(null, new PointND(geometryFactory, 2));
    Text value = lineReader.getCurrentValue();
    try {
      Object[] values = new Object[Schema.length()];
      String[] parts = value.toString().split(",");
      double latitude = Double.parseDouble(parts[0]);
      double longitude = Double.parseDouble(parts[1]);
      values[0] = new PointND(geometryFactory, 2, longitude, latitude);
      values[1] = parts[2]; // Reserved
      values[2] = Double.parseDouble(parts[3]); // Altitude in feed
      values[3] = Double.parseDouble(parts[4]); // Days
      values[4] = parts[5];
      values[5] = parts[6];
      feature = new Feature(values, Schema);
      return true;
    } catch (Exception e) {
      LOG.error(String.format("Error reading object at %s:%d with value '%s'", filePath, lineReader.getCurrentKey().get(), value));
      throw e;
    }
  }

  @Override
  public IFeature getCurrentValue() {
    return feature;
  }

  @Override
  public float getProgress() throws IOException {
    return lineReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineReader.close();
  }
}
