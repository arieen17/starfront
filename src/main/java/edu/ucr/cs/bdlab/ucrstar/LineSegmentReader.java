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
import edu.ucr.cs.bdlab.beast.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.beast.io.FeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialReaderMetadata;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads CSV files that contain line segments defined by two end points
 */
@SpatialReaderMetadata(
    description = "Parses CSV file with line segments",
    shortName = "csvline",
    extension = ".csv"
)
public class LineSegmentReader extends FeatureReader {
  private static final Log LOG = LogFactory.getLog(LineSegmentReader.class);

  /**Columns that identify the four coordinates (x1, y1, x2, y2)*/
  @OperationParam(
          description = "Column indexes that contain x1,y1,x2,y2 in this order separated by comma",
          required = true)
  public static String ColumnIndexes = "columns";

  /**An underlying reader for the text file*/
  protected final LineRecordReader lineReader = new LineRecordReader();

  /**The returned feature*/
  protected Feature feature;

  /**Cache the file path to report errors*/
  protected Path filePath;

  /**Indexes of the columns to read*/
  protected int[] columnIndexes;

  protected GeometryFactory geometryFactory = FeatureReader.DefaultGeometryFactory;

  @Override
  public void initialize(InputSplit split, BeastOptions opts) throws IOException {
    lineReader.initialize(split, new TaskAttemptContextImpl(opts.loadIntoHadoopConf(null), new TaskAttemptID()));
    this.filePath = ((FileSplit)split).getPath();
    String[] columns = opts.getString(ColumnIndexes).split(",");
    columnIndexes = new int[columns.length];
    for (int i = 0; i < columns.length; i++)
      columnIndexes[i] = Integer.parseInt(columns[i]);
    adjustCoordinateColumnIndexes(columnIndexes);;
  }


  /**
   * Adjust the coordinate column indexes so that they can be retrieved from a line in order.
   * If a coordinate index is followed by another index that appears after it, then this function
   * reduces the value of the second coordinate. For example, if the input is
   * [1, 2, 3], the output is [1, 1, 1], because removing attributes in this order from an input line
   * will produce the correct result. In other words, after removing attribute #1, then attribute #2 is now #1.
   * Similarly, after removing the second attribute, the third attribute also becomes at position #1.
   * On the other hand, if the input is [3, 2, 1], the output is also [3, 2, 1] because removing the first attribute
   * does not modify the position of the other two attributes because they appear earlier in the line.
   * @param coordinateColumnIndexes array of coordinate column indexes to adjust
   */
  protected static void adjustCoordinateColumnIndexes(int[] coordinateColumnIndexes) {
    for (int $i = 0; $i < coordinateColumnIndexes.length; $i++) {
      for (int $j = $i + 1; $j < coordinateColumnIndexes.length; $j++) {
        if (coordinateColumnIndexes[$j] > coordinateColumnIndexes[$i])
          coordinateColumnIndexes[$j]--;
      }
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (!lineReader.nextKeyValue())
      return false;
    Text value = lineReader.getCurrentValue();
    try {
      double x1 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', columnIndexes[0],
              CSVFeatureReader.DefaultQuoteCharacters));
      double y1 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', columnIndexes[1],
              CSVFeatureReader.DefaultQuoteCharacters));
      double x2 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', columnIndexes[2],
              CSVFeatureReader.DefaultQuoteCharacters));
      double y2 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', columnIndexes[3],
              CSVFeatureReader.DefaultQuoteCharacters));
      Geometry geometry = geometryFactory.createLineString(new Coordinate[]{
              new CoordinateXY(x1, y1),
              new CoordinateXY(x2, y2),
      });
      List<Object> values = new ArrayList<>();
      while (value.getLength() > 0)
        values.add(CSVFeatureReader.deleteAttribute(value,
            ',', 0, CSVFeatureReader.DefaultQuoteCharacters));
      feature = Feature.create(geometry, null, null, values.toArray());
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
