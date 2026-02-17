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

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat}
import edu.ucr.cs.bdlab.beast.operations.FeatureWriterSize
import edu.ucr.cs.bdlab.beast.util.OperationMetadata
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

/**
 * Break-apart and index. Breaks Multi* to *, e.g., MultiLineString into LineStrings.
 * Additionally, it breaks LineStrings into line segments and Polygons and LineSegments.
 * It also removes all attributes and keeps the geometry only.
 */
@OperationMetadata(shortName = "breakandindex",
  description = "Breaks down geometries into small pieces that are easier to visualize",
  inputArity = "1", outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat]))
object BreakAndIndex extends CLIOperation {
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    // Read the input
    val input = sc.spatialFile(inputs(0), opts)
    // Break the geometries apart
    val geometries: RDD[Geometry] = input.flatMap(f => new GeometryBreaker(f.getGeometry))
    // Package into features
    val features: RDD[IFeature] = geometries.map(g => Feature.create(null, g))
    // Extract index parameters from the command line arguments
    val gIndex = opts.getString(IndexHelper.GlobalIndex, "rsgrove")
    val partitionerClass: Option[Class[_ <: SpatialPartitioner]] = IndexHelper.partitioners.get(gIndex)
    if (partitionerClass.isEmpty) {
      System.err.println(s"Unrecognized partitioner ${gIndex}")
      System.exit(1)
    }
    // Index the data
    val partitionedFeatures = IndexHelper.partitionFeatures(features, partitionerClass.get,
      new FeatureWriterSize(opts), opts)
    // Save the index to disk
    partitionedFeatures.saveAsIndex(outputs(0))
  }
}