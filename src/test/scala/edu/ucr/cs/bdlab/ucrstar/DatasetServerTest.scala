/*
 * Copyright 2021 University of California, Riverside
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
import edu.ucr.cs.bdlab.beast.util.BeastServer
import org.apache.spark.test.ScalaSparkTest
import org.bson.Document
import org.scalatest.FunSuite

import java.io.{ByteArrayOutputStream, File}
import java.net.URL
import java.util
import java.util.Random

class DatasetServerTest extends FunSuite with ScalaSparkTest {

  def startServer(opts: BeastOptions): BeastServer = {
    val server = new BeastServer()
    val port = new Random().nextInt(10000) + 10000
    opts.setInt("port", port)
        .set("connectortype", "file")
        .set("jsondatafile", "/datasets.json")
    server.setup(opts)
    val serverThread = new Thread(() => server.run(opts, null, null, sparkContext))
    serverThread.start()
    server.waitUntilStarted()
    server
  }

  /**
   * Send a GET request to the provided URL and returns the content result as a byte array.
   * @param url the url to read
   * @return the contents as a byte array
   */
  def getURL(url: URL): Array[Byte] = {
    val result = new ByteArrayOutputStream()
    val inputStream = url.openStream
    val buffer = new Array[Byte](4096)
    var readSize = 0
    do {
      readSize = inputStream.read(buffer)
      if (readSize > 0)
        result.write(buffer, 0, readSize)
    } while (readSize > 0)
    result.toByteArray
  }

  test("Download in GeoJSON format") {
    val datasetDir: File = locateResource("/datasets")
    val opts = new BeastOptions().set(DatasetServer.DatasetDirectory, datasetDir.getPath)
    val server = startServer(opts)
    val port = opts.getInt("port", -1)
    try {
      val url = new URL(s"http://127.0.0.1:$port/datasets/randpoints/features.geojson")
      val result = new String(getURL(url))
      val document = Document.parse(result)
      assert(document.get("features").asInstanceOf[util.Collection[Any]].size() == 1000)
    } finally {
      server.stop()
      server.waitUntilStopped()
    }
  }

  test("Reconstruct original format") {
    val datasetDir: File = locateResource("/datasets")
    val opts = new BeastOptions().set(DatasetServer.DatasetDirectory, datasetDir.getPath)
    val server = startServer(opts)
    val port = opts.getInt("port", -1)
    try {
      val url = new URL(s"http://127.0.0.1:$port/datasets/randpoints/features.csv")
      val result = new String(getURL(url)).split("\n")
      assert(result(1).split(";").length == 2)
    } finally {
      server.stop()
      server.waitUntilStopped()
    }
  }

  test("View a single record in JSON format") {
    val datasetDir: File = locateResource("/datasets")
    val opts = new BeastOptions().set(DatasetServer.DatasetDirectory, datasetDir.getPath)
    val server = startServer(opts)
    val port = opts.getInt("port", -1)
    try {
      val url = new URL(s"http://127.0.0.1:$port/datasets/randpoints/features/view.json?mbr=0,0,1,1")
      val result = new String(getURL(url))
      val document = Document.parse(result)
      assertResult(0)(document.size())
    } finally {
      server.stop()
      server.waitUntilStopped()
    }
  }

  test("View a single record in JSON format with extents") {
    val datasetDir: File = locateResource("/datasets")
    val opts = new BeastOptions().set(DatasetServer.DatasetDirectory, datasetDir.getPath)
    val server = startServer(opts)
    val port = opts.getInt("port", -1)
    try {
      val url = new URL(s"http://127.0.0.1:$port/datasets/randpoints/features/view.json?mbr=0,0,1,1&extents=1")
      val result = new String(getURL(url))
      val document = Document.parse(result)
      assertResult(1)(document.size())
    } finally {
      server.stop()
      server.waitUntilStopped()
    }
  }

  test("View a single record in GeoJSON format") {
    val datasetDir: File = locateResource("/datasets")
    val opts = new BeastOptions().set(DatasetServer.DatasetDirectory, datasetDir.getPath)
    val server = startServer(opts)
    val port = opts.getInt("port", -1)
    try {
      val url = new URL(s"http://127.0.0.1:$port/datasets/randpoints/features/view.geojson?mbr=0,0,1,1")
      val result = new String(getURL(url))
      val document = Document.parse(result)
      assert(document.getString("type") == "Feature")
    } finally {
      server.stop()
      server.waitUntilStopped()
    }
  }
}
