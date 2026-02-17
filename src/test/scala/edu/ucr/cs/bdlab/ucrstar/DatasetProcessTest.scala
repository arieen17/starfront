package edu.ucr.cs.bdlab.ucrstar

import com.mongodb.BasicDBObject
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import org.apache.spark.test.ScalaSparkTest
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DatasetProcessTest extends FunSuite with ScalaSparkTest {

  lazy val connector: DatasetConnector = new JSONFileConnector

  test("should parse beastOptions as a string") {
    val dataset: Document = new Document(BasicDBObject.parse("{beastOptions: \"iformat:point(1,2) -skipheader 'separator: '\"}"))
    val datasetProcess = new DatasetProcess(dataset, sparkContext, connector)
    assert(datasetProcess.opts.getString("iformat") == "point(1,2)")
    assert(datasetProcess.opts.getString("skipheader") == "true")
    assert(datasetProcess.opts.getString("separator") == " ")
  }

  test("should parse beastOptions as a list of options") {
    val dataset: Document = new Document(BasicDBObject.parse("{beastOptions: ['iformat:point(1,2)', 'separator: ']}"))
    val datasetProcess = new DatasetProcess(dataset, sparkContext, connector)
    assert(datasetProcess.opts.getString("iformat") == "point(1,2)")
    assert(datasetProcess.opts.getString("separator") == " ")
  }

  test("should parse beastOptions as map") {
    val dataset: Document = new Document(BasicDBObject.parse("{beastOptions: {iformat: 'point(1,2)', separator: ' '}}"))
    val datasetProcess = new DatasetProcess(dataset, sparkContext, connector)
    assert(datasetProcess.opts.getString("iformat") == "point(1,2)")
    assert(datasetProcess.opts.getString("separator") == " ")
  }

  test("should work with no beastOptions") {
    val dataset: Document = new Document().append("_id", new ObjectId("123456789012345678901234"))
    val datasetProcess = new DatasetProcess(dataset, sparkContext, connector)
    datasetProcess.setup(new BeastOptions().set("iformat", "point(1,2)").set("separator", " "))
    assert(datasetProcess.opts.getString("iformat") == "point(1,2)")
    assert(datasetProcess.opts.getString("separator") == " ")
  }
}
