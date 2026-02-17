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

import com.mongodb.client.model.Projections
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.util.OperationParam
import edu.ucr.cs.bdlab.ucrstar.JSONFileConnector.FileName
import org.bson
import org.bson.Document

import java.io.{BufferedInputStream, ByteArrayOutputStream}
import java.util
import scala.annotation.varargs

/**
 * A connector that reads from a JSON file and does not allow modification
 */
class JSONFileConnector extends DatasetConnector {

  var filename: String = _

  def loadDocuments: Array[Document] = {
    var is: BufferedInputStream = null
    val baos = new ByteArrayOutputStream()
    val buffer = new Array[Byte](4096)
    try {
      is = new BufferedInputStream(getClass.getResourceAsStream(filename))
      var readSize: Int = 0
      do {
        readSize = is.read(buffer, 0, buffer.length)
        if (readSize > 0)
          baos.write(buffer, 0, readSize)
      } while (readSize > 0)
    } finally if (is != null) is.close()

    Document.parse("{\"datasets\": "+new String(baos.toByteArray)+"}")
      .getList("datasets", classOf[Document]).toArray(new Array[Document](0))
  }

  override def setup(opts: BeastOptions): Unit = {
    this.filename = opts.getString(FileName, "datasets.json")
  }

  override def insertDocument(document: Document): String = ???

  override def updateDocument(document: Document): Unit = ???

  @varargs override def listDatasets(status: String, attributes: String*): util.List[Document] = {
    var documents = loadDocuments
    if (status != null)
      documents = documents.filter(_.getString("status") == status)
    if (attributes != null) {
      for (i <- documents.indices) {
        val newDoc = new Document()
        newDoc.append("_id", documents(i).getObjectId("_id"))
        for (att <- attributes)
          newDoc.append(att, documents(i).get(att))
        documents(i) = newDoc
      }
    }
    java.util.Arrays.asList(documents:_*)
  }

  @varargs def findDocument(documentID: String, excludeAtts: String*): Document = {
    val documents = loadDocuments.filter(_.getObjectId("_id").toString == documentID)
    if (documents.isEmpty)
      return null
    val result = documents.head
    for (att <- excludeAtts) {
      result.remove(att)
    }
    result
  }

  override def findDocumentByName(name: String): Document = {
    val documents = loadDocuments.filter(_.getString("name") == name)
    if (documents.isEmpty) null else documents.head
  }

  override def deleteDocument(documentID: String): Boolean = ???

  override def getLastModifiedDocument(status: String): Long = {
    var documents = loadDocuments
    if (status != null)
      documents = documents.filter(_.getString("status") == status)
    documents.map(_.getDate("modifiedAt").getTime).max
  }

  override def updateStatusMany(currentStatus: String, newStatus: String): Long = ???
}

object JSONFileConnector {
  /** The file to read the data from */
  @OperationParam(description = "The file to read the data from", defaultValue = "datasets.json")
  val FileName: String = "jsondatafile"
}