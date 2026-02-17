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

import com.mongodb.MongoClient
import com.mongodb.client.model.{Filters, Projections, Sorts, Updates}
import com.mongodb.client.{FindIterable, MongoCollection, MongoDatabase}
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.util.OperationParam
import org.bson.Document
import org.bson.types.ObjectId

import java.util
import java.util.Date

class MongoDBConnector extends DatasetConnector {
  import MongoDBConnector._

  /**The name of the MongoDB database to connect to*/
  var databaseName: String = "ucrstar"

  /**The name of the collection to access*/
  var collectionName: String = "datasets"

  override def setup(opts: BeastOptions): Unit = {
    this.databaseName = opts.getString(DatabaseName, this.databaseName)
    this.collectionName = opts.getString(CollectionName, this.collectionName)
  }

  /**The client connector to MongoDB*/
  private lazy val mongoClient: MongoClient =  new MongoClient(MongoDBHost, MongoDBPort)

  /**The database connector to MongoDB*/
  private lazy val mongoDatabase: MongoDatabase = mongoClient.getDatabase(databaseName)

  /**The collection of documents (datasets)*/
  private lazy val mongoCollection: MongoCollection[Document] = mongoDatabase.getCollection(collectionName)

  override def insertDocument(document: Document): String = {
    document.put("modifiedAt", new Date(System.currentTimeMillis))
    mongoCollection.insertOne(document)
    document.getObjectId("_id").toString
  }

  override def updateDocument(document: Document): Unit = {
    document.put("modifiedAt", new Date(System.currentTimeMillis))
    val updateQuery = new Document()
    updateQuery.append("$set", document)
    val searchQuery = new Document()
    searchQuery.append("_id", document.getObjectId("_id"))
    mongoCollection.updateOne(searchQuery, updateQuery)
  }

  override def listDatasets(status: String, attributes: String*): java.util.List[Document] = {
    var documents: FindIterable[Document] = mongoCollection.find()
    if (status != null)
      documents = documents.filter(Filters.eq("status", status))
    if (attributes != null)
      documents = documents.projection(Projections.fields(Projections.include(attributes:_*)))
    documents = documents.sort(Sorts.descending("features"))
    documents.into(new util.ArrayList[Document]())
  }

  override def findDocument(documentID: String, excludeAtts: String*): Document =
    mongoCollection.find(new Document("_id", new ObjectId(documentID)))
      .projection(Projections.exclude(excludeAtts:_*))
      .first()

  override def findDocumentByName(name: String): Document = mongoCollection.find(new Document("name", name)).first()

  override def deleteDocument(documentID: String): Boolean = {
    val findIterable = mongoCollection.find(new Document("_id", new ObjectId(documentID)))
    mongoCollection.deleteOne(findIterable.first()).getDeletedCount == 1
  }

  override def getLastModifiedDocument(status: String): Long = {
    var documents: FindIterable[Document] = mongoCollection.find()
    if (status != null)
      documents = documents.filter(Filters.eq("status", status))
    documents = documents.projection(Projections.fields(Projections.include("modifiedAt")))
      .sort(Sorts.descending("modifiedAt"))
    val lastModifiedDocument = documents.first()
    if (lastModifiedDocument == null)
      System.currentTimeMillis()
    else
      lastModifiedDocument.getDate("modifiedAt").getTime
  }

  override def updateStatusMany(currentStatus: String, newStatus: String): Long =
    mongoCollection.updateMany(Filters.eq("status", currentStatus), Updates.set("status", newStatus))
      .getModifiedCount
}

object MongoDBConnector {
  /** The MongoDB database name */
  @OperationParam(description = "The name of the MongoDB database name", defaultValue = "ucrstar")
  val DatabaseName: String = "database"

  /** The MongoDB collection name */
  @OperationParam(description = "The name of the MongoDB collection name", defaultValue = "datasets")
  val CollectionName: String = "collection"

  /** The host on which the server is running */
  val MongoDBHost: String = "localhost"

  /** The port on which the server is running */
  val MongoDBPort: Int = 27017
}