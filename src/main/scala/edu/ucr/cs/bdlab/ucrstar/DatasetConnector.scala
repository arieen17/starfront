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

import edu.ucr.cs.bdlab.beast.util.{IConfigurable, OperationParam}
import org.bson.Document

import scala.annotation.varargs

/**
 * An interface for accessing a database of datasets.
 */
trait DatasetConnector extends IConfigurable {
  /**
   * Inserts the given document into the database and assigns an ID to it, if needed.
   *
   * @param document the document to insert
   * @return the assigned ID of the document.
   */
  def insertDocument(document: Document): String

  /**
   * Updates the given document in the database. The given document should already have an ID.
   *
   * @param document the document to update in the database. All fields will be updated based on the ID.
   */
  def updateDocument(document: Document): Unit

  /**
   * List all published documents and keep only the given list of attributes.
   *
   * @param attributes
   * @return
   */
  @varargs def listPublishedDatasets(attributes: String*): java.util.List[Document] =
    listDatasets("published", attributes:_*)

  /**
   * List all the documents with the given status (if not null) and limit the attributes to the given ones.
   *
   * @param status     if not null, select only the documents with that status
   * @param attributes if not null, limit the selected attributes to this list.
   * @return the list of documents that match the status or an empty list of no matches.
   */
  @varargs def listDatasets(status: String, attributes: String*): java.util.List[Document]

  /**
   * Return one document in a specific collection of a specific database by giving the document ID #
   *
   * @param documentID the ID of the document
   * @return the document with the given ID or {@code null}
   */
  @varargs def findDocument(documentID: String, excludeAtts: String*): Document

  /**
   * Searches for a dataset given its name
   *
   * @param name the name of the dataset
   * @return the document with the given name or {@code null} if no matches are found
   */
  def findDocumentByName(name: String): Document

  /**
   * Deletes the document with the given ID from the database.
   *
   * @param documentID the ID of the document to delete
   * @return {@code true} if the document was found and deleted. {@code false} otherwise.
   */
  def deleteDocument(documentID: String): Boolean

  /**
   * Get the timestamp of the last modified document in the database with the given status.
   *
   * @return the timestamp of the last modified document.
   */
  def getLastModifiedDocument(status: String): Long

  /**
   * Update all documents that have one status to the new status
   *
   * @param currentStatus the status used for search
   * @param newStatus     the new status to assign to documents
   * @return the number of documents that were updated
   */
  def updateStatusMany(currentStatus: String, newStatus: String): Long
}

object DatasetConnector {
  /** The type of the connector to use */
  @OperationParam(description = "The type of the connector to use {mongodb,file}", defaultValue = "mongodb")
  val ConnectorType: String = "connectortype"

  def createConnector(name: String): DatasetConnector = name.toLowerCase match {
    case "mongodb" => new MongoDBConnector
    case "file" => new JSONFileConnector
  }
}