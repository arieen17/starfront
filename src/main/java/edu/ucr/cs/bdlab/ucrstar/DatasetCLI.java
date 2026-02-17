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
import edu.ucr.cs.bdlab.beast.common.JCLIOperation;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.util.OperationMetadata;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Command line interface to the dataset server
 */
@OperationMetadata(shortName = "datasets", description = "Query the UCR-Star datasets", inputArity = "0",
outputArity = "0", inheritParams = DatasetServer.class)
public class DatasetCLI implements JCLIOperation {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(DatasetCLI.class);

  /**
   * An enumerated type for the possible commands that the user can issue on the dataset database.
   */
  public enum Command {
    /**List all datasets with an optional filter*/
    List,
    /**Start the processing of a specific dataset*/
    Process,
    /**Rebuild data and visualization indexes without changing the dataset status*/
    Reindex,
    /**Delete a specific dataset*/
    Delete,
    /**Retrieve all the information for the given dataset*/
    Get,
    /**Change the status of a dataset to 'published'*/
    Publish
  }

  /**The command to issue on the dataset database*/
  @OperationParam( description = "The command to run on the datasets {List, Process, Delete, Get}", required = true)
  public static String CommandOption = "cmd";

  /**The ID of the dataset*/
  @OperationParam( description = "The id of the dataset to retrieve or process")
  public static String DatasetID = "id";

  /**The status of the datasets to filter*/
  @OperationParam( description = "The status of the datasets to filter")
  public static String Status = "status";

  private DatasetConnector connector;

  @Override
  public void setup(BeastOptions opts) {
    connector = DatasetConnector.createConnector(opts.getString(DatasetConnector.ConnectorType(), "mongodb"));
    connector.setup(opts);
  }

  /**
   * Start the processing of an existing dataset
   * @param id the document id to process. If {@code null} the earliest "created" document is processed.
   * @param sc
   * @param opts
   * @return
   * @throws IOException
   */
  public boolean processDataset(String id, BeastOptions opts, JavaSparkContext sc) {
    Collection<Document> datasets;
    if (id != null) {
      // The user specified a document to process
      Document dataset = connector.findDocument(id);
      if (dataset == null) {
        LOG.warn(String.format("Dataset with ID '%s' not found", id));
        return false;
      }
      // Reset the status to a give one if specified by the user
      String resetStatusTo = opts.getString(Status);
      if (resetStatusTo != null)
        dataset.put("status", resetStatusTo);
      datasets = Arrays.asList(dataset);
    } else {
      // No document specified, process all datasets that are currently in the created state
      datasets = connector.listDatasets("created");
      if (datasets.isEmpty())
        LOG.info("No datasets in 'created' state to process");
      else
        LOG.info(String.format("Found %d datasets in 'created' state. Will process all of them.", datasets.size()));
    }
    // Process the retrieved datasets
    for (Document dataset : datasets) {
      LOG.info(String.format("Processing dataset #%s", dataset.getObjectId("_id").toString()));
      long t1 = System.nanoTime();
      DatasetProcess process = new DatasetProcess(dataset, sc, connector);
      process.setup(opts);
      // Star the process in a background process
      process.run();
      long t2 = System.nanoTime();
      LOG.info(String.format("Dataset #%s processing in %f seconds",
          dataset.getObjectId("_id").toString(), (t2-t1)*1E-9));
    }
    return true;
  }


  /**
   * Start the processing of an existing dataset
   * @param id the document id to process. If {@code null} the earliest "created" document is processed.
   * @param opts the user options
   * @param sc the Spark context
   * @return
   * @throws IOException
   */
  public boolean reindexDataset(String id, BeastOptions opts, JavaSparkContext sc) {
    try {
      // The user specified a document to process
      Document dataset = connector.findDocument(id);
      if (dataset == null) {
        LOG.warn(String.format("Dataset with ID '%s' not found", id));
        return false;
      }
      // Reset the status to a give one if specified by the user
      DatasetProcess process = new DatasetProcess(dataset, sc, connector);
      process.setup(opts);
      process.buildIndexes();
      return true;
    } catch (IOException e) {
      LOG.error("Error building the index", e);
      return false;
    }
  }

  @Override
  public Object run(BeastOptions opts, String[] inputs, String[] outputs, JavaSparkContext sc) {
    Command cmd = opts.getEnumIgnoreCase(CommandOption, Command.List);
    switch (cmd) {
      case List:
        String status = opts.getString(Status);
        List<Document> documents = connector.listDatasets(status, "_id", "status", "name");
        if (documents.isEmpty())
          System.out.println("--------- No datasets found ---------");
        else
          printDocumentsAsTable(documents);
        break;
      case Process: {
        String datasetID = opts.getString(DatasetID);
        processDataset(datasetID, opts, sc);
        break;
      }
      case Reindex: {
        String datasetID = opts.getString(DatasetID);
        reindexDataset(datasetID, opts, sc);
        break;
      }
      case Get: {
        String datasetID = opts.getString(DatasetID);
        if (datasetID == null)
          throw new RuntimeException("Dataset ID must be set");
        Document document = connector.findDocument(datasetID);
        if (document != null)
          System.out.println(document.toJson(JsonWriterSettings.builder().indent(true).build()));
        else
          System.out.printf("Could not find dataset with ID '%s'\n", datasetID);
        break;
      }
      case Publish: {
        String datasetID = opts.getString(DatasetID);
        if (datasetID == null) {
          // Publish all ready datasets
          long modifiedCount = connector.updateStatusMany("ready", "published");
          LOG.info(String.format("Modified %d documents from status '%s' to '%s",
              modifiedCount, "ready", "published"));
        } else {
          Document document = connector.findDocument(datasetID);
          if (document != null) {
            document.put("status", "published");
            connector.updateDocument(document);
          } else
            System.out.printf("Could not find dataset with ID '%s'\n", datasetID);
        }
        break;
      }
      case Delete: {
        String datasetID = opts.getString(DatasetID);
        if (datasetID == null)
          throw new RuntimeException("Dataset ID must be set");
        connector.deleteDocument(datasetID);
        break;
      }
      default:
        throw new RuntimeException(String.format("Unsupported command '%s'", cmd));
    }
    return null;
  }

  private void printDocumentsAsTable(List<Document> documents) {
    Map<Object, Integer> fieldLengths = new HashMap<>();
    for (Document document : documents) {
      for (Map.Entry entry : document.entrySet()) {
        int length = entry.getValue() == null? 0 : entry.getValue().toString().length();
        if (!fieldLengths.containsKey(entry.getKey()) || fieldLengths.get(entry.getKey()) < length) {
          fieldLengths.put(entry.getKey(), length);
        }
      }
    }
    // Print table header
    for (Map.Entry<Object, Integer> entry : fieldLengths.entrySet()) {
      System.out.print("+");
      for (int i = 0; i < entry.getValue(); i++)
        System.out.print("-");
    }
    System.out.println("+");
    for (Map.Entry<Object, Integer> entry : fieldLengths.entrySet()) {
      System.out.print("|");
      System.out.print(StringUtils.center(entry.getKey().toString(), entry.getValue()));
    }
    System.out.println("|");
    for (Map.Entry<Object, Integer> entry : fieldLengths.entrySet()) {
      System.out.print("+");
      for (int i = 0; i < entry.getValue(); i++)
        System.out.print("-");
    }
    System.out.println("+");

    // Print table contents
    for (Document document : documents) {
      for (Map.Entry<Object, Integer> entry : fieldLengths.entrySet()) {
        System.out.print("|");
        Object value = document.get(entry.getKey());
        String formatted;
        if (value instanceof Integer || value instanceof Float || value instanceof Long || value instanceof Double) {
          // Right justify
          formatted = StringUtils.leftPad(value.toString(), entry.getValue());
        } else {
          // left justify
          if (value == null)
            value = "";
          formatted = StringUtils.rightPad(value.toString(), entry.getValue());
        }
        System.out.print(formatted);
      }
      System.out.println("|");
    }

    // Print last line
    for (Map.Entry<Object, Integer> entry : fieldLengths.entrySet()) {
      System.out.print("+");
      for (int i = 0; i < entry.getValue(); i++)
        System.out.print("-");
    }
    System.out.println("+");
  }
}
