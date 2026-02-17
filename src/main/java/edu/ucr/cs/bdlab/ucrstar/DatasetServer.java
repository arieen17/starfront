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

import edu.ucr.cs.bdlab.beast.cg.GeometryQuadSplitter;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.common.WebMethod;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.FeatureReader;
import edu.ucr.cs.bdlab.beast.io.FeatureWriter;
import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureReader;
import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.KMLFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.KMZFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.SpatialWriter;
import edu.ucr.cs.bdlab.beast.io.shapefile.CompressedShapefileWriter;
import edu.ucr.cs.bdlab.beast.io.shapefile.ShapefileFeatureWriter;
import edu.ucr.cs.bdlab.beast.util.CounterOutputStream;
import edu.ucr.cs.bdlab.beast.util.IConfigurable;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import edu.ucr.cs.bdlab.beast.util.AbstractWebHandler;
import edu.ucr.cs.bdlab.davinci.MultilevelPyramidPlotHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.*;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.TopologyException;

import javax.mail.MessagingException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

//JSON generator
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

// for region search
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.collection.JavaConverters;
import scala.collection.Seq;


/**
 * Services for submitting a new dataset
 */
public class DatasetServer extends AbstractWebHandler implements IConfigurable {

  private static final Log LOG = LogFactory.getLog(DatasetServer.class);

  /**E-mail used to send notification emails*/
  @OperationParam( description = "Sender's email address")
  public static String Email = "email";

  /**Password to authenticate the email server*/
  @OperationParam( description = "Sender's email password")
  public static String Password = "password";

  /**Email address to use with the SMTP server for sending emails. {@code null} if not provided.*/
  public static String email;

  /**Password to authenticate the SMTP server. {@code null} if not provided*/
  public static String password;

  @OperationParam(description = "Overwrite existing directories", defaultValue = "true")
  public static String Overwrite = "overwrite";

  @OperationParam(description = "Decompress files before processing", defaultValue = "true")
  public static String Decompress = "decompress";

  @OperationParam(description = "Create separate indexes for data and visualization", defaultValue = "auto")
  public static String SeparateIndexes = "separateindexes";

  @OperationParam(description = "Threshold for flattening the geometries, maximum number of points per geometry.", defaultValue = "100")
  public static String FlattenThreshold = "flattenthreshold";

  /**The directory to which the output files (indexes and visualizations) should be written*/
  @OperationParam(
      description = "Output directory for indexes and visualizations",
      defaultValue = ". (working directory)"
  )
  public static String DatasetDirectory = "datadir";

  private DatasetConnector connector;

  /**The directory that contains the datasets*/
  private Path datasetDir;

  @Override public void setup(SparkContext sc, BeastOptions opts) {
    super.setup(sc, opts);
    this.email = opts.getString(Email);
    this.password = opts.getString(Password);
    this.datasetDir = new Path(opts.getString(DatasetDirectory, "."));
    this.connector = DatasetConnector.createConnector(opts.getString(DatasetConnector.ConnectorType(), "mongodb"));
    connector.setup(opts);
  }

  @Override
  public void addDependentClasses(BeastOptions opts, Stack<Class<?>> classes) {
    classes.push(MongoDBConnector.class);
  }

  /**
   * This method is called when the user submits a request to add a new dataset to the UCR-Star server.
   * @param target the target path requested by the client
   * @param request HTTP request
   * @param response HTTP response
   * @return {@code true} if the request was handled correctly
   * @throws IOException if an error happens while creating the response
   */
  @WebMethod(url="/datasets/", method = "post")
  public boolean addDataset(String target, HttpServletRequest request, HttpServletResponse response) throws IOException{
    request.setCharacterEncoding("UTF-8");
    String datasetTitle = request.getParameter("title");
    String datasetDescription = request.getParameter("description");
    String[] urls = request.getParameter("urls").trim().split("\\s*[\\r\\n]+\\s*");
    for (int i = 0; i < urls.length; i++)
      urls[i] = urls[i].trim();
    String creator = request.getParameter("author");
    String name = request.getParameter("name");
    String contactEmail = request.getParameter("email");
    String homepage = request.getParameter("project_homepage");
    String format = request.getParameter("format");
    String license = request.getParameter("license");
    if (license.equals("Other"))
      license = request.getParameter("license-other");
    String separator = request.getParameter("delimiter");
    boolean skipheader = request.getParameter("skipheader") != null;
    String[] tags = request.getParameter("tags") == null? null : request.getParameter("tags").split(",");

    Map<String, String> beastOpts = new HashMap<>();
    //Create the document
    Document dataDocument = new Document("title", datasetTitle).
        append("description", datasetDescription).
        append(urls.length == 1 ? "download_url" : "download_urls", urls.length == 1? urls[0] : Arrays.asList(urls)).
        append("name", name).
        append("author", creator).
        append("email", contactEmail).
        append("project_homepage", homepage).
        append("license", license).
        append("status", "created");
    if (tags != null && tags.length > 0)
      dataDocument.append("tags", Arrays.asList(tags));
    if (separator != null && !separator.isEmpty())
      beastOpts.put("separator", separator);
    if (skipheader)
      beastOpts.put("skipheader", "true");
    if (format.equals("csv")) {
      String geometryType = request.getParameter("geometry-type");
      if (geometryType.equals("point")) {
        String xColumn = request.getParameter("x-column");
        String yColumn = request.getParameter("y-column");
        beastOpts.put("iformat", String.format("%s(%s,%s)", geometryType,
            xColumn == null || xColumn.isEmpty()? "0" : xColumn,
            yColumn == null || yColumn.isEmpty()? "1" : yColumn));
      } else if (geometryType.equals("wkt")) {
        String wktColumn = request.getParameter("wkt-column");
        beastOpts.put("iformat", String.format("%s(%s)", geometryType,
            wktColumn == null || wktColumn.isEmpty()? "0" : wktColumn));
      }
    } else {
      beastOpts.put("iformat", format);
    }
    dataDocument.append("beastOptions", beastOpts);

    // Insert the new document into the mongoDB database
    String datasetID = connector.insertDocument(dataDocument);

    //Give relevant feedback to the front-end
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("text/plain");
    PrintWriter writer = response.getWriter();
    writer.println("You have successfully submitted a request for the data set titled "+ datasetTitle);
    writer.println("The id of your submission is " + datasetID);
    writer.close();

    try {
      EmailClient.sendDatasetReceivedEmail(conf, dataDocument);
    } catch (MessagingException e) {
      throw new RuntimeException("Error sending dataset received email", e);
    }

    return true;
  }

  /**
   * List a summary of all datasets in JSON format.
   * @param target the target path requested by the client
   * @param request HTTP request
   * @param response HTTP response
   * @return {@code true} if the request was handled correctly
   * @throws IOException if an error happens while creating the response
   */
  @WebMethod(url="/datasets/", method = "get")
  public boolean listDatasets(String target, HttpServletRequest request, HttpServletResponse response) throws IOException{
    long cacheDate = request.getDateHeader("If-Modified-Since");
    long lastModifiedDocument = connector.getLastModifiedDocument("published");
    if (lastModifiedDocument < cacheDate) {
      // The cached version is OK
      LOG.info("Skipped the datasets. Cached version is up-to-date");
      response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
      return true;
    }
    final String[] attributes = {
      "_id", "name", "description", "size", "num_features", "num_points", "geometry_type", "format", "tags"
    };
    List<Document> allDatasets = connector.listPublishedDatasets(attributes);
    response.setStatus(HttpServletResponse.SC_OK);
    response.addDateHeader("Last-Modified", System.currentTimeMillis());
    response.addDateHeader("Expires", System.currentTimeMillis() + (1000L * 60 * 60 * 24));
    response.addHeader("Cache-Control", "max-age=86400");
    response.setContentType("application/json");
    PrintWriter writer = response.getWriter();
    writer.print("{\"datasets\":[");
    for (int iDoc = 0; iDoc < allDatasets.size(); iDoc++) {
      writer.print(allDatasets.get(iDoc).toJson(getJSONBuilder()));
      if (iDoc != allDatasets.size() - 1)
        writer.print(",");
    }
    writer.print("]}");
    writer.close();
    return true;
  }

  private JsonWriterSettings getJSONBuilder() {
    return JsonWriterSettings.builder()
        .int64Converter((value, w) -> w.writeNumber(value.toString()))
        .objectIdConverter((oid, w) -> w.writeString(oid.toString()))
        .build();
  }

  /**
   * Retrieve all details about a specific dataset
   * @param target the target path requested by the client
   * @param request HTTP request
   * @param response HTTP response
   * @return {@code true} if the request was handled correctly
   * @throws IOException if an error happens while creating the response
   */
  @WebMethod(url="/datasets/([0-9a-fA-F]+).json", method = "get")
  public boolean retrieveDataset(String target, HttpServletRequest request, HttpServletResponse response) throws IOException{
    String id = target.substring(10, 10 + 24);
    Document dataset = connector.findDocument(id, "status");
    if (dataset == null) {
      // Find it by name instead
      dataset = connector.findDocumentByName(id);
      if (dataset == null) {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        return true;
      }
    }
    // If a client-cached version exists, keep it
    long cacheDate = request.getDateHeader("If-Modified-Since");
    if (dataset.containsKey("modifiedAt") && dataset.get("modifiedAt") instanceof java.util.Date) {
      long documentModificationDate = dataset.getDate("modifiedAt").getTime();
      if (documentModificationDate < cacheDate) {
        // The cached version is OK
        response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
        return true;
      }
    }
    // Prepare the result
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("application/json");
    PrintWriter writer = response.getWriter();
    writer.print("{\"dataset\":");
    writer.print(dataset.toJson(getJSONBuilder()));
    writer.print("}");
    writer.close();
    return true;
  }

  /**
   * Retrieve a page that can be pointed to for social media sharing. This page adds metadata information specific
   * to one dataset to make the sharing more (personalized).
   * @param target the target path requested by the client
   * @param request HTTP request
   * @param response HTTP response
   * @param name the name of the dataset
   * @return {@code true} if the request was handled correctly
   * @throws IOException if an error happens while creating the response
   */
  @WebMethod(url="/datasets/{name}/share.html", method = "get")
  public boolean retrieveDatasetSocialMediaPage(String target, HttpServletRequest request,
                                                HttpServletResponse response, String name) throws IOException {
    // Find the dataset pointed to by this request
    Document dataset = connector.findDocumentByName(name);
    if (dataset == null || !dataset.getString("status").equals("published")) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return true;
    }
    // Note: We cannot utilized client-side caching efficiently due to the request parameters center and zoom
    String center = request.getParameter("center");
    String zoom = request.getParameter("zoom");
    String location = center != null && zoom != null? String.format("#center=%s&zoom=%s", center, zoom) : "";
    // Prepare the result
    // Load the template first
    String templateFile =  "/socialmedia.html";
    LineReader templateFileReader = new LineReader(MultilevelPyramidPlotHelper.class.getResourceAsStream(templateFile));
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("text/html");
    PrintWriter htmlOut = response.getWriter();
    try {
      Text line = new Text();
      while (templateFileReader.readLine(line) > 0) {
        String lineStr = line.toString();
        lineStr = lineStr.replace("${dataset.title}", dataset.getString("title"));
        lineStr = lineStr.replace("${dataset.description}", dataset.getString("description"));
        lineStr = lineStr.replace("${dataset.url}",
            "https://ucrstar.com?"+dataset.getString("name")+location);
        lineStr = lineStr.replace("${image}", "https://ucrstar.com/images/logo-square.png");
        htmlOut.println(lineStr);
      }
    } finally {
      templateFileReader.close();
      htmlOut.close();
    }
    return true;
  }

  private static final Map<Class<? extends FeatureWriter>, String> MimeTypes = new HashMap<>();

  static {
    MimeTypes.put(CSVFeatureWriter.class, "text/csv");
    MimeTypes.put(GeoJSONFeatureWriter.class, "application/geo+json");
    MimeTypes.put(KMLFeatureWriter.class, "application/vnd.google-earth.kml+xml");
    MimeTypes.put(KMZFeatureWriter.class, "application/vnd.google-earth.kmz");
    MimeTypes.put(CompressedShapefileWriter.class, "application/zip");
  }

  /**
   * Returns the non-spatial attributes of a single record that matches the given query range.
   * The query range is provided as a parameter "mbr" with value "x1,y1,x2,y2".
   * If the format is JSON, an additional optional parameter "extents" can be added to return the extents
   * of the geometry.
   * @param target the URL of the request
   * @param request the HTTP request
   * @param response the HTTP response
   * @param name the name of the dataset to retrieve from
   * @param format the format of the result. Currently, only JSON and GeoJSON are supported.
   * @return the non-spatial attributes of the first record that matches the query. An empty object if no records found.
   * @throws Exception if an error happens while processing the request
   */
  @WebMethod(url="/datasets/{name}/features/view\\.{format}", method = "get")
  public boolean viewFeature(String target, HttpServletRequest request,
                             HttpServletResponse response, String name,String format) throws Exception {
    Document document = connector.findDocumentByName(name);
    if (document == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return true;
    }
    Path dataPath = new Path(datasetDir, document.getString("index_path"));
    // Prepare the output writer
    OutputStream out = response.getOutputStream();
    BeastOptions opts = new BeastOptions(conf);
    String mbrString = request.getParameter("mbr");
    JsonGenerator generator = new JsonFactory().createGenerator(out);
    response.setStatus(HttpServletResponse.SC_OK);

    // Now, read all input splits one-by-one and write their contents to the writer
    EnvelopeNDLite[] mbrs = null;
    if (mbrString != null) {
      EnvelopeNDLite mbr = EnvelopeNDLite.decodeString(mbrString, new EnvelopeNDLite());
      if (mbr.getSideLength(0) > 360.0) {
        mbr.setMinCoord(0, -180.0);
        mbr.setMaxCoord(0, +180.0);
      } else {
        // Adjust the minimum and maximum longitude to be in the range [-180.0, +180.0]
        mbr.setMinCoord(0, mbr.getMinCoord(0) - 360.0 * Math.floor((mbr.getMinCoord(0) + 180.0) / 360.0));
        mbr.setMaxCoord(0, mbr.getMaxCoord(0) - 360.0 * Math.floor((mbr.getMaxCoord(0) + 180.0) / 360.0));
      }
      if (mbr.getMinCoord(0) > mbr.getMaxCoord(0)) {
        // The MBR crosses the international day line, split it into two MBRs
        mbrs = new EnvelopeNDLite[2];
        mbrs[0] = new EnvelopeNDLite(2, mbr.getMinCoord(0), mbr.getMinCoord(1), +180.0, mbr.getMaxCoord(1));
        mbrs[1] = new EnvelopeNDLite(2, -180.0, mbr.getMinCoord(1), mbr.getMaxCoord(0), mbr.getMaxCoord(1));
      } else {
        // A simple MBR that does not cross the line.
        mbrs = new EnvelopeNDLite[]{mbr};
      }
    }

    int iMBR = 0;
    boolean featureFound = false;
    do {
      BeastOptions readOpts = new BeastOptions(opts.loadIntoHadoopConf(this.conf));
      if (mbrs != null)
        readOpts.set(SpatialFileRDD.FilterMBR(), mbrs[iMBR].encodeAsString());
      Geometry queryGeom = mbrs == null? null : FeatureReader.DefaultGeometryFactory.toGeometry(mbrs[iMBR].toJTSEnvelope());

      Class<? extends FeatureReader> featureReaderClass = SpatialFileRDD.getFeatureReaderClass(dataPath.toString(), readOpts);
      SpatialFileRDD.FilePartition[] partitions = SpatialFileRDD.createPartitions(dataPath.toString(),
              readOpts, this.conf);
      for (int iPartition = 0; iPartition < partitions.length && !featureFound; iPartition++) {
        SpatialFileRDD.FilePartition partition = partitions[iPartition];
        Iterator<IFeature> features = SpatialFileRDD.readPartitionJ(partition, featureReaderClass, readOpts);
        while (!featureFound && features.hasNext()) {
          IFeature feature = features.next();
          // Apply the refinement step by checking the actual geometry not just the MBR
          if (queryGeom == null || feature.getGeometry().intersects(queryGeom)) {
            featureFound = true;
            // Found the geometry. Write it to the output in the desired format
            if (format.equalsIgnoreCase("json")) {
              generator.writeStartObject();
              StructField[] fields = feature.schema().fields();
              for (int i : feature.iNonGeomJ()) {
                if (!feature.isNullAt(i)) {
                  DataType dataType = fields[i].dataType();
                  String attName = fields[i].name();
                  Object value = feature.get(i);
                  generator.writeFieldName(attName);
                  writeAttributeValue(generator, dataType, value);
                }
              }
              if (request.getParameter("extents") != null) {
                // Add the MBR of the geometry
                Envelope geometryExtents = feature.getGeometry().getEnvelopeInternal();
                generator.writeFieldName("extents");
                writeAttributeValue(generator, ArrayType.apply(DataTypes.DoubleType),
                    JavaConverters.asScalaBufferConverter(java.util.Arrays.asList(
                    new Double[] {geometryExtents.getMinX(), geometryExtents.getMinY(),
                        geometryExtents.getMaxX(), geometryExtents.getMaxY()})).asScala().toSeq());
              }
              generator.writeEndObject();
            } else if (format.equalsIgnoreCase("geojson")) {
              GeoJSONFeatureWriter.writeFeature(generator, feature, true);
            }
          }
        }
      }
    } while (!featureFound && mbrs != null && ++iMBR < mbrs.length);
    if (!featureFound) {
      generator.writeStartObject();
      generator.writeNumberField("out_bound", 1);
      generator.writeEndObject();
    }
    generator.close();
    return true;
  }

  private static void writeAttributeValue(JsonGenerator generator, DataType attType, Object attVal)
          throws IOException {
    if (attType == DataTypes.IntegerType)
      generator.writeNumber((Integer) attVal);
    else if (attType == DataTypes.LongType) generator.writeNumber((Long) attVal);
    else if (attType == DataTypes.DoubleType) generator.writeNumber((Double) attVal);
    else if (attType == DataTypes.BooleanType) generator.writeBoolean((Boolean) attVal);
    else if (attType == DataTypes.TimestampType || attType == DataTypes.StringType)
      generator.writeString(attVal.toString());
    else if (attType instanceof ArrayType) {
      scala.collection.Seq arrayValues = (scala.collection.Seq) attVal;
      DataType arrayType = ((ArrayType)attType).elementType();
      generator.writeStartArray();
      for (int i = 0; i < arrayValues.length(); i++)
        writeAttributeValue(generator, arrayType, arrayValues.apply(i));
      generator.writeEndArray();
    } else if (attType instanceof MapType) {
      scala.collection.Map mapValues = (scala.collection.Map) attVal;
      DataType valueType = ((MapType)attType).valueType();
      scala.collection.Iterator<Object> keys = mapValues.keysIterator();
      scala.collection.Iterator<Object> values = mapValues.valuesIterator();
      generator.writeStartObject();
      while (keys.hasNext()) {
        generator.writeFieldName(keys.next().toString());
        writeAttributeValue(generator, valueType, values.next());
      }
      generator.writeEndObject();
    }
  }

  /**
   * Download a dataset in a standard format. The request can contain an additional parameter {@code mbr}
   * that contains the subset of the data to download in the format {@code west,south,east,north}
   * OR parameter {@code region} that contains a region code to specify the subset of the data to download
   * @param target the full path of the request
   * @param request the request parameters
   * @param response the response to write to
   * @param name the name of the dataset to download
   * @param format the format of the downloaded file
   * @return {@code true} if the request was handled correctly
   * @throws IOException if an error happens while handling the request
   */
  @WebMethod(url="/datasets/{name}/features\\.{format}", method = "get")
  public boolean downloadDataset(String target, HttpServletRequest request,
                                 HttpServletResponse response, String name,
                                 String format) throws Exception {
    String formatExtension = format;
    if (format.equals("kmz"))
      format = "kml";
    Document document = connector.findDocumentByName(name);
    if (document == null)
      return false; // 404
    Path dataPath = new Path(datasetDir, document.getString("index_path"));
    FileSystem fileSystem = dataPath.getFileSystem(this.conf);
    // Retrieve the master file path to determine the modification time
    Path masterFilePath = SpatialFileRDD.getMasterFilePath(fileSystem, dataPath);
    if (masterFilePath == null)
      return false;
    long masterFileModificationTime = fileSystem.getFileStatus(masterFilePath).getModificationTime();
    String mbrString = request.getParameter("mbr");
    String regionString = request.getParameter("region");
    long clientCachedTimestamp = request.getHeader("If-Modified-Since") != null ?
        request.getDateHeader("If-Modified-Since") : 0;
    if (mbrString == null && regionString == null && clientCachedTimestamp >= masterFileModificationTime) {
      // The client already has an up-to-date copy. Skip the request.
      response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
      LOG.info(String.format("Not returning data layer for '%s' since the client has an up-to-date version", target));
      return true;
    }
    if (mbrString == null && regionString == null) {
      // If the entire file is downloaded, add a header to make the file available for a day
      // This is helpful when the file is loaded for visualization as a GeoJSON file
      response.addDateHeader("Last-Modified", masterFileModificationTime);
      response.addDateHeader("Expires", masterFileModificationTime + AbstractWebHandler.OneDay);
      response.addHeader("Cache-Control", "max-age=86400");
    }

    // Prepare the output writer
    CounterOutputStream counterOut = new CounterOutputStream(response.getOutputStream());
    OutputStream out = counterOut;
    // Search for the writer that supports the given format
    CompressionCodec codec = new CompressionCodecFactory(this.conf).getCodec(new Path(format));
    boolean resultIsCompressed = false;
    if (codec != null) {
      resultIsCompressed = true;
      format = format.substring(0, format.lastIndexOf('.'));
      out = codec.createOutputStream(out);
    }
    Class<? extends FeatureWriter> matchingWriter = null;
    Iterator<Class<? extends FeatureWriter>> writerIter = FeatureWriter.featureWriters.values().iterator();
    while (matchingWriter == null && writerIter.hasNext()) {
      Class<? extends FeatureWriter> writerClass = writerIter.next();
      FeatureWriter.Metadata metadata = writerClass.getAnnotation(FeatureWriter.Metadata.class);
      if (metadata.extension().equalsIgnoreCase("."+format))
        matchingWriter = writerClass;
    }
    if (matchingWriter == null) {
      reportError(response, String.format("Unrecognized extension '%s'", format));
      return false;
    }
    // Mark the data as compressed to avoid the overhead of double compression
    if (matchingWriter == KMZFeatureWriter.class || matchingWriter == CompressedShapefileWriter.class)
      resultIsCompressed = true;
    if (!resultIsCompressed && isGZIPAcceptable(request)) {
      out = new GZIPOutputStream(out);
      response.setHeader("Content-Encoding", "gzip");
    }
    String mimetype = MimeTypes.get(matchingWriter);
    if (mimetype == null)
      LOG.warn("Unknown Mime type for "+matchingWriter.getSimpleName());
    BeastOptions opts = new BeastOptions(conf);
    if (matchingWriter == CSVFeatureWriter.class) {
      // Special handling for CSV files
      // TODO use the same column name and positions of the input file.
      // TODO currently, this is not possible in case the input format was provided as "point(x,y)" (with column names)
      if (document.getString("geometry_type").equalsIgnoreCase("point"))
        opts.set(SpatialWriter.OutputFormat(), "point");
      else
        opts.set(SpatialWriter.OutputFormat(), "wkt");
      if (document.containsKey("beastOptions")) {
        Document options = (Document) document.get("beastOptions");
        if (options.containsKey("separator"))
          opts.set(CSVFeatureWriter.FieldSeparator, options.getString("separator"));
      }
    } else if (matchingWriter == CompressedShapefileWriter.class || matchingWriter == ShapefileFeatureWriter.class) {
      // Set the part size in ShapefileWriter to 32 MB to make the download start asap and avoid client timeout.
      opts.setLong(CompressedShapefileWriter.PartSize, 32 * 1024 * 1024);
    }

    // Get region Geometry and MBR regions.geojson for specified region code
    // A list of regions to search in
    List<Geometry> searchRegions = new ArrayList<>();
    String regionDisplayName = null;
    if (regionString != null) {
      String regionOptionsFileName = "regions.geojson.gz";
      IFeature regionF = null;
      Path regionsFilePath = new Path(regionOptionsFileName);
      try (GeoJSONFeatureReader regionsFileReader = new GeoJSONFeatureReader()) {
        regionsFileReader.initialize(regionsFilePath, opts);
        while (regionsFileReader.nextKeyValue()) {
          IFeature f = regionsFileReader.getCurrentValue();
          if ( regionString.equals( f.getAs("code") ) ) {
            regionF = f;
          }
        }
      }
      if (regionF == null) {
        reportError(response, String.format("Unrecognized region-code '%s'", regionString));
        return false;
      } else {
        regionDisplayName = regionF.getAs("display_name");
        GeometryQuadSplitter subGeoms = new GeometryQuadSplitter(regionF.getGeometry(), 100);
        while (subGeoms.hasNext()) {
          searchRegions.add(subGeoms.next());
        }
      }
    } else if (mbrString != null) {
      // A query MBR is provided and no refinement geometry is needed
      EnvelopeNDLite mbr = EnvelopeNDLite.decodeString(mbrString, new EnvelopeNDLite());
      if (mbr.getSideLength(0) > 360.0) {
        mbr.setMinCoord(0, -180.0);
        mbr.setMaxCoord(0, +180.0);
      } else {
        // Adjust the minimum and maximum longitude to be in the range [-180.0, +180.0]
        mbr.setMinCoord(0, mbr.getMinCoord(0) - 360.0 * Math.floor((mbr.getMinCoord(0) + 180.0) / 360.0));
        mbr.setMaxCoord(0, mbr.getMaxCoord(0) - 360.0 * Math.floor((mbr.getMaxCoord(0) + 180.0) / 360.0));
      }
      if (mbr.getMinCoord(0) > mbr.getMaxCoord(0)) {
        // The MBR crosses the international day line, split it into two MBRs
        searchRegions.add(FeatureReader.DefaultGeometryFactory.toGeometry(
            new Envelope(mbr.getMinCoord(0), +180.0, mbr.getMinCoord(1), mbr.getMaxCoord(1))
        ));
        searchRegions.add(FeatureReader.DefaultGeometryFactory.toGeometry(
            new Envelope(-180.0, mbr.getMaxCoord(0), mbr.getMinCoord(1), mbr.getMaxCoord(1))
        ));
      } else {
        // A simple MBR that does not cross the line.
        searchRegions.add(FeatureReader.DefaultGeometryFactory.toGeometry(mbr.toJTSEnvelope()));
      }
    }

    // Prepare download file name
    String downloadFileNamePrefix = name.replaceAll("[/\\s]", "_");
    if (regionDisplayName != null)
      downloadFileNamePrefix = downloadFileNamePrefix + "_" + regionDisplayName.replaceAll("[^a-zA-Z0-9]+", "_");
    String downloadFileName = downloadFileNamePrefix + "." + formatExtension;
    response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", downloadFileName));

    // Now, read all input splits one-by-one and write their contents to the writer
    Class<? extends FeatureReader> featureReaderClass = SpatialFileRDD.getFeatureReaderClass(dataPath.toString(), opts);
    SpatialFileRDD.FilePartition[] partitions = SpatialFileRDD.createPartitions(dataPath.toString(), opts, this.conf);

    // Prepare the writing
    FeatureWriter featureWriter = matchingWriter.newInstance();
    response.setStatus(HttpServletResponse.SC_OK);
    if (mimetype != null)
      response.setContentType(mimetype);
    // Initialize the feature writer
    featureWriter.initialize(out, opts.loadIntoHadoopConf(this.conf));

    try {
      for (SpatialFileRDD.FilePartition partition : partitions) {
        if (searchRegions.isEmpty()) {
          // No search provided, match all geometries
          Iterator<IFeature> features = SpatialFileRDD.readPartitionJ(partition, featureReaderClass, opts);
          while (features.hasNext())
            featureWriter.write(features.next());
        } else {
          // Check which geometries overlap with this partition
          List<Geometry> matchingGeoms = new ArrayList<>();
          if (partition instanceof SpatialFileRDD.SpatialFilePartition) {
            Geometry partitionMBR = FeatureReader.DefaultGeometryFactory
                .toGeometry(((SpatialFileRDD.SpatialFilePartition) partition).mbr().toJTSEnvelope());
            for (Geometry geom : searchRegions) {
              if (geom.intersects(partitionMBR))
                matchingGeoms.add(geom);
            }
          } else {
            // No global index is provided, match all geometries
            matchingGeoms = searchRegions;
          }
          if (!matchingGeoms.isEmpty()) {
            Iterator<IFeature> features = SpatialFileRDD.readPartitionJ(partition, featureReaderClass, opts);
            while (features.hasNext()) {
              IFeature nextFeature = features.next();
              try {
                int iGeom = 0;
                while (iGeom < matchingGeoms.size() && !nextFeature.getGeometry().intersects(matchingGeoms.get(iGeom)))
                  iGeom++;
                // If matched with at least one geometry, return the record
                if (iGeom < matchingGeoms.size())
                  featureWriter.write(nextFeature);
              } catch (TopologyException e) {
                // Skip this geometries
                LOG.warn("Error matching feature " + nextFeature, e);
              }
            }
          }
        }
      }
    } finally {
      featureWriter.close();
    }
    long numWrittenBytes = counterOut.getCount();
    LOG.info(String.format("Request '%s' resulted in %d bytes", target, numWrittenBytes));
    return true;
  }

  /**
   * This method returns the list of available regions that can be used for the region search option in the download section of a dataset.
   * @param target the target path requested by the client
   * @param request HTTP request
   * @param response HTTP response
   * @return {@code true} if the request was handled correctly
   * @throws IOException if an error happens while creating the response
   */
  @WebMethod(url="/regions.json", method = "get")
  public boolean listRegions(String target, HttpServletRequest request, HttpServletResponse response) throws IOException{
    String regionOptionsResource = "regions.geojson.gz";
    String codeKey = "code";
    String regionsText;

    // If a client-cached version exists, keep it
    long cacheDate = request.getDateHeader("If-Modified-Since");
    long modificationDate = new File(regionOptionsResource).lastModified();
    if (modificationDate < cacheDate) {
      LOG.info("Skipped reading regions because client has a cached copy.");
      // The cached version is OK
      response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
      return true;
    }

    InputStream is = new GZIPInputStream(new FileInputStream(regionOptionsResource));

    try ( InputStreamReader reader = new InputStreamReader(is) )
    {
      JSONParser jsonParser = new JSONParser();
      // read JSON file
      JSONObject obj = (JSONObject) jsonParser.parse(reader);

      JSONObject regionsObj = new JSONObject();
      JSONArray features = (JSONArray) obj.get("features");

      // iterate over each region to remove geometry
      features.forEach( feature -> {
        JSONObject properties = (JSONObject) ((JSONObject) feature).get("properties");
        String code = (String) properties.get(codeKey);
        properties.remove(codeKey);
        regionsObj.put(code, properties);
      } );

      // convert json to string
      StringWriter writer = new StringWriter();
      regionsObj.writeJSONString(writer);
      regionsText = writer.toString();

    } catch (IOException | ParseException e) {
      e.printStackTrace();
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return true;
    }

    // Prepare the result
    response.addDateHeader("Last-Modified", modificationDate);
    response.addDateHeader("Expires", modificationDate + AbstractWebHandler.OneDay);
    response.addHeader("Cache-Control", "max-age=86400");
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("application/json");
    if (isGZIPAcceptable(request))
      response.setHeader("Content-Encoding", "gzip");

    OutputStream out = response.getOutputStream();
    if (isGZIPAcceptable(request))
      out = new GZIPOutputStream(out);
    out.write(regionsText.getBytes());
    out.close();
    return true;
  }
}
