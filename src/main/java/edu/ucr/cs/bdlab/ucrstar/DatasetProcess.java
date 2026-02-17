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
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper;
import edu.ucr.cs.bdlab.beast.indexing.RSGrovePartitioner;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.io.SpatialWriter;
import edu.ucr.cs.bdlab.beast.operations.FeatureWriterSize;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.util.FileUtil;
import edu.ucr.cs.bdlab.beast.util.Parallel;
import edu.ucr.cs.bdlab.davinci.CommonVisualizationHelper;
import edu.ucr.cs.bdlab.davinci.GeometricPlotter;
import edu.ucr.cs.bdlab.davinci.MultilevelPlot;
import edu.ucr.cs.bdlab.davinci.Plotter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Handles the processing of a new dataset added to UCR-Star.
 * This can run as a background thread.
 */
public class DatasetProcess implements Runnable {

  private static final Log LOG = LogFactory.getLog(DatasetProcess.class);

  /**User options for processing data with Spark*/
  protected BeastOptions opts;

  /** The MongoDB document that contains all the information about the dataset*/
  private final Document dataset;

  /**The Spark context for processing the data*/
  private final JavaSparkContext sc;

  /**The path in which downloaded, indexed, and visualized files should be stored*/
  private Path datasetDir;

  /**The input data RDD*/
  private JavaRDD<IFeature> inputFeatures;

  /**Overwrite output directories*/
  private boolean overwrite;

  /**Set to true to build separate indexes for data and visualization*/
  private boolean createSeparateIndexes;

  /**The file system that contains the dataset*/
  private FileSystem fileSystem;

  private static final Pattern CommandLineArg = Pattern.compile("(\'[^\']*\')|(\"[^\"]*\")|([^\"\'][^\\s]*)");

  /**The connector that accesses datasets.*/
  private DatasetConnector connector;

  /**
   * Handles the given dataset
   * @param dataset a MongoDB document that contains all dataset information
   * @param sc the spark context used to process the dataset
   */
  public DatasetProcess(Document dataset, JavaSparkContext sc, DatasetConnector connector) {
    this.connector = connector;
    this.dataset = dataset;
    this.sc = sc;
    // Read additional configuration from the document
    if (dataset.containsKey("beastOptions")) {
      Object additionalOptions = dataset.get("beastOptions");
      if (additionalOptions instanceof String) {
        // Scan the command-line arguments in the string and add them to the user options
        Matcher matcher = CommandLineArg.matcher((String) additionalOptions);
        int start = 0;
        List<String> args = new ArrayList<>();
        while (start < ((String)additionalOptions).length() && matcher.find(start)) {
          int end = matcher.end();
          String arg = matcher.group();
          if (arg.startsWith("\'") || arg.startsWith("\""))
            arg = arg.substring(1, arg.length() - 1);
          args.add(arg);
          start = end + 1;
        }
        this.opts = BeastOptions.fromStringArray(args.toArray(new String[0]));
      } else if (additionalOptions instanceof List) {
        this.opts = BeastOptions.fromStringArray((String[])(((List) additionalOptions).toArray(new String[0])));
      } else if (additionalOptions instanceof Map) {
        this.opts = new BeastOptions();
        for (Map.Entry<String, String> additionalOpt : ((Map<String, String>)additionalOptions).entrySet()) {
          this.opts.set(additionalOpt.getKey(), additionalOpt.getValue());
        }
      }
    }
  }

  /**
   * Sets up the configuration of this process
   * @param opts user options
   */
  public void setup(BeastOptions opts) {
    // Merge the passed options with the dataset options while giving precedence (priority) to the dataset options
    if (this.opts == null)
      this.opts = new BeastOptions(opts);
    else
      this.opts = new BeastOptions(opts).mergeWith(this.opts);
    this.datasetDir = new Path(opts.getString(DatasetServer.DatasetDirectory, "."),
            dataset.containsKey("name") && dataset.get("name") != null?
            dataset.getString("name") :
            dataset.getObjectId("_id").toString());
    this.overwrite = opts.getBoolean(DatasetServer.Overwrite, true);
    try {
      this.fileSystem = this.datasetDir.getFileSystem(this.opts.loadIntoHadoopConf(sc.hadoopConfiguration()));
    } catch (IOException e) {
      throw new RuntimeException(String.format("Could not obtain the file system for path '%s'", this.datasetDir), e);
    }
  }

  public Document getDataset() {
    return dataset;
  }

  public void run() {
    Path basePath = new Path(opts.getString(DatasetServer.DatasetDirectory, "."));
    try {
      switch (dataset.getString("status")) {
        case "created":
          // 1- Download the data
          LOG.info("Downloading files for "+dataset.getObjectId("_id"));
          Path tempDownload = new Path(datasetDir, "temp_download");
          fileSystem.delete(tempDownload, true);
          downloadFiles(tempDownload);
          renameAndOverwrite(fileSystem, tempDownload, getDownloadDir());
          dataset.put("status", "downloaded");
          connector.updateDocument(dataset);
          // Fall through to the next step
        case "downloaded":
          if (opts.getBoolean(DatasetServer.Decompress, true)) {
            // 2- Decompress any compressed files in the download directory
            LOG.info("Decompresseing files for " + dataset.getObjectId("_id"));
            decompressFiles();
            dataset.put("status", "decompressed");
            connector.updateDocument(dataset);
          }
          // Fall through to the next step
        case "decompressed":
          // 3- Summarize the data
          LOG.info("Computing the summary for "+dataset.getObjectId("_id"));
          computeSummary();
          dataset.put("status", "summarized");
          connector.updateDocument(dataset);
          // Fall through to next step
        case "summarized":
          // 4- Index the data
          LOG.info("Building the index for"+dataset.getObjectId("_id"));
          long pointsPerFeature = getAsLong(dataset, "num_points") / getAsLong(dataset, "num_features");
          int flattenThreshold = opts.getInt(DatasetServer.FlattenThreshold, 100);
          createSeparateIndexes = pointsPerFeature >= flattenThreshold;
          createSeparateIndexes = opts.getBoolean(DatasetServer.SeparateIndexes, createSeparateIndexes);
          if (createSeparateIndexes) {
            Path tempDataIndex = new Path(datasetDir, "temp_data_index");
            fileSystem.delete(tempDataIndex, true);
            Path tempVizIndex = new Path(datasetDir, "temp_viz_index");
            fileSystem.delete(tempVizIndex, true);
            buildIndexes(tempDataIndex, tempVizIndex);
            renameAndOverwrite(fileSystem, tempDataIndex, getDataIndexDir());
            renameAndOverwrite(fileSystem, tempVizIndex, getVizIndexDir());
          } else {
            Path tempDataIndex = new Path(datasetDir, "temp_data_index");
            fileSystem.delete(tempDataIndex, true);
            buildIndexes(tempDataIndex, null);
            renameAndOverwrite(fileSystem, tempDataIndex, getDataIndexDir());
          }

          dataset.append("index_path", FileUtil.relativize(getDataIndexDir(), basePath).toString());
          dataset.put("status", "indexed");
          connector.updateDocument(dataset);
          // Fall through to next step
        case "indexed":
          createSeparateIndexes = fileSystem.exists(new Path(datasetDir, "viz_index"));
          // 5- Visualize the data
          LOG.info("Building a visualization index for "+dataset.getObjectId("_id"));
          if (getAsLong(dataset,"size") < 1024 * 1024) {
            // A very small dataset < 1 MB, plot it using a single vector layer
            Map<String, String> visualization = new HashMap<>();
            visualization.put("type", "Vector");
            visualization.put("url", String.format("/dynamic/download.cgi/%s.geojson", FileUtil.relativize(getDataIndexDir(), basePath)));
            dataset.append("visualization", visualization);
          } else {
            Path tempPlot = new Path(datasetDir, "temp_plot.zip");
            fileSystem.delete(tempPlot, true);
            buildVisualizationIndex(tempPlot);
            Class<? extends Plotter> plotterClass = Plotter.getPlotterClass(opts.getString(CommonVisualizationHelper.PlotterName, "gplot"));
            String extension = plotterClass.getAnnotation(Plotter.Metadata.class).imageExtension();
            renameAndOverwrite(fileSystem, tempPlot, getPlotPath());
            Map<String, String> visualization = new HashMap<>();
            visualization.put("type", "Tile");
            visualization.put("url", String.format("/dynamic/visualize.cgi/%s/tile-{z}-{x}-{y}%s",
                FileUtil.relativize(getPlotPath(), basePath), extension));
            dataset.append("visualization", visualization);
          }

          dataset.put("status", "ready");
          connector.updateDocument(dataset);

          LOG.info("Successfully processed the dataset "+dataset.getObjectId("_id"));
          // Send success email
          EmailClient.sendSuccessEmail(opts, this);
          break;
        default:
          throw new RuntimeException(String.format("Cannot process dataset in status '%s'", dataset.getString("status")));
      }
    } catch (Exception e) {
      // Mark the document status accordingly
      dataset.put("status", dataset.getString("status")+"-error");
      connector.updateDocument(dataset);
      // Send failure email
      e.printStackTrace();
      try {
        EmailClient.sendFailureEmail(opts, this, e);
      } catch (Exception ex) {
        throw new RuntimeException("Error while sending the error email", ex);
      }
    }
  }

  private static long getAsLong(Document doc, String fieldName) {
    Object v = doc.get(fieldName);
    if (v instanceof Integer)
      return (Integer)v;
    if (v instanceof Long)
      return (Long)v;
    return 0;
  }

  /**
   * Rename the source directory to the destination directory and overwrite the destination directory if it exists
   * and the overwrite flag is ste.
   * @param fileSystem the file system that contains the file
   * @param src the path to the files
   * @param dest the destination path
   */
  private void renameAndOverwrite(FileSystem fileSystem, Path src, Path dest) throws IOException {
    if (fileSystem.exists(dest)) {
      if (!overwrite)
        throw new RuntimeException(String.format("Directory '%s' already exists and overwrite flag is not set", dest));
      fileSystem.delete(dest, true);
    }
    fileSystem.rename(src, dest);
  }

  /**
   * Returns the RDD of features
   * @return an RDD of the features, either loaded from the original files or from the index
   */
  private JavaRDD<IFeature> getDatasetFeatures() {
    if (this.inputFeatures == null) {
      BeastOptions readOpts = new BeastOptions(opts);
      if (dataset.getString("status").equals("indexed")) {
        // Read the indexed data
        this.inputFeatures = SpatialReader.readInput(sc, readOpts, getDataIndexDir().toString(), "rtree");
      } else {
        readOpts.setBoolean(SpatialFileRDD.Recursive(), true);
        this.inputFeatures = SpatialReader.readInput(sc, readOpts, getDownloadDir().toString(), readOpts.getString("iformat"));
      }
    }
    return this.inputFeatures;
  }

  /**
   * Download all files for this document
   * @param downloadDir the download directory for this dataset
   * @throws InterruptedException if an error happens while processing the files
   * @throws IOException if an error happens while reading or writing the data
   */
  private void downloadFiles(Path downloadDir) throws InterruptedException, IOException {
    List<String> urls = new ArrayList<>();
    if (dataset.containsKey("download_url")) {
      urls.add(dataset.getString("download_url"));
    } else {
      urls.addAll(dataset.getList("download_urls", String.class));
    }
    if (fileSystem.exists(downloadDir))
      fileSystem.delete(downloadDir, true);
    fileSystem.mkdirs(downloadDir);
    // Create a local temporary directory for downloading file
    Parallel.forEach(urls.size(), (i1, i2) -> {
      for (int i = i1; i < i2; i++) {
        String url = urls.get(i);
        try {
          // Download the file from the remote server
          downloadFileJava(url, downloadDir);
        } catch (IOException e) {
          throw new RuntimeException(String.format("Error download file '%s'", urls.get(i)), e);
        }
      }
      return null;
    });
  }

  /**
   * A regular expression that matches a valid file name
   */
  public static final String FilenameRegex = "[^\"\'\\?\\*\\/\\\\]+";

  /**
   * A regular expression that matches the filename in the HTTP response header Content-Disposition.
   */
  static final Pattern FileNameRegexHTTPHeader = Pattern.compile(
      String.format(".*filename=[\\\"\\\']?(%s)[\\\"\\\']?.*", FilenameRegex));

  /**
   * Download the given file to the download directory using a pure Java method.
   * @param url the download URL
   * @param downloadPath path to a directory to download the file into
   * @return the path of the downloaded file including the filename
   * @throws IOException if an error happens while downloading the files
   */
  public Path downloadFileJava(String url, Path downloadPath) throws IOException {
    InputStream inputStream = null;
    OutputStream out = null;
    FileSystem fs;
    Path outputFile = null;

    try {
      if (url.startsWith("http")) {
        // Handle files served on HTTP
        URL downloadURL = new URL(getDownloadURL(url));
        URLConnection urlConnection = downloadURL.openConnection();
        urlConnection.connect();
        if (urlConnection instanceof HttpURLConnection) {
          HttpURLConnection httpURLConnection = (HttpURLConnection) urlConnection;
          while (httpURLConnection.getResponseCode() == 301 || httpURLConnection.getResponseCode() == 302 || httpURLConnection.getResponseCode() == 304) {
            // Follow redirect
            downloadURL = new URL(urlConnection.getHeaderField("Location"));
            LOG.info(String.format("%d: Redirecting ... '%s'", httpURLConnection.getResponseCode(), downloadURL));
            urlConnection = httpURLConnection = (HttpURLConnection) downloadURL.openConnection();
            urlConnection.connect();
          }
          if (httpURLConnection.getResponseCode() != 200)
            throw new RuntimeException("Unexpected response code " + httpURLConnection.getResponseCode());
        }
        fs = downloadPath.getFileSystem(opts.loadIntoHadoopConf(sc.hadoopConfiguration()));
        String contentDisposition = urlConnection.getHeaderField("Content-Disposition");

        Matcher matcher;
        String filename;
        if (contentDisposition != null && (matcher = FileNameRegexHTTPHeader.matcher(contentDisposition)).matches()) {
          // Get the filename from the response header
          filename = matcher.group(1);
        } else {
          // Get the filename from the URL
          filename = new Path(url).getName();
          if (!filename.matches(FilenameRegex)) {
            // Generate a random filename that does not exist
            do {
              filename = String.format("part-%05d", (int) (Math.random() * 100000));
            } while (fs.exists(new Path(downloadPath, filename)));
          }
        }
        outputFile = new Path(downloadPath, filename);

        inputStream = new BufferedInputStream(downloadURL.openStream());

        // Create channel to the output file
        out = fs.create(outputFile, true);

        // Transfer the data
        byte[] buffer = new byte[1024 * 1024];
        int size;
        while ((size = inputStream.read(buffer)) > 0) {
          out.write(buffer, 0, size);
        }
        return outputFile;
      } else if (url.startsWith("ftp")) {
        // Handle files served on FTP
        URL ftpURL = new URL(url);
        FTPClient ftpClient = new FTPClient();
        try {
          if (ftpURL.getPort() == -1)
            ftpClient.connect(ftpURL.getHost());
          else
            ftpClient.connect(ftpURL.getHost(), ftpURL.getPort());
          if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
            throw new RuntimeException(String.format("Error connecting to FTP server at URL '%s'", ftpURL));
          }
          ftpClient.login("anonymous", null);
          ftpClient.changeWorkingDirectory(ftpURL.getPath());
          FTPFile[] files = ftpClient.listFiles(ftpURL.getPath());
          fs = downloadPath.getFileSystem(opts.loadIntoHadoopConf(sc.hadoopConfiguration()));
          for (FTPFile file : files) {
            if (file.isFile()) {
              // Download this file
              inputStream = new BufferedInputStream(ftpClient.retrieveFileStream(file.getName()));
              try {
                outputFile = new Path(downloadPath, file.getName());
                // Create channel to the output file
                out = fs.create(outputFile, true);
                byte[] buffer = new byte[1024 * 1024];
                int size;
                while ((size = inputStream.read(buffer)) > 0) {
                  out.write(buffer, 0, size);
                }
              } finally {
                inputStream.close();
                inputStream = null;
              }
            }
          }
        } finally {
          if (ftpClient.isConnected())
            ftpClient.disconnect();
        }
        return outputFile;
      }
      throw new RuntimeException(String.format("Unsupported protocol for URL '%s'", url));
    } finally {
      if (inputStream != null)
        inputStream.close();
      if (out != null)
        out.close();
    }
  }

  /**A list of regular expressions for Google Drive links with file ID*/
  static final Pattern[] GoogleIDRegex = {
      Pattern.compile(".*id=([^\\/\\&]+).*"),
      Pattern.compile(".*file/d/([^\\/\\&]+).*"),
  };

  /**A list of regular expressions for Dropbox links with file ID*/
  static final Pattern[] DropboxIDRegex = {
      Pattern.compile(".*dropbox.com/([^\\&\\?]+).*"),
  };

  /**
   * Get the URL form which the file can be downloaded directly using a simple HTTP GET command.
   * @param url the URL given by the user
   * @return an updated URL that can be used to access the file
   */
  private static String getDownloadURL(String url) {
    if (url.contains("drive.google.com")) {
      // Google drive link
      for (Pattern googleDrive : GoogleIDRegex) {
        Matcher matcher = googleDrive.matcher(url);
        if (matcher.matches()) {
          String googleID = matcher.group(1);
          String google_url = String.format("https://docs.google.com/uc?export=download&id=%s", googleID);
          return google_url;
        }
      }
      throw new RuntimeException(String.format("Cannot parse Google URL '%s'", url));
    } else if (url.contains("onedrive.live.com")) {
      // OneDrive link
      String one_url = url.substring(url.indexOf("file/d/"));
      one_url = one_url.substring(7).trim();
      one_url = String.format("https://onedrive.live.com/download?cid=%s", one_url);
      return one_url;
    } else if (url.contains("dropbox.com")) {
      // Dropbox link
      for (Pattern dropboxLink : DropboxIDRegex) {
        Matcher matcher = dropboxLink.matcher(url);
        if (matcher.matches()) {
          String dropboxID = matcher.group(1);;
          String dropboxURL = String.format("https://www.dropbox.com/%s?dl=1", dropboxID);
          return dropboxURL;
        }
      }
      throw new RuntimeException(String.format("Cannot parse Dropbox URL '%s'", url));
    } else {
      // Direct link
      return url;
    }
  }

  private Path getDownloadDir() {
    return new Path(datasetDir, "download");
  }

  /**
   * Decompress any downloaded files
   * @throws IOException if an error happens while reading/writing the files
   */
  private void decompressFiles() throws IOException {
    Path downloadDir = getDownloadDir();
    FileSystem fs = downloadDir.getFileSystem(opts.loadIntoHadoopConf(sc.hadoopConfiguration()));
    boolean compressedFiles;
    do {
      compressedFiles = false;
      for (FileStatus fileStatus : fs.listStatus(downloadDir)) {
        if (fileStatus.isFile() && FileUtil.extensionMatches(fileStatus.getPath().getName(), ".zip")) {
          compressedFiles = true;
          // Decompress a ZIP file
          String filename = fileStatus.getPath().getName();
          int iLastDot = filename.lastIndexOf('.');
          if (iLastDot != -1)
            filename = filename.substring(0, iLastDot);
          else
            filename = filename+"_extracted";
          Path extractDir = new Path(fileStatus.getPath().getParent(), filename);
          fs.mkdirs(extractDir);
          // Open the Zip file
          // There is a problem with reading some ZIP files in a streaming fashion
          // https://stackoverflow.com/questions/47208272
          // To work around it, we copy the file to the local file system first
          File localZipFileName;
          if (!(fs instanceof LocalFileSystem)) {
            // Copy the file to the local file system
            localZipFileName = File.createTempFile("tempdownload", "zip");
            fs.copyToLocalFile(fileStatus.getPath(), new Path(localZipFileName.getPath()));
          } else {
            // Already stored locally
            localZipFileName = new File(fileStatus.getPath().toUri().getPath());
          }
          ZipFile zipFile = new ZipFile(localZipFileName);
          try {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
              ZipEntry zipEntry = entries.nextElement();
              String expandedFileName = zipEntry.getName();
              if (expandedFileName.endsWith("/")) {
                // ZIP entry represents a directory. Skip over it
              } else {
                Path expandedFile = new Path(extractDir, expandedFileName);
                FSDataOutputStream out = fs.create(expandedFile);
                try (InputStream zinStream = zipFile.getInputStream(zipEntry)) {
                  byte[] buffer = new byte[1024 * 1024];
                  int size;
                  while ((size = zinStream.read(buffer)) > 0) {
                    out.write(buffer, 0, size);
                  }
                } finally {
                  out.close();
                }
              }
            }
          } finally {
            zipFile.close();
          }
          // Delete the ZIP file after it is already expanded
          fs.delete(fileStatus.getPath(), false);
        }
      }
    } while (compressedFiles);
  }

  /**
   * Returns the path to the data index
   * @return the path of the data index
   */
  private Path getDataIndexDir() {
    return new Path(datasetDir, "data_index");
  }

  /**
   * Returns the path to the visualization index.
   * If no separate indexes are required, it returns the same path to the data index.
   * @return the path of the visualization index
   */
  private Path getVizIndexDir() {
    return createSeparateIndexes? new Path(datasetDir, "viz_index") : getDataIndexDir();
  }

  private Path getPlotPath() {
    return new Path(datasetDir, "plot.zip");
  }

  /**
   * Convert the integer numeric value to (int) if possible to make the internal representation more concise.
   * @param doc the MongoDB document
   * @param name the name of the field
   * @param value the value of the field
   */
  private static void putNumeric(Document doc, String name, long value) {
    if (value <= Integer.MAX_VALUE)
      doc.put(name, (int) value);
    else
      doc.put(name, value);
  }

  /**
   * Compute the summary of the dataset and update the MongoDB document.
   */
  private void computeSummary() {
    // 1- Add summary information
    Summary summary = Summary.computeForFeatures(getDatasetFeatures());
    // Convert all numeric values to integer if possible to make the internal formal more concise
    putNumeric(dataset, "size", summary.size());
    putNumeric(dataset, "num_features", summary.numFeatures());
    if (summary.numPoints() != summary.numFeatures())
      putNumeric(dataset, "num_points", summary.numPoints());
    dataset.put("geometry_type", summary.geometryType().toString());

    List<Double> mbr = new ArrayList<>(4);
    mbr.add(summary.getMinCoord(0));
    mbr.add(summary.getMinCoord(1));
    mbr.add(summary.getMaxCoord(0));
    mbr.add(summary.getMaxCoord(1));
    dataset.put("extent", mbr);

    if (summary.averageSideLength(0) + summary.averageSideLength(1) > 0) {
      List<Double> avgSideLength = new ArrayList<>();
      avgSideLength.add(summary.averageSideLength(0));
      avgSideLength.add(summary.averageSideLength(1));
      dataset.put("avg_sidelength", avgSideLength);
    }

    // 2- Add attribute information
    IFeature sampleFeature = getDatasetFeatures().first();
    if (sampleFeature.length() > 1) {
      List<Map> attributes = new ArrayList<>();
      for (int iAttr : sampleFeature.iNonGeomJ()) {
        Map<String, String> attribute = new HashMap<>();
        attribute.put("name", sampleFeature.getName(iAttr));
        Object value = sampleFeature.get(iAttr);
        Class<?> valueClass = value == null? null : value.getClass();
        String type;
        if (value == null)
          type = "unknown";
        else if (valueClass == String.class)
          type = "string";
        else if (valueClass == Integer.class || valueClass == Long.class)
          type = "integer";
        else if (valueClass == Float.class || valueClass == Double.class)
          type = "number";
        else if (valueClass == Boolean.class)
          type = "boolean";
        else
          type = "unknown";
        attribute.put("type", type);
        attributes.add(attribute);
      }
      dataset.put("attributes", attributes);
    }
  }

  public void buildIndexes() throws IOException {
    Path basePath = new Path(opts.getString(DatasetServer.DatasetDirectory, "."));

    LOG.info("Building the index for"+dataset.getObjectId("_id"));
    int flattenThreshold = opts.getInt(DatasetServer.FlattenThreshold, 100);
    long pointsPerFeature = getAsLong(dataset, "num_points") / getAsLong(dataset, "num_features");
    createSeparateIndexes = pointsPerFeature >= flattenThreshold;
    createSeparateIndexes = opts.getBoolean(DatasetServer.SeparateIndexes, createSeparateIndexes);
    if (createSeparateIndexes) {
      Path tempDataIndex = new Path(datasetDir, "temp_data_index");
      fileSystem.delete(tempDataIndex, true);
      Path tempVizIndex = new Path(datasetDir, "temp_viz_index");
      fileSystem.delete(tempVizIndex, true);
      buildIndexes(tempDataIndex, tempVizIndex);
      renameAndOverwrite(fileSystem, tempDataIndex, getDataIndexDir());
      renameAndOverwrite(fileSystem, tempVizIndex, getVizIndexDir());
    } else {
      Path tempDataIndex = new Path(datasetDir, "temp_data_index");
      fileSystem.delete(tempDataIndex, true);
      buildIndexes(tempDataIndex, null);
      renameAndOverwrite(fileSystem, tempDataIndex, getDataIndexDir());
    }

    dataset.append("index_path", FileUtil.relativize(getDataIndexDir(), basePath).toString());
    dataset.put("status", "indexed");
    connector.updateDocument(dataset);
  }

  /**
   * Builds an R-tree index on the input to prepare it for visualization.
   * @param dataIndexDir the index directory
   * @param vizImageDir the index for the visualization
   */
  private void buildIndexes(Path dataIndexDir, Path vizImageDir) {
    // Partition the input records using the created partitioner
    BeastOptions indexOpts = new BeastOptions(opts);
    indexOpts.setBoolean(IndexHelper.BalancedPartitioning(), true);
    indexOpts.set(SpatialWriter.OutputFormat(), "rtree");
    indexOpts.setBoolean(SpatialFileRDD.Recursive(), true);
    LOG.info(String.format("Index command 'beast index %s'", indexOpts));
    JavaRDD<IFeature> partitionedData = IndexHelper.partitionFeatures2(getDatasetFeatures(),
            RSGrovePartitioner.class, new FeatureWriterSize(indexOpts), indexOpts);
    // Save the index to disk
    IndexHelper.saveIndex2J(partitionedData, dataIndexDir.toString(), indexOpts);
    if (vizImageDir != null) {
      LOG.info("Building an index for visualization");
      int flattenThreshold = opts.getInt(DatasetServer.FlattenThreshold, 100);
      JavaRDD<IFeature> strippedFeatures = getDatasetFeatures()
          .filter(f -> f.getGeometry() != null)
          .flatMap(f -> new GeometryBreaker(f.getGeometry(), flattenThreshold))
          .filter(g -> !g.isEmpty() && g.getEnvelopeInternal().intersects(CommonVisualizationHelper.MercatorMapBoundariesEnvelope))
          .map(geometry -> Feature.create(null, geometry));
      JavaRDD<IFeature> partitionedData2 = IndexHelper.partitionFeatures2(strippedFeatures,
              RSGrovePartitioner.class, new FeatureWriterSize(indexOpts), indexOpts);
      IndexHelper.saveIndex2J(partitionedData2, vizImageDir.toString(), indexOpts);
    }
  }

  /**
   * Builds the multilevel visualization index
   * @param plotPath the path of the plotted data
   * @throws IOException if an error happens while building the visualization
   */
  private void buildVisualizationIndex(Path plotPath) throws IOException {
    BeastOptions vizOpts = new BeastOptions(opts);
    vizOpts.setInt(MultilevelPlot.NumLevels(), 20);
    if (!vizOpts.contains(CommonVisualizationHelper.PlotterName))
      vizOpts.set(CommonVisualizationHelper.PlotterName, "gplot");
    vizOpts.setBoolean(GeometricPlotter.Antialiasing, true);
    vizOpts.setInt(GeometricPlotter.PointSize, 6);
    vizOpts.set(SpatialFileRDD.InputFormat(), "rtree");
    vizOpts.setBoolean(CommonVisualizationHelper.UseMercatorProjection, true);
    Path vizIndexPath = getVizIndexDir();
    LOG.info(String.format("Visualization command 'beast mplot %s %s %s'", vizIndexPath, plotPath, vizOpts));
    sc.setJobGroup("Visualize", "Build a visualization for the dataset");
    MultilevelPlot.run(vizOpts, new String[] {vizIndexPath.toString()}, new String[] {plotPath.toString()}, sc.sc());
  }
}