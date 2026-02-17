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
import org.apache.hadoop.conf.Configuration;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Properties;

public class EmailClient {

  /**SMTP host for Outlook*/
  public static final String OutlookSMTPServer = "smtp.office365.com";


  /**
   * Send a success email for the given dataset
   * @param conf
   * @param dataset
   */
  public static void sendDatasetReceivedEmail(Configuration conf, Document dataset) throws UnsupportedEncodingException, MessagingException {
    String fromEmail = conf.get(DatasetServer.Email);
    String password = conf.get(DatasetServer.Password);
    if (fromEmail == null || password == null)
      return;
    String toEmail = dataset.getString("email");

    // Create parameter configurations for connecting to Email servers
    Properties props = new Properties();
    props.setProperty("mail.transport.protocol", "smtp");
    props.setProperty("mail.smtp.host", OutlookSMTPServer);
    props.setProperty("mail.smtp.auth", "true");
    props.setProperty("mail.smtp.port", "587");
    props.setProperty("mail.smtp.starttls.enable", "true");

    // Create session objects based on configuration to interact with Email servers
    Session session = Session.getInstance(props);
    session.setDebug(true);  // Set it to debug mode to view the detailed sending log

    // Create the email
    String id = dataset.getObjectId("_id").toString();
    MimeMessage message = new MimeMessage(session);
    // From
    message.setFrom(new InternetAddress(fromEmail, "UCR-Star", "UTF-8"));

    // To
    //message.setRecipient(MimeMessage.RecipientType.TO,
    //    new InternetAddress(toEmail, "UCR-Star User", "UTF-8"));
    message.setRecipient(MimeMessage.RecipientType.CC,
        new InternetAddress("ucrstar@outlook.com", "UCR-Star Admin", "UTF-8"));
    // Subject
    message.setSubject(String.format("Dataset ID %s has been received", id));
    // Body
    String content = "Dear UCR-Star Admin,<br/>";
    content += "The following dataset has been received\n";
    content += dataset.toJson(JsonWriterSettings.builder().indent(true).build());

    message.setContent(content, "text/plain;charset=UTF-8");
    // Date
    message.setSentDate(new Date());
    // Save
    message.saveChanges();

    // Getting Email transfer objects according to Session
    Transport transport = session.getTransport();

    // Connecting Mail Server with Email address and password
    transport.connect(fromEmail, password);

    // Send mail to all listing Email addresses
    transport.sendMessage(message, message.getAllRecipients());

    // Close connection
    transport.close();
  }

  /**
   * Send a success email for the given dataset
   * @param conf
   * @param dataset
   */
  public static void sendSuccessEmail(BeastOptions conf, DatasetProcess dataset) throws UnsupportedEncodingException, MessagingException {
    String fromEmail = conf.getString(DatasetServer.Email);
    String password = conf.getString(DatasetServer.Password);
    if (fromEmail == null || password == null)
      return;
    Document document = dataset.getDataset();
    String toEmail = document.getString("email");

    // Create parameter configurations for connecting to Email servers
    Properties props = new Properties();
    props.setProperty("mail.transport.protocol", "smtp");
    props.setProperty("mail.smtp.host", OutlookSMTPServer);
    props.setProperty("mail.smtp.auth", "true");
    props.setProperty("mail.smtp.port", "587");
    props.setProperty("mail.smtp.starttls.enable", "true");

    // Create session objects based on configuration to interact with Email servers
    Session session = Session.getInstance(props);
    session.setDebug(true);  // Set it to debug mode to view the detailed sending log

    // Create the email
    String id = document.getObjectId("_id").toString();
    MimeMessage message = new MimeMessage(session);
    // From
    message.setFrom(new InternetAddress(fromEmail, "UCR-Star", "UTF-8"));

    // To
    //message.setRecipient(MimeMessage.RecipientType.TO,
    //    new InternetAddress(toEmail, "UCR-Star User", "UTF-8"));
    message.setRecipient(MimeMessage.RecipientType.CC,
        new InternetAddress("ucrstar@outlook.com", "UCR-Star Admin", "UTF-8"));
    // Subject
    message.setSubject(String.format("Dataset ID %s is processed successfully", id));
    String content = "Dear "+document.getString("authors")+",\n";
    content += "Thank you for adding your dataset to UCR-Star open geospatial archive.\n";
    content += String.format("Your dataset with ID %s has been processed succesfully.\n", id);
    content += "You can visualize your dataset at the following URL.\n";
    content += String.format("https://ucrstar.com/?%s", id);
    message.setContent(content, "text/plain;charset=UTF-8");
    // Date
    message.setSentDate(new Date());
    // Save
    message.saveChanges();

    // Getting Email transfer objects according to Session
    Transport transport = session.getTransport();

    // Connecting Mail Server with Email address and password
    transport.connect(fromEmail, password);

    // Send mail to all listing Email addresses
    transport.sendMessage(message, message.getAllRecipients());

    // Close connection
    transport.close();
  }

  /**
   * Send a success email for the given dataset
   * @param conf
   * @param dataset
   */
  public static void sendFailureEmail(BeastOptions conf, DatasetProcess dataset, Exception e) throws UnsupportedEncodingException, MessagingException {
    String fromEmail = conf.getString(DatasetServer.Email);
    String password = conf.getString(DatasetServer.Password);
    if (fromEmail == null || password == null)
      return;
    Document document = dataset.getDataset();
    String toEmail = document.getString("email");

    // Create parameter configurations for connecting to Email servers
    Properties props = new Properties();
    props.setProperty("mail.transport.protocol", "smtp");
    props.setProperty("mail.smtp.host", OutlookSMTPServer);
    props.setProperty("mail.smtp.auth", "true");
    props.setProperty("mail.smtp.port", "587");
    props.setProperty("mail.smtp.starttls.enable", "true");

    // Create session objects based on configuration to interact with Email servers
    Session session = Session.getInstance(props);
    session.setDebug(true);  // Set it to debug mode to view the detailed sending log

    // Create the email
    String id = document.getObjectId("_id").toString();
    MimeMessage message = new MimeMessage(session);
    // From
    message.setFrom(new InternetAddress(fromEmail, "UCR-Star", "UTF-8"));

    // To
    //message.setRecipient(MimeMessage.RecipientType.TO,
    //    new InternetAddress(toEmail, "UCR-Star User", "UTF-8"));
    message.setRecipient(MimeMessage.RecipientType.CC,
        new InternetAddress("ucrstar@outlook.com", "UCR-Star Admin", "UTF-8"));
    // Subject
    message.setSubject(String.format("Processing of dataset ID %s failed", id));
    String content = "Dear UCR-Star Admin,\n";
    content += String.format("The dataset with ID %s has failed to process.\n", id);
    content += "The current status of the document is given below.\n";
    content += document.toJson(JsonWriterSettings.builder().indent(true).build());
    content += "\nThe stack trace is below:\n";
    content += "Error message: "+e.getMessage();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    e.printStackTrace(ps);
    ps.close();
    content += new String(baos.toByteArray());
    message.setContent(content, "text/plain;charset=UTF-8");
    // Date
    message.setSentDate(new Date());
    // Save
    message.saveChanges();

    // Getting Email transfer objects according to Session
    Transport transport = session.getTransport();

    // Connecting Mail Server with Email address and password
    transport.connect(fromEmail, password);

    // Send mail to all listing Email addresses
    transport.sendMessage(message, message.getAllRecipients());

    // Close connection
    transport.close();
  }

}