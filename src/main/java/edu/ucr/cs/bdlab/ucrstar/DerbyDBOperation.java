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

import java.sql.*;

public class DerbyDBOperation {
    //Set the derby database parameters
    public static String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    public static String JDBC_URL = "jdbc:derby:datasetsDB;create=true";

    //createDB creates a new derby database and inserts one piece of data
    public void createDB(){
        try{
            Class.forName(DRIVER);
            Connection connection = DriverManager.getConnection(JDBC_URL);
            //connection.createStatement().execute("derby.language.sequence.preallocator=1");
            connection.createStatement().execute("create table datasetsInfo(ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)NOT NULL," +
                    "DatasetName varchar(250)," +
                    "DatasetDescription varchar(5000)," +
                    "DatasetURL varchar(5000)," +
                    "URLType varchar(20)," +
                    "Publisher varchar(250)," +
                    "ContactEmail varchar(250)," +
                    "Homepage varchar(500)," +
                    "Format varchar(20)," +
                    "License varchar(20)," +
                    "Xcolumn varchar(20)," +
                    "Ycolumn varchar(20)," +
                    "SeparatorSymbol varchar(20)," +
                    "Tags varchar(250)," +
                    "Flag varchar(20),"+
                    "MRB_1 double," +
                    "MRB_2 double," +
                    "MRB_3 double," +
                    "MRB_4 double," +
                    "Datasetsize int," +
                    "numFeatures int," +
                    "numPoints int," +
                    "avgSideLength_1 double,"+
                    "avgSideLength_2 double)");
            System.out.println("A new derby dataset table created.");
            if(connection.createStatement() != null) connection.createStatement().close();
            if(connection != null) connection.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    //insertDB inserts one piece of data into the derby database which already exists
    public void insertDB(String datasetName, String datasetDescription, String urls, String URLstype, String publisher,
                         String contactEmail, String homepage, String format, String license, String xcolumn,
                         String ycolumn, String separatorSymbol, String tags){
        try{
            Class.forName(DRIVER);
            Connection connection = DriverManager.getConnection(JDBC_URL);
            connection.createStatement().execute("insert into datasetsInfo (DatasetName,DatasetDescription,DatasetURL,URLType,Publisher," +
                    "ContactEmail,Homepage,Format,License,Xcolumn,Ycolumn,SeparatorSymbol,Tags,Flag," +
                    "MRB_1,MRB_2,MRB_3,MRB_4,Datasetsize,numFeatures,numPoints,avgSideLength_1,avgSideLength_2) values('"+
                    datasetName + "', '" + datasetDescription + "', '" + urls + "', '" + URLstype + "', '" + publisher + "', '" +
                    contactEmail + "', '" + homepage + "', '" + format + "', '" + license + "', '" + xcolumn + "', '" +
                    ycolumn + "', '" + separatorSymbol + "', '" + tags + "','created',null,null,null,null,null,null,null,null,null)");
            System.out.println("One record successfully inserted into DataSetsInfo.");
            if(connection.createStatement() != null) connection.createStatement().close();
            if(connection != null) connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //queryDB prints out the derby database
    public void queryDB(){
        try{
            Connection connection = DriverManager.getConnection(JDBC_URL);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from datasetsInfo");
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            for(int i = 1; i <= columnCount; i++){
                System.out.format("%20s", resultSetMetaData.getColumnName(i)+" | ");
            }
            while(resultSet.next()){
                System.out.println();
                for(int i = 1; i <= columnCount; i++){
                    System.out.format("%20s", resultSet.getString(i)+" | ");
                }
            }
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //tableExist determines whether the database and the table exists
    public boolean tableExist(){
        try{
            Class.forName(DRIVER);
            Connection connection = DriverManager.getConnection(JDBC_URL);
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery("SELECT * FROM DataSetsInfo");
            while(rs!=null){
                return true;
            }
            rs.close();
            s.close();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return false;
    }

    public void updateDB(String datasetName, double [] MRB, int Datasetsize, int numFeatures, int numPoints, double [] avgSideLength){
        try{
            Class.forName(DRIVER);
            Connection connection = DriverManager.getConnection(JDBC_URL);
            connection.createStatement().execute("update datasetsInfo SET MRB_1 = " + MRB[0] +  ",MRB_2=" + MRB[1] +
                    ",MRB_3=" + MRB[2] + ",MRB_4=" + MRB[3] + ",Datasetsize=" + Datasetsize + ",numFeatures=" + numFeatures
                    + ",numPoints=" + numPoints + ",avgSideLength_1=" + avgSideLength[0] + ",avgSideLength_2=" + avgSideLength[1] +
                    " where DatasetName = " + datasetName + ";");
            System.out.println("One record successfully updated into DataSetsInfo.");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}