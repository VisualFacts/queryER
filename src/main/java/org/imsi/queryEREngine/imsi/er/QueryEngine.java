package org.imsi.queryEREngine.imsi.er;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;


import org.imsi.queryEREngine.apache.calcite.jdbc.CalciteConnection;
import org.imsi.queryEREngine.imsi.er.ConnectionPool.CalciteConnectionPool;
import org.imsi.queryEREngine.imsi.er.Utilities.DumpDirectories;

import au.com.bytecode.opencsv.CSVWriter;

public class QueryEngine {

	/**
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws ValidationException
	 * @throws RelConversionException
	 * @throws SqlParseException
	 * Main function of the program. 
	 * @throws IOException 
	 */
	private static String pathToPropertiesFile = "config.properties";
	private static Properties properties;

	private static final String SCHEMA_NAME = "schema.name";
	private static final String CALCITE_CONNECTION = "calcite.connection";
	private static final String DUMP_PATH = "dump.path";

	
	private static String schemaName = "";
	private static String dumpPath = "";
	private static String calciteConnectionString = "";
	private static CalciteConnectionPool calciteConnectionPool = null;
	private CalciteConnection calciteConnection = null;
	private static DumpDirectories dumpDirectories  = null;


	public void initialize() throws IOException, SQLException {
		setProperties();
		// Create output folders
		dumpDirectories = new DumpDirectories(dumpPath);
		dumpDirectories.generateDumpDirectories();
		dumpDirectories.storeDumpMap();
		// Create Connection
		calciteConnectionPool = new CalciteConnectionPool();
		CalciteConnection calciteConnection = null;
		try {
			calciteConnection = (CalciteConnection) calciteConnectionPool.setUp(calciteConnectionString);
			this.calciteConnection = calciteConnection;
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		initializeDB(calciteConnection, schemaName);
	}

	private void initializeDB(CalciteConnection calciteConnection, String schemaName2) throws SQLException {
		System.out.println("Initializing Database...");
		Set<String> schemas = calciteConnection.getRootSchema().getSubSchemaNames();
		HashMap<String, Set<String>> tables = new HashMap<>();
		for(String schemaName : schemas) {
			if(schemaName.contentEquals("metadata") || schemaName.contentEquals("test")) continue;
			Set<String> tablesInSchema = calciteConnection.getRootSchema().getSubSchema(schemaName).getTableNames();
			tables.put(schemaName, tablesInSchema);
		}
		String fSchema = "";
		String fTable = "";
		for(String schema : tables.keySet()) {
			Set<String> tablesInSchema = tables.get(schema);
			boolean flag = false;
			for(String table: tablesInSchema)
				if(!table.contains("/")) {
					fSchema = schema;
					fTable = table;
					flag = true;
					break;
				}
			if(flag) break;
		}
		String query = "SELECT 1 FROM " + fSchema + "." + fTable;
		runQuery(query);	
		System.out.println("Initializing Finished!");

	}


	public ResultSet runQuery(String query) throws SQLException {
		System.out.println("Running query...");
		ResultSet resultSet;
		Connection connection = calciteConnectionPool.getConnection();
		
		double queryStartTime = System.currentTimeMillis();
		resultSet = connection.createStatement().executeQuery(query);
		double queryEndTime = System.currentTimeMillis();
		double runTime = (queryEndTime - queryStartTime)/1000;
		System.out.println("Finished query, time: " + runTime);	
		return resultSet;
		
	}
	
	

	private void setProperties() {
		properties = loadProperties();
		if(!properties.isEmpty()) {
            schemaName = properties.getProperty(SCHEMA_NAME);
            calciteConnectionString = properties.getProperty(CALCITE_CONNECTION);
			dumpPath = properties.getProperty(DUMP_PATH);
		}
	}
	
	private Properties loadProperties() {
		
        Properties prop = new Properties();
       
		try (InputStream input = new FileInputStream(pathToPropertiesFile)){
            // load a properties file
            prop.load(input);
                       
        } catch (IOException ex) {
            ex.printStackTrace();
        }
		return prop;
	}
	
}
