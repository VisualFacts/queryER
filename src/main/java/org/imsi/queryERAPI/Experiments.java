package org.imsi.queryERAPI;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.imsi.queryEREngine.apache.calcite.jdbc.CalciteConnection;
import org.imsi.queryEREngine.apache.calcite.sql.parser.SqlParseException;
import org.imsi.queryEREngine.apache.calcite.tools.RelConversionException;
import org.imsi.queryEREngine.apache.calcite.tools.ValidationException;
import org.imsi.queryEREngine.imsi.calcite.util.DeduplicationExecution;
import org.imsi.queryEREngine.imsi.er.ConnectionPool.CalciteConnectionPool;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.IdDuplicates;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.ComparisonRefinement.AbstractDuplicatePropagation;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.ComparisonRefinement.UnilateralDuplicatePropagation;
import org.imsi.queryEREngine.imsi.er.Utilities.BlockStatistics;
import org.imsi.queryEREngine.imsi.er.Utilities.DumpDirectories;
import org.imsi.queryEREngine.imsi.er.Utilities.ExecuteBlockComparisons;
import org.imsi.queryEREngine.imsi.er.Utilities.SerializationUtilities;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import au.com.bytecode.opencsv.CSVWriter;


public class Experiments {

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

	private static final String QUERY_FILE_PROPERTY = "query.filepath";
	private static final String QUERY_TOTAL_RUNS = "query.runs";
	private static final String SCHEMA_NAME = "schema.name";
	private static final String CALCITE_CONNECTION = "calcite.connection";
	private static final String CALCULATE_GROUND_TRUTH = "ground_truth.calculate";
	private static final String DIVIDE_GROUND_TRUTH = "ground_truth.divide";
	private static final String DUMP_PATH = "dump.path";

	private static String queryFilePath = "";
	private static String dumpPath = "";
	private static Integer groundTruthDivide = 500;
	private static Integer totalRuns = 1;
	private static String schemaName = "";
	private static String calciteConnectionString = "";
	private static Boolean calculateGroundTruth = false;
	private static CalciteConnectionPool calciteConnectionPool = null;
	private static DumpDirectories dumpDirectories  = null;

	public static void main(String[] args)
			throws  ClassNotFoundException, SQLException, ValidationException, RelConversionException, SqlParseException, IOException
	{
		setProperties();
		// Create output folders
		dumpDirectories = new DumpDirectories(dumpPath);
		dumpDirectories.generateDumpDirectories();
		// Create Connection
		calciteConnectionPool = new CalciteConnectionPool();
		CalciteConnection calciteConnection = null;
		try {
			calciteConnection = (CalciteConnection) calciteConnectionPool.setUp(calciteConnectionString);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Enter a query or read query from file
		List<String> queries = new ArrayList<>();
		File resultDir = new File("./data/queryResults");
		if(resultDir.exists()) {
			FileUtils.cleanDirectory(resultDir); //clean out directory (this is optional -- but good know)
			FileUtils.forceDelete(resultDir); //delete directory
			FileUtils.forceMkdir(resultDir); //create directory
		}
		else FileUtils.forceMkdir(resultDir); //create directory
		File queryFile = new File("./data/queryResults/queryResults.csv");
		FileWriter csvWriter = new FileWriter(queryFile);
		csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,entities_in_blocks,detected_duplicates,PC,PQ\n");
		initializeDB(calciteConnection, schemaName);
		if(queryFilePath == null) {
			while(true) {
				String query = readQuery();
				Double runTime = 0.0;
				double queryStartTime = System.currentTimeMillis();
				try {
					ResultSet queryResults = runQuery(calciteConnection, query);
					double queryEndTime = System.currentTimeMillis();
					runTime = (queryEndTime - queryStartTime)/1000;
					//printQueryContents(queryResults);
					//exportQueryContent(queryResults, "./data/queryResults.csv");
					if(calculateGroundTruth)
						calculateGroundTruth(calciteConnection, query, csvWriter);
					System.out.println("Finished query, time: " + runTime);
				}
				catch(Exception e) {
					e.printStackTrace();
				}


			}
		}
		else {
			readQueries(queries, queryFilePath);
			runQueries(calciteConnection, queries, totalRuns, schemaName);
		}

	}


	private static void initializeDB(CalciteConnection calciteConnection, String schemaName2) throws SQLException {
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
		runQuery(calciteConnection, query);
		writeHeader();
		System.out.println("Initializing Finished!");

	}

	public static void writeHeader(){
		try {
			FileWriter logWriter = new FileWriter(dumpDirectories.getNewLogsDirPath());
			logWriter.write("table_name,query_entities,table_scan_time,links_time,block_join_time,blocking_time,query_blocks,max_query_block_size,avg_query_block_size,total_query_comps,block_entities,"
					+ "purge_blocks,purge_time,max_purge_block_size,avg_purge_block_size,total_purge_comps,purge_entities,filter_blocks,filter_time,max_filter_block_size,avg_filter_block_size,"
					+ "total_filter_comps,filter_entities,ep_time,ep_comps,ep_entities,matches_found,executed_comparisons,jaro_time,comparison_time,rev_uf_creation_time,total_entities,total_dedup_time\n");
			logWriter.close();
		} catch (IOException e) {
			System.out.println("Log file creation error occurred.");
			e.printStackTrace();
		}
	}

	private static void readQueries(List<String> queries, String queryFilePath) {
		// read query line by line
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(queryFilePath));
			String line = reader.readLine();

			while (line != null) {
				if(!line.contains("--") && !line.isEmpty()) {
					queries.add(line);
					// read next line
				}
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	private static String readQuery() throws IOException {
//		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		Scanner scanner = new Scanner(System.in).useDelimiter("\n");
		System.out.println("Enter a query: ");
		String query = "";
		String line;
		query = scanner.next();
//		while ((line = scanner.next()) != null && line.length()!= 0) {
//			query += line;
//		}
		query = query.replaceAll("[\r\n]+", " ");
//		scanner.close();
		return query;
	}

	private static void runQueries(CalciteConnection calciteConnection, List<String> queries,
								   Integer totalRuns, String schemaName) throws IOException, SQLException {
		int index = 1;

		File resultDir = new File("./data/queryResults");
		if(resultDir.exists()) {
			FileUtils.cleanDirectory(resultDir); //clean out directory (this is optional -- but good know)
			FileUtils.forceDelete(resultDir); //delete directory
			FileUtils.forceMkdir(resultDir); //create directory
		}
		else FileUtils.forceMkdir(resultDir); //create directory
		File blocksDir = new File("./data/blocks");
		if(blocksDir.exists()) {
			FileUtils.cleanDirectory(blocksDir); //clean out directory (this is optional -- but good know)
			FileUtils.forceDelete(blocksDir); //delete directory
			FileUtils.forceMkdir(blocksDir); //create directory
		}
		else FileUtils.forceMkdir(blocksDir); //create directory

		File queryFile = new File("./data/queryResults/queryResults.csv");
		FileWriter csvWriter = new FileWriter(queryFile);
		//csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,total_entities,entities_in_blocks,singleton_entities,average_block,BC,detected_duplicates,PC,PQ\n");
		csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,entities_in_blocks,detected_duplicates,PC,PQ\n");
		for(String query : queries) {
			double totalRunTime = 0.0;
			ResultSet queryResults = null;
			for(int i = 0; i < totalRuns; i++) {
				Double runTime = 0.0;
				double queryStartTime = System.currentTimeMillis();
				queryResults = runQuery(calciteConnection, query);
				//printQueryContents(queryResults);
				//exportQueryContent(queryResults, "./data/queryResults" + index + ".csv");
				//sizeQueryContents(queryResults);
				double queryEndTime = System.currentTimeMillis();
				runTime = (queryEndTime - queryStartTime)/1000;
				totalRunTime += runTime;
			}
			csvWriter.append("\"" + query + "\"" + "," + totalRuns + "," + totalRunTime/totalRuns + ",");
			System.out.println("Finished query: " + index + " runs: " + totalRuns + " time: " + totalRunTime/totalRuns);
			// Get the ground truth for this query
			if(calculateGroundTruth)
				calculateGroundTruth(calciteConnection, query, csvWriter);
			csvWriter.append("\n");
			csvWriter.flush();
			index ++;
		}
		csvWriter.close();
	}

	private static void sizeQueryContents(ResultSet resultSet) throws SQLException {
		int count = 0;
		while (resultSet.next()) {
			//Print one row
			count++;
		}
		System.out.println(count);
	}


	private static ResultSet runQuery(CalciteConnection calciteConnection, String query) throws SQLException {
		//System.out.println("Running query...");
		return calciteConnection.createStatement().executeQuery(query);

	}

	public static CsvParser openCsv(String tablePath) throws IOException {
		// The settings object provides many configuration options
		CsvParserSettings parserSettings = new CsvParserSettings();
		//You can configure the parser to automatically detect what line separator sequence is in the input
		parserSettings.setNullValue("");
		parserSettings.setEmptyValue("");
		parserSettings.setDelimiterDetectionEnabled(true);
		CsvParser parser = new CsvParser(parserSettings);
		parser.beginParsing(new File(tablePath), Charset.forName("US-ASCII"));
		parser.parseNext();  //skip header row
		return parser;
	}

	@SuppressWarnings("unchecked")
	private static void calculateGroundTruth(CalciteConnection calciteConnection, String query, FileWriter csvWriter) throws SQLException, IOException {

		// Trick to get table name from a single sp query
		if(!query.contains("DEDUP")) return;
		final String tableName;
		if(query.indexOf("WHERE") != -1) {
			tableName = query.substring(query.indexOf(schemaName) + schemaName.length() + 1  , query.indexOf("WHERE")).trim();;
		}
		else {
			tableName = query.substring(query.indexOf(schemaName) + schemaName.length() + 1, query.length()).trim();;
		}
		String name = query.replace("'", "").replace("*","ALL").replace(">", "BIGGER").replace("<", "LESS");
		//OffsetIdsMap offsetIdsMap = offsetToIds(tableName);

//    	HashMap<Integer, Integer> offsetToId = offsetIdsMap.offsetToId;
//    	HashMap<Integer, Integer> idsToOffset = offsetIdsMap.idToOffset;
		// Construct ground truth query
		Set<IdDuplicates> groundDups = new HashSet<IdDuplicates>();
		Set<String> groundMatches = new HashSet<>();
		File blocksDir = new File(dumpDirectories.getGroundTruthDirPath() + name);
		if(blocksDir.exists()) {
			groundDups = (Set<IdDuplicates>) SerializationUtilities.loadSerializedObject(dumpDirectories.getGroundTruthDirPath() + name);
		}
		else {
			System.out.println("Calculating ground truth..");

			CalciteConnection qCalciteConnection = null;
			try {
				qCalciteConnection = (CalciteConnection) calciteConnectionPool.setUp(calciteConnectionString);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			Set<Integer> qIds = DeduplicationExecution.qIds;
			List<Set<Integer>> inIdsSets = new ArrayList<>();
			Set<Integer> currSet = null;
			for (Integer value : qIds) {
				if (currSet == null || currSet.size() == groundTruthDivide)
					inIdsSets.add(currSet = new HashSet<>());
				currSet.add(value);
			}
			List<String> inIds = new ArrayList<>();
			inIdsSets.forEach(inIdSet -> {
				String inId = "(";
				for(Integer qId : inIdSet) {
					inId += qId + ",";
				}
				inId = inId.substring(0, inId.length() - 1) + ")";
				inIds.add(inId);
			});
			System.out.println("Will execute " + inIds.size() + " queries");

			for(String inIdd : inIds) {
				String groundTruthQuery = "SELECT id_d, id_s FROM " + schemaName + ".ground_truth_" + tableName +
						" WHERE id_s IN " + inIdd + " OR id_d IN " + inIdd ;
				ResultSet gtQueryResults = runQuery(calciteConnection, groundTruthQuery);
				while (gtQueryResults.next()) {
					Integer id_d = Integer.parseInt(gtQueryResults.getString("id_d"));
					Integer id_s = Integer.parseInt(gtQueryResults.getString("id_s"));
					IdDuplicates idd = new IdDuplicates(id_d, id_s);
					groundDups.add(idd);
//					String uniqueComp = "";
//					if (id_d > id_s)
//						uniqueComp = id_d + "u" + id_s;
//					else
//						uniqueComp = id_s + "u" + id_d;
//					if (groundMatches.contains(uniqueComp))
//						continue;
//					groundMatches.add(uniqueComp);
				}
			}
			SerializationUtilities.storeSerializedObject(groundDups, dumpDirectories.getGroundTruthDirPath() + name);
		}


		final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(groundDups);
		System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());
		List<AbstractBlock> blocks = DeduplicationExecution.blocks;
		duplicatePropagation.resetDuplicates();
		BlockStatistics bStats = new BlockStatistics(blocks, duplicatePropagation, csvWriter);
		bStats.applyProcessing();

		Set<String> matches = ExecuteBlockComparisons.matches;
		double sz_before = matches.size();
		matches.removeAll(groundMatches);
		double sz_after = matches.size();
		//System.out.println("ACC\t:\t " + sz_after/sz_before);
		csvWriter.flush();

	}

	private static void exportQueryContent(ResultSet queryResults, String path) throws SQLException, IOException {
		CSVWriter writer = new CSVWriter(new FileWriter(path),',');
		writer.writeAll(queryResults, true);
		writer.close();

	}


	private static void printQueryContents(ResultSet resultSet) throws SQLException {
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		while (resultSet.next()) {
			//Print one row
			for(int i = 1 ; i <= columnsNumber; i++){
				System.out.print(resultSet.getString(i) + " || "); //Print one element of a row
			}
			System.out.println();//Move to the next line to print the next row.
		}
	}


	private static void setProperties() {
		properties = loadProperties();
		if(!properties.isEmpty()) {
			queryFilePath = properties.getProperty(QUERY_FILE_PROPERTY);
			totalRuns = Integer.parseInt(properties.getProperty(QUERY_TOTAL_RUNS));
			schemaName = properties.getProperty(SCHEMA_NAME);
			calciteConnectionString = properties.getProperty(CALCITE_CONNECTION);
			if(calciteConnectionString == null) {
				URL res = Experiments.class.getClassLoader().getResource("model.json");
				File file = null;
				try {
					file = Paths.get(res.toURI()).toFile();
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				calciteConnectionString = "jdbc:calcite:model=" + file.getAbsolutePath();
			}
			calculateGroundTruth = Boolean.parseBoolean(properties.getProperty(CALCULATE_GROUND_TRUTH));
			groundTruthDivide = Integer.parseInt(properties.getProperty(DIVIDE_GROUND_TRUTH));
			dumpPath = properties.getProperty(DUMP_PATH);
		}
	}

	private static Properties loadProperties() {

		Properties prop = new Properties();

		try (InputStream input =  Experiments.class.getClassLoader().getResourceAsStream(pathToPropertiesFile)) {
			// load a properties file
			prop.load(input);

		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return prop;
	}





}
