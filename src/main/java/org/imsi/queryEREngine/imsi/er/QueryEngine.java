package org.imsi.queryEREngine.imsi.er;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.imsi.queryEREngine.apache.calcite.jdbc.CalciteConnection;
import org.imsi.queryEREngine.apache.calcite.util.Source;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvEnumerator;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvSchema;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvTranslatableTable;
import org.imsi.queryEREngine.imsi.calcite.util.DeduplicationExecution;
import org.imsi.queryEREngine.imsi.er.ConnectionPool.CalciteConnectionPool;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.DecomposedBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.IdDuplicates;
import org.imsi.queryEREngine.imsi.er.DataStructures.UnilateralBlock;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.ComparisonRefinement.AbstractDuplicatePropagation;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.ComparisonRefinement.UnilateralDuplicatePropagation;
import org.imsi.queryEREngine.imsi.er.Utilities.BlockStatistics;
import org.imsi.queryEREngine.imsi.er.Utilities.DumpDirectories;
import org.imsi.queryEREngine.imsi.er.Utilities.ExecuteBlockComparisons;
import org.imsi.queryEREngine.imsi.er.Utilities.OffsetIdsMap;
import org.imsi.queryEREngine.imsi.er.Utilities.SerializationUtilities;
import org.imsi.queryEREngine.apache.calcite.schema.Table;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import au.com.bytecode.opencsv.CSVWriter;
import net.minidev.json.JSONArray;
import net.minidev.json.parser.JSONParser;

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
	private static final String CALCULATE_GROUND_TRUTH = "ground_truth.calculate";
	private static final String DIVIDE_GROUND_TRUTH = "ground_truth.divide";
	
	private static String schemaName = "";
	private static String dumpPath = "";
	private static String calciteConnectionString = "";
	private static Integer groundTruthDivide = 500;
	private static Boolean calculateGroundTruth = false;
	private static CalciteConnectionPool calciteConnectionPool = null;
	private static DumpDirectories dumpDirectories  = null;


	public QueryEngine(String modelPath) {
		calciteConnectionString = "jdbc:calcite:model=" + modelPath;
		
	}

	public QueryEngine() {
		super();
	}

	public void initialize() throws IOException, SQLException {
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
			e1.printStackTrace();
		}
		initializeDB(calciteConnection);
	}

	private void initializeDB(CalciteConnection calciteConnection) throws SQLException {
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
		if(calculateGroundTruth)
			try {
				calculateGroundTruth(query);
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		return resultSet;
		
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
	
	private static OffsetIdsMap offsetToIds(String tableName) throws IOException {
    	
		Map<String, Table> tableMap = CsvSchema.tableMap;
		Table table = tableMap.get(tableName);
		CsvTranslatableTable csvTable = (CsvTranslatableTable) table;
        CsvParser parser = openCsv(csvTable.getSource().path());
        String[] row;
        HashMap<Integer, Integer> offsetToId = new HashMap<>();
        HashMap<Integer, Integer> idToOffset = new HashMap<>();
        
    	long rowOffset = parser.getContext().currentChar() - 1;
        while ((row = parser.parseNext()) != null) {
        	int rowOffsetInt = (int) rowOffset;
        	try {
	        	Integer id = Integer.parseInt(row[csvTable.getKey()]);
	        	offsetToId.put(rowOffsetInt, id);
//	        	System.out.print(rowOffsetInt + " = ");
//	        	for(String s : row) System.out.print(s + ", ");
//	        	System.out.println();
	        	idToOffset.put(id, rowOffsetInt);
        	}
        	catch(Exception e) {
        	}
        	
        	rowOffset = parser.getContext().currentChar() - 1;
        }
        return new OffsetIdsMap(offsetToId, idToOffset);
    }
	
	@SuppressWarnings("unchecked")
	private void calculateGroundTruth(String query) throws SQLException, IOException {
        File queryFile = new File("queryResults.csv");
        FileWriter csvWriter = new FileWriter(queryFile);
        //csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,total_entities,entities_in_blocks,singleton_entities,average_block,BC,detected_duplicates,PC,PQ\n");
        csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,entities_in_blocks,detected_duplicates,PC,PQ\n");
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
		OffsetIdsMap offsetIdsMap = offsetToIds(tableName);
    	
    	HashMap<Integer, Integer> offsetToId = offsetIdsMap.offsetToId;
    	HashMap<Integer, Integer> idsToOffset = offsetIdsMap.idToOffset;
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
			    Integer id = offsetToId.get(value);
			    if(id == null) continue;//  bug fix
				currSet.add(id);
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
				String groundTruthQuery = "SELECT id_d, id_s FROM "  + "ground_truth.ground_truth_" + tableName +
						" WHERE id_s IN " + inIdd + " OR id_d IN " + inIdd ;
				ResultSet gtQueryResults = runQuery(groundTruthQuery);
				while (gtQueryResults.next()) {
					Integer id_d = Integer.parseInt(gtQueryResults.getString("id_d"));
					Integer id_s = Integer.parseInt(gtQueryResults.getString("id_s"));
					Integer offset_d = idsToOffset.get(id_d);
					Integer offset_s = idsToOffset.get(id_s);
					if(offset_d == null || offset_s == null) continue; //  bug fix
					IdDuplicates idd = new IdDuplicates(offset_d, offset_s);
					groundDups.add(idd);
					
					String uniqueComp = "";
					if (offset_d > offset_s)
						uniqueComp = offset_d + "u" + offset_s;
					else
						uniqueComp = offset_s + "u" + offset_d;
					if (groundMatches.contains(uniqueComp))
						continue;
					groundMatches.add(uniqueComp);
				}		
			}
			//SerializationUtilities.storeSerializedObject(groundDups, dumpDirectories.getGroundTruthDirPath() + name);
		}
		

		final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(groundDups);
		System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());
		List<AbstractBlock> blocks = DeduplicationExecution.blocks;
		duplicatePropagation.resetDuplicates();
//		for(AbstractBlock block : blocks) {
//			DecomposedBlock uBlock = (DecomposedBlock) block;
//			for (int entity : uBlock.getEntities1())
//				System.out.println(entity);
//				//System.out.print(String.valueOf(offsetToId.get(entity)) + " ");
//			System.out.println();
//			for (int entity : uBlock.getEntities2())
//				System.out.println(entity);
//				//System.out.print(String.valueOf(offsetToId.get(entity)) + " ");
//			System.out.println();
//		}
		System.out.println(blocks.size());		
		BlockStatistics bStats = new BlockStatistics(blocks, duplicatePropagation, csvWriter);
		bStats.applyProcessing();		
		
		Set<String> matches = ExecuteBlockComparisons.matches;
		double sz_before = matches.size();
		matches.removeAll(groundMatches);
		double sz_after = matches.size();
		System.out.println("ACC\t:\t " + sz_after/sz_before);
		csvWriter.flush();
		
	}
	

	private void setProperties() {
		properties = loadProperties();
		if(!properties.isEmpty()) {
            schemaName = properties.getProperty(SCHEMA_NAME);
            if(calciteConnectionString.equals("")) {
	            calciteConnectionString = properties.getProperty(CALCITE_CONNECTION);
	            if(calciteConnectionString == null) {
	            	URL res = QueryEngine.class.getClassLoader().getResource("model.json");
	            	File file = null;
					try {
						file = Paths.get(res.toURI()).toFile();
					} catch (URISyntaxException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					calciteConnectionString = "jdbc:calcite:model=" + file.getAbsolutePath();
	            }
            }
            calculateGroundTruth = Boolean.parseBoolean(properties.getProperty(CALCULATE_GROUND_TRUTH));
            groundTruthDivide = Integer.parseInt(properties.getProperty(DIVIDE_GROUND_TRUTH));
			dumpPath = properties.getProperty(DUMP_PATH);
		}
	}
	
	private Properties loadProperties() {
		
        Properties prop = new Properties();
       
		try (InputStream input =  QueryEngine.class.getClassLoader().getResourceAsStream(pathToPropertiesFile)){
            // load a properties file
            prop.load(input);
                       
        } catch (IOException ex) {
            ex.printStackTrace();
        }
		return prop;
	}
	
}
