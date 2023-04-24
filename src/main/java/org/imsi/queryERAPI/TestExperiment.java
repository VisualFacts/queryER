package org.imsi.queryERAPI;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.imsi.queryEREngine.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataTypeField;
import org.imsi.queryEREngine.apache.calcite.util.Source;
import org.imsi.queryEREngine.apache.calcite.util.Sources;
import org.imsi.queryEREngine.imsi.calcite.adapter.enumerable.csv.CsvEnumerator;
import org.imsi.queryEREngine.imsi.calcite.adapter.enumerable.csv.CsvTranslatableTable;
import org.imsi.queryEREngine.imsi.er.BlockIndex.BaseBlockIndex;
import org.imsi.queryEREngine.imsi.er.BlockIndex.BlockIndexStatistic;
import org.imsi.queryEREngine.imsi.er.Utilities.DumpDirectories;

import com.fasterxml.jackson.databind.ObjectMapper;



public class TestExperiment {

	private static DumpDirectories dumpDirectories  = null;
	private static String dumpPath = "/usr/local/share/data";

	private static BaseBlockIndex createBlockIndex(CsvTranslatableTable table, String tableName) {
		// Create Block index and store into data folder (only if not already created)

		BaseBlockIndex blockIndex = new BaseBlockIndex();
		if((!new File(dumpDirectories.getBlockIndexDirPath() + tableName + "InvertedIndex").exists())) {
			System.out.println("Creating Block Index..");
			AtomicBoolean ab = new AtomicBoolean();
			ab.set(false);
			@SuppressWarnings({ "unchecked", "rawtypes" })
			CsvEnumerator<Object[]> enumerator = new CsvEnumerator(table.getSource(), ab,
					table.getFieldTypes(), table.getKey());

			blockIndex.createBlockIndex(enumerator, table.getKey());
			System.out.println("created");
			blockIndex.buildBlocks();
			blockIndex.sortIndex();
			blockIndex.storeBlockIndex(dumpDirectories.getBlockIndexDirPath(), tableName );
			BlockIndexStatistic blockIndexStatistic = new BlockIndexStatistic(blockIndex.getInvertedIndex(), 
					blockIndex.getEntitiesToBlocks(), tableName);
			blockIndex.setBlockIndexStatistic(blockIndexStatistic);
			try {
				blockIndexStatistic.storeStatistics();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			System.out.println("Block Index created!");
		}
		else {
			System.out.println("Block Index already created!");
			blockIndex.loadBlockIndex(dumpDirectories.getBlockIndexDirPath(), tableName);
			ObjectMapper objectMapper = new ObjectMapper();  
			try {
				blockIndex.setBlockIndexStatistic(objectMapper.readValue(new File(dumpDirectories.getBlockIndexStatsDirPath() + tableName + ".json"),
						BlockIndexStatistic.class));
			} catch (IOException e) {
				BlockIndexStatistic blockIndexStatistic = new BlockIndexStatistic(blockIndex.getInvertedIndex(), 
						blockIndex.getEntitiesToBlocks(), tableName);
				blockIndex.setBlockIndexStatistic(blockIndexStatistic);
				try {
					blockIndexStatistic.storeStatistics();
				} catch (IOException e1) {
					e1.printStackTrace();
				}

			}

		}
		return blockIndex;

	}
	
	/** Creates table */
	private static CsvTranslatableTable createTable(Source source, String name) {
		return new CsvTranslatableTable(source, name, null);

	}
	
	public static void main(String[] args) throws IOException {
		dumpDirectories = new DumpDirectories(dumpPath);
		dumpDirectories.generateDumpDirectories();
		String path = "all/people200k.csv";
		File file = new File(path);
		File directoryFile = new File("all");
		final Source baseSource = Sources.of(directoryFile);
		Source source = Sources.of(file);
		Source sourceSansGz = source.trim(".gz");

		final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");

		if (sourceSansCsv != null) {
			
			final CsvTranslatableTable table = createTable(source, sourceSansCsv.relative(baseSource).path());
			
			String tableName = table.getName();
			System.out.println(tableName + ": " + table.getRowType(new JavaTypeFactoryImpl()));
			List<RelDataTypeField> fields = (table.getRowType(new JavaTypeFactoryImpl()).getFieldList());
			List<String> fieldNames = new ArrayList<String>();
			List<String> fieldTypes = new ArrayList<String>();
			// Instantiate keyFieldName here
			for(RelDataTypeField field : fields) {
				fieldNames.add(field.getName());
				fieldTypes.add(field.getType().toString());

			}
			String[] keys = {"rec_id", "id"};
			for(String key : keys) {
				if(fieldNames.contains(key)) {
					table.setKey(fieldNames.indexOf(key));
					break;
				}
			}				
			// createVETI(source, table.getKey(), tableName);
			BaseBlockIndex blockIndex = createBlockIndex(table, tableName);
		}
	}
}
