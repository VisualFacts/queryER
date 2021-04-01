package org.imsi.queryEREngine.imsi.er.Utilities;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.imsi.queryEREngine.imsi.calcite.util.DeduplicationExecution;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityResolvedTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class ExecuteBlockComparisons<T> {

	private HashMap<Integer, Object[]> newData = new HashMap<>();
	HashMap<Integer, Long> veti = new HashMap<>();
	private RandomAccessReader randomAccessReader;
	protected static final Logger DEDUPLICATION_EXEC_LOGGER =  LoggerFactory.getLogger(DeduplicationExecution.class);
	CsvParser parser =  null;
	private Integer noOfFields;
	public ExecuteBlockComparisons(HashMap<Integer, Object[]> newData) {
		this.newData = newData;
	}

	public ExecuteBlockComparisons(RandomAccessReader randomAccessReader) {
		this.randomAccessReader = randomAccessReader;
	}

	public ExecuteBlockComparisons(HashMap<Integer, Object[]> queryData, RandomAccessReader randomAccessReader) {
		this.randomAccessReader = randomAccessReader;
		this.newData = queryData;
		CsvParserSettings parserSettings = new CsvParserSettings();
		parserSettings.setNullValue("");
		parserSettings.setEmptyValue("");
		parserSettings.setDelimiterDetectionEnabled(true);
		File file = new File(randomAccessReader.getPath());
		//parserSettings.selectIndexes(key);
		this.parser = new CsvParser(parserSettings);
		this.parser.beginParsing(file);
		char delimeter = this.parser.getDetectedFormat().getDelimiter();
		parserSettings.getFormat().setDelimiter(delimeter);
		this.parser = new CsvParser(parserSettings);
	}

	public EntityResolvedTuple comparisonExecutionAll(List<AbstractBlock> blocks, Set<Integer> qIds,
			Integer keyIndex, Integer noOfFields) {
		return comparisonExecutionJdk(blocks, qIds, keyIndex, noOfFields);
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	public EntityResolvedTuple comparisonExecutionJdk(List<AbstractBlock> blocks, Set<Integer> qIds,
			Integer keyIndex, Integer noOfFields) {
		int comparisons = 0;
		UnionFind uFind = new UnionFind(qIds);
		
		Set<String> matches = new HashSet<>();
		Set<AbstractBlock> nBlocks = new HashSet<>(blocks);
		Set<String> uComparisons = new HashSet<>();
		this.noOfFields = noOfFields;
		double compTime = 0.0;
		for (AbstractBlock block : nBlocks) {
			ComparisonIterator iterator = block.getComparisonIterator();
			while (iterator.hasNext()) {
				Comparison comparison = iterator.next();
				int id1 = comparison.getEntityId1();
				int id2 = comparison.getEntityId2();
				if (!qIds.contains(id1) && !qIds.contains(id2))
					continue;
				
				String uniqueComp = "";
				if (comparison.getEntityId1() > comparison.getEntityId2())
					uniqueComp = id1 + "u" + id2;
				else
					uniqueComp = id2 + "u" + id1;
				if (uComparisons.contains(uniqueComp))
					continue;
				uComparisons.add(uniqueComp);

				Object[] entity1 = getEntity(id1);
				Object[] entity2 = getEntity(id2);				

				double compStartTime = System.currentTimeMillis();
				double similarity = ProfileComparison.getJaroSimilarity(entity1, entity2, keyIndex);
				double compEndTime = System.currentTimeMillis();
				compTime += compEndTime - compStartTime;
				comparisons++;
				if (similarity >= 0.92) {
					matches.add(uniqueComp);
					uFind.union(id1, id2); 	
				}
			}
		}	
		EntityResolvedTuple eRT = new EntityResolvedTuple(newData, uFind, keyIndex, noOfFields);	
		eRT.setComparisons(comparisons);
		eRT.setMatches(matches.size());
		eRT.setCompTime(compTime/1000);
		eRT.getAll();
		return eRT;
	}
	
	
	private Object[] getEntity(int id) {
		try {
			if(newData.containsKey(id)) return newData.get(id);
			randomAccessReader.seek(id);
			String line = randomAccessReader.readLine();
			if (line != null) {
			    Object[] entity = parser.parseLine(line);
			    newData.put(id, entity);
			    return entity;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Object[] emptyVal = new Object[noOfFields];
		for(int i = 0; i < noOfFields; i++) emptyVal[i] = "";
		return emptyVal;
	}
	
}