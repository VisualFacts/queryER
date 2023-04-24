package org.imsi.queryEREngine.imsi.er.Utilities;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import it.unimi.dsi.fastutil.Hash;
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
    private RandomAccessReader randomAccessReader;
    public static Set<String> matches;
    protected static final Logger DEDUPLICATION_EXEC_LOGGER = LoggerFactory.getLogger(DeduplicationExecution.class);
    CsvParser parser = null;
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
                                                      Integer keyIndex, Integer noOfFields, String tableName) {
        return comparisonExecutionJdk(blocks, qIds, keyIndex, noOfFields, tableName);
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    public EntityResolvedTuple comparisonExecutionJdk(List<AbstractBlock> blocks, Set<Integer> qIds,
                                                      Integer keyIndex, Integer noOfFields, String tableName) {
        int comparisons = 0;
        UnionFind uFind = new UnionFind(qIds);

//		Set<AbstractBlock> nBlocks = new HashSet<>(blocks);
//		Set<String> uComparisons = new HashSet<>();
        HashMap<Integer, HashMap<Integer, Double>> similarities = new HashMap<>();
        this.noOfFields = noOfFields;
        double compTime = 0.0;
        matches = new HashSet<>();
        DumpDirectories dumpDirectories = new DumpDirectories();
        HashMap<Integer, Long> offsetIds = (HashMap<Integer, Long>) SerializationUtilities
                .loadSerializedObject(dumpDirectories.getOffsetsDirPath() + tableName);
        for (AbstractBlock block : blocks) {
//            ComparisonIterator iterator = block.getComparisonIterator();
			QueryComparisonIterator iterator = block.getQueryComparisonIterator(qIds);

            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                int id1 = comparison.getEntityId1();
                int id2 = comparison.getEntityId2();
//				if (!qIds.contains(id1) && !qIds.contains(id2))
//					continue;
//				String uniqueComp = "";
//				if (comparison.getEntityId1() > comparison.getEntityId2())
//					uniqueComp = id1 + "u" + id2;
//				else
//					uniqueComp = id2 + "u" + id1;
//				if (uComparisons.contains(uniqueComp))
//					continue;
//				uComparisons.add(uniqueComp);

                Object[] entity1 = getEntity(offsetIds.get(id1), id1);
                Object[] entity2 = getEntity(offsetIds.get(id2), id2);

//                if (uFind.isInSameSet(id1, id2)){
//                    System.err.println("Same Set");
//                    System.err.println(id1+"  "+id2+"  "+uFind.isInSameSet(id1, id2));
//                    System.err.println(uFind.find(id1)+"  "+uFind.find(id2));
//
//                    continue;
//                }
                if (uFind.isInSameSet(id1, id2)){
//                    uFind.union(id1,id2);
                    continue;
                }


//                Comparisons: 23227
//                ufind size: 3397


                double compStartTime = System.currentTimeMillis();
                double similarity = ProfileComparison.getJaccardSimilarity(entity1, entity2, keyIndex);
                double compEndTime = System.currentTimeMillis();
                compTime += compEndTime - compStartTime;
                comparisons++;
                if (similarity >= 0.92) {
                    //matches.add(uniqueComp);
                    uFind.union(id1, id2);
                    //for id1
                    HashMap<Integer, Double> similarityValues = similarities.computeIfAbsent(id1, x -> new HashMap<>());
                    similarityValues.put(id2, similarity);
                    // for id2
                    similarityValues = similarities.computeIfAbsent(id2, x -> new HashMap<>());
                    similarityValues.put(id1, similarity);
                }
            }
        }
        try {
            randomAccessReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        EntityResolvedTuple eRT = new EntityResolvedTuple(newData, uFind, similarities, keyIndex, noOfFields);
        eRT.setComparisons(comparisons);
        eRT.setMatches(matches.size());
        eRT.setCompTime(compTime / 1000);
        eRT.getAll();
//        System.err.println("Comparisons: " + comparisons);
//        System.err.println("ufind size: "+uFind.getParent().size());

        //Print the union ufind.getParent() and for each key print the values in the set
//        uFind.getParent().keySet().forEach(
//                            		(key)->{
//            			System.err.println("key: "+key+" value: "+uFind.getParent().get(key));
//            		}
//        );



        return eRT;
    }


    private Object[] getEntity(long offset, int id) {
        try {
            if (newData.containsKey(id)) return newData.get(id);
            randomAccessReader.seek(offset);
            String line = randomAccessReader.readLine();
            if (line != null) {
                try {
                    Object[] entity = parser.parseLine(line);
                    newData.put(id, entity);
                    return entity;
                } catch (Exception e) {
                    line = line.substring(1);
                    Object[] entity = parser.parseLine(line);
                    newData.put(id, entity);
                    return entity;
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Object[] emptyVal = new Object[noOfFields];
        for (int i = 0; i < noOfFields; i++) emptyVal[i] = "";
        return emptyVal;
    }

}