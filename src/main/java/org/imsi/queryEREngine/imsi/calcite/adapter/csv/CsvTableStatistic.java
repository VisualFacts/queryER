package org.imsi.queryEREngine.imsi.calcite.adapter.csv;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.imsi.queryEREngine.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.imsi.queryEREngine.apache.calcite.util.Source;
import org.imsi.queryEREngine.apache.calcite.util.Sources;
import org.imsi.queryEREngine.imsi.er.BlockIndex.QueryBlockIndex;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityResolvedTuple;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.BlockFiltering;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.EfficientEdgePruning;
import org.imsi.queryEREngine.imsi.er.Utilities.ExecuteBlockComparisons;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import net.agkn.hll.HLL;

public class CsvTableStatistic {

	protected CsvEnumerator<Object[]> csvEnumerator;
	protected Map<Integer, Long> columnCardinalities;
	protected Map<String, Integer> foreignKeys;
	protected int columnCount;
	protected List<Object[]> sample;
	protected int tableKey;
	protected String tableName;
	protected Double SAMPLING_PERCENT = 0.1;
	protected File[] tableFiles;
	protected Source baseSource;
	
	public CsvTableStatistic(CsvEnumerator<Object[]> csvEnumerator, String tableName, int columnCount, 
			int tableKey, File[] files, Source baseSource) {
		this.csvEnumerator = csvEnumerator;
		this.columnCount = columnCount;
		this.tableKey = tableKey;
		this.tableName = tableName;
		this.sample = new ArrayList<>();
		this.columnCardinalities = new HashMap<Integer, Long>(columnCount);
		this.tableFiles = files;
		this.baseSource = baseSource;
		
	}
	public void getStatistics() {
		//sampleTable();
		//deduplicate();
		//getJoins();
		getCardinalities();

	}
	
	private <T> void getJoins() {
		 
		for (File file : tableFiles) {
			Source source = Sources.of(file);
			
			if (source != null) {
				CsvTranslatableTable table = new CsvTranslatableTable(source, source.relative(baseSource).path(), null);
				if(!source.toString().contains(tableName)){
					if(tableName.equals(table.getName())) continue;
					table.getRowType(new JavaTypeFactoryImpl());
					CsvEnumerator<T> thisEnumerator = new CsvEnumerator(csvEnumerator.getSource(),
			        		csvEnumerator.getCancelFlag(), csvEnumerator.getFieldTypes(), table.getKey());
					CsvEnumerator<T> otherEnumerator = new CsvEnumerator(source,
							csvEnumerator.getCancelFlag(), table.getFieldTypes(), tableKey);
					getAllDirtyMatches(thisEnumerator, otherEnumerator, tableName, table.getName());
						
				}
			}
		}
		initializeFKs();
		
	}
	
	private void initializeFKs() {
		foreignKeys = new HashMap<>();
		foreignKeys.put("projects", 8);
		foreignKeys.put("people", 9);
		foreignKeys.put("organisations", 1);		
	}
	
	public void sampleTable() {
		while(csvEnumerator.moveNext()) {
			double random_double = Math.random();
			if(random_double < SAMPLING_PERCENT) {
				sample.add(csvEnumerator.current());
			}
		}
	}
	
	public <T> void deduplicate() {

        CsvEnumerator<T> originalEnumerator = new CsvEnumerator(csvEnumerator.getSource(),
        		csvEnumerator.getCancelFlag(), csvEnumerator.getFieldTypes(), tableKey);

		QueryBlockIndex queryBlockIndex = new QueryBlockIndex();
		queryBlockIndex.createBlockIndex(sample, tableKey);
		queryBlockIndex.buildQueryBlocks();
		Set<Integer> qIds = queryBlockIndex.getIds();
		System.out.println(qIds.size());
		ComparisonsBasedBlockPurging blockPurging = new ComparisonsBasedBlockPurging();
        List<AbstractBlock> blocks = queryBlockIndex
                .joinBlockIndices(tableName, true);
	    blockPurging.applyProcessing(blocks);
	    if (blocks.size() > 10) {
			 BlockFiltering bFiltering = new BlockFiltering(0.35);
			 bFiltering.applyProcessing(blocks);
			 EfficientEdgePruning eEP = new EfficientEdgePruning();
	         eEP.applyProcessing(blocks);
	    }
	    Set<Integer> totalIds = queryBlockIndex.blocksToEntitiesD(blocks);
        AbstractEnumerable<Object[]> comparisonEnumerable = createEnumerable((CsvEnumerator<Object[]>) originalEnumerator, totalIds, tableKey);
		 HashMap<Integer, Object[]> entityMap = createMap(comparisonEnumerable, tableKey);
        ExecuteBlockComparisons ebc = new ExecuteBlockComparisons(entityMap);
		EntityResolvedTuple entityResolvedTuple = ebc.comparisonExecutionAll(blocks, qIds, tableKey, columnCount);
		System.out.println(entityResolvedTuple.finalData.size());

	}
	public <T> void getCardinalities() {
		CsvEnumerator<T> csvEnumerator = new CsvEnumerator(this.csvEnumerator.getSource(),
        		this.csvEnumerator.getCancelFlag(), this.csvEnumerator.getFieldTypes(), tableKey);
		HashFunction hashFunction = Hashing.murmur3_128();
		List<HLL> cardinalityList = new ArrayList<>();
		int col = 0;
		while(col < columnCount) {
			cardinalityList.add(new HLL(14, 5));
			col += 1;
		}
		int rows = 0;
		while(csvEnumerator.moveNext() && rows < 100000) {
			rows ++;
			Object[] curr = (Object[]) csvEnumerator.current();
			for(int i = 0 ; i < columnCount; i++) {
				String attribute = curr[i].toString();
				long hashedValue = hashFunction.newHasher().putString(attribute, Charset.forName("UTF-8")).hash().asLong();
				cardinalityList.get(i).addRaw(hashedValue);	
			}
		}
		col = 0;
		for(HLL columnCardinality : cardinalityList) {
			long cardinality = columnCardinality.cardinality();
			columnCardinalities.put(col, cardinality);
		}
		
	}

	public void storeStatistics() {
		// TODO Auto-generated method stub
		
	}
	
	/**
     * @param enumerable Data of the table
     * @param key        Key column of the table
     * @return HashMap from key to entity
     */
    private static HashMap<Integer, Object[]> createMap(AbstractEnumerable<Object[]> enumerable, Integer key) {
        List<Object[]> entityList = enumerable.toList();
        HashMap<Integer, Object[]> entityMap = new HashMap<Integer, Object[]>(entityList.size());
        for (Object[] entity : entityList) {
            entityMap.put(Integer.parseInt(entity[key].toString()), entity);
        }
        return entityMap;
    }
    
	/**
     * @param enumerator Enumerator data
     * @param qIds       Qids to pick from enumerator
     * @param key        Key column
     * @return AbstractEnumerable filtered by ids
     */
    private static AbstractEnumerable<Object[]> createEnumerable(CsvEnumerator<Object[]> enumerator, Set<Integer> qIds, Integer key) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new Enumerator<Object[]>() {
                    @Override
                    public Object[] current() {
                        return enumerator.current();
                    }

                    @Override
                    public boolean moveNext() {
                        while (enumerator.moveNext()) {
                            final Object[] current = enumerator.current();
                            String entityKey = current[key].toString();
                            if (entityKey.contentEquals("")) continue;
                            if (qIds.contains(Integer.parseInt(entityKey))) {
                                return true;
                            }
                        }
                        return false;
                    }

                    @Override
                    public void reset() {
                    }

                    @Override
                    public void close() {
                    }
                };
            }

        };
    }
    
    
    private static HashMap<String, Object[]> createLookup(CsvEnumerator<Object[]> right, String rightName){
    	HashMap<String, Object[]> lookup = new HashMap<>();
    	while(right.moveNext()) {
    		Object[] curr = right.current();
//    		lookup.put(curr[], value)
    		
    	}
    	return lookup;
    }
    
    private static  <T> List<Integer> getAllDirtyMatches(CsvEnumerator<T> left, CsvEnumerator<T> right, String leftName, String rightName) {
    	
//    	createLookup((CsvEnumerator<Object[]>) right);
    	return null;
    }
}
