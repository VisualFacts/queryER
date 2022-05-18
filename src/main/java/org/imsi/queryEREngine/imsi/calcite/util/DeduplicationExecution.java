package org.imsi.queryEREngine.imsi.calcite.util;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.imsi.queryEREngine.apache.calcite.util.Sources;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvEnumerator;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvFieldType;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.EdgePruning;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.WeightingScheme;
import org.imsi.queryEREngine.imsi.er.QueryEngine;
import org.imsi.queryEREngine.imsi.er.BlockIndex.QueryBlockIndex;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.DecomposedBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityResolvedTuple;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.BlockFiltering;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.EfficientEdgePruning;
import org.imsi.queryEREngine.imsi.er.Utilities.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * Single table deduplication execution.
 */
public class DeduplicationExecution<T> {

    protected static final Logger DEDUPLICATION_EXEC_LOGGER = LoggerFactory.getLogger(DeduplicationExecution.class);


    /**
     * Performs deduplication on a single table's entities.
     * The steps for performing the resolution are as follows:
     * 1) Get filtered data from filter.
     * 2) Create QueryBlockIndex
     * 3) BlockJoin
     * 4) Apply MetaBlocking
     * 5) Create an enumberable by index scanning with the IDs as derived from the MetaBlocking
     * 6) Execute Block Comparisons to find matches
     *
     * @param enumerable The data after the filter
     * @param tableName  Name of the table used to get the BlockIndex
     * @param key        Key column id of the table for block indexing
     * @param source     Source of the table data for the scan
     * @param fieldTypes Types of the data
     * @param ab         Used for the csv enumerator, nothing else
     * @return EntityResolvedTuple contains the UnionFind + HashMap of the table to be used in merging/join
     * @throws IOException
     */


	private static final String pathToPropertiesFile = "deduplication.properties";
	private static Properties properties;

	private static final String BP = "mb.bp";
	private static final String BF = "mb.bf";
	private static final String EP = "mb.ep";
	private static final String LINKS = "links";
	private static final String FILTER_PARAM = "filter.param";

	private static boolean runBP = true;
	private static boolean runBF = true;
	private static boolean runEP = true;
	private static boolean runLinks = true;
	private static double filterParam = 0.0;
    private static double scanTime = 0.0;
	private static DumpDirectories dumpDirectories = new DumpDirectories();
	public static List<AbstractBlock> blocks;
	public static Set<Integer> qIds = new HashSet<>();
    @SuppressWarnings({"rawtypes", "unchecked"})

    public static <T> EntityResolvedTuple deduplicateEnumerator(Enumerable<T> enumerable, String tableName,
    		Integer key, String source, List<CsvFieldType> fieldTypes, AtomicBoolean ab) {
    	CsvEnumerator<Object[]> originalEnumerator = new CsvEnumerator(Sources.of(new File(source)), ab, fieldTypes, key);
        double scanStart = System.currentTimeMillis();
        HashMap<Integer, Object[]> queryData = createMap((AbstractEnumerable<Object[]>) enumerable, key);
        scanTime = (System.currentTimeMillis() - scanStart) / 1000;
        return deduplicate(queryData, key, fieldTypes.size(), tableName, originalEnumerator, source);
    	
    }

    public static EntityResolvedTuple deduplicate(HashMap<Integer, Object[]> queryData, Integer key, Integer noOfAttributes,
			String tableName, Enumerator<Object[]> originalEnumerator, String source) {

    	double setPropertiesStartTime = System.currentTimeMillis();
    	setProperties();
    	double setPropertiesTime = (System.currentTimeMillis() - setPropertiesStartTime);
        boolean firstDedup = false;

        System.out.println("Deduplicating: " + tableName);
    	double deduplicateStartTime = System.currentTimeMillis() - setPropertiesTime;
        
        // Check for links and remove qIds that have links
        double linksStartTime = System.currentTimeMillis();
        HashMap<Integer, Set<Integer>> links = loadLinks(tableName);
        HashMap<Integer, Object[]> dataWithLinks = new HashMap<>();
        if(links == null) firstDedup = true;
        Set<Integer> qIds = new HashSet<>();
        Set<Integer> totalIds = new HashSet<>();

        qIds = MapUtilities.deepCopySet(queryData.keySet());
        double idsTime = storeIds(qIds);
        deduplicateStartTime -= idsTime;

        /* If there are links then we get all ids that are in the links HashMap (both on keys and the values).
         * Then we get all these data and put it onto the dataWithLinks hashMap.
         * Now we have two hashmaps 1) dataWithLinks, queryData = data without links.
         * After we deduplicate queryData, we will merge these two tables.
         */
        if(!firstDedup) {
            // Clear links and keep only qIds
        	Set<Integer> linkedIds = getLinkedIds(key, links,  qIds); // Get extra Link Ids that are not in queryData
        	dataWithLinks = (HashMap<Integer, Object[]>) links.keySet().stream()
        		    .filter(queryData::containsKey)
        		    .collect(Collectors.toMap(Function.identity(), queryData::get));
        	dataWithLinks = getExtraData(dataWithLinks, linkedIds, originalEnumerator, key);
        	queryData.keySet().removeAll(links.keySet()); 
    		totalIds.addAll(linkedIds);  // Add links back
    		
        }
        final Set<Integer> qIdsNoLinks = MapUtilities.deepCopySet(queryData.keySet());

        double linksEndTime = System.currentTimeMillis();
        double links1Time = (linksEndTime - linksStartTime) / 1000;

        String queryDataSize = Integer.toString(queryData.size());

        double blockingStartTime = System.currentTimeMillis();
        QueryBlockIndex queryBlockIndex = new QueryBlockIndex();
        queryBlockIndex.createBlockIndex(queryData, key);
        queryBlockIndex.buildQueryBlocks();
        double blockingEndTime = System.currentTimeMillis();
        String blockingTime = Double.toString((blockingEndTime - blockingStartTime) / 1000);
        boolean doER = queryData.size() > 0 ? true : false;

        double blockJoinStart = System.currentTimeMillis();
        List<AbstractBlock> blocks = queryBlockIndex
                .joinBlockIndices(tableName, doER);
        double blockJoinEnd = System.currentTimeMillis();
        String blockJoinTime = Double.toString((blockJoinEnd - blockJoinStart) / 1000);


        String blocksSize = Integer.toString(blocks.size());
        String blockSizes = getBlockSizes(blocks);
        String blockEntities = Integer.toString(queryBlockIndex.blocksToEntities(blocks).size());

        // PURGING
        double blockPurgingStartTime = System.currentTimeMillis();
        
        ComparisonsBasedBlockPurging blockPurging = new ComparisonsBasedBlockPurging();
        if(runBP) blockPurging.applyProcessing(blocks);
        
        double blockPurgingEndTime = System.currentTimeMillis();

        String purgingBlocksSize = Integer.toString(blocks.size());
        String purgingTime = Double.toString((blockPurgingEndTime - blockPurgingStartTime) / 1000);
        String purgingBlockSizes = getBlockSizes(blocks);
        int entities = queryBlockIndex.blocksToEntities(blocks).size();
        String purgeBlockEntities = Integer.toString(entities);
        double comps = 0.0;

		for(AbstractBlock block : blocks) {
			comps += block.getNoOfComparisons();
		}
        String filterBlocksSize = "";
        String filterTime = "";
        String filterBlockSizes = "";
        String epTime = "";
        String epTotalComps = "";
        String filterBlockEntities = "";
        String ePEntities = "";
        boolean epFlag = false;
        if (blocks.size() > 10) {
        	
        	// FILTERING
            double blockFilteringStartTime = System.currentTimeMillis();
            filterParam = 0.45;
            if(tableName.contains("publications")) filterParam = 0.55;
	        BlockFiltering bFiltering = new BlockFiltering(filterParam);
	        if(runBF) bFiltering.applyProcessing(blocks);

            double blockFilteringEndTime = System.currentTimeMillis();
            filterBlocksSize = Integer.toString(blocks.size());
            filterTime = Double.toString((blockFilteringEndTime - blockFilteringStartTime) / 1000);
            filterBlockSizes = getBlockSizes(blocks);
            filterBlockEntities = Integer.toString(queryBlockIndex.blocksToEntities(blocks).size());
            // EDGE PRUNING
            double edgePruningStartTime = System.currentTimeMillis();
            //EdgePruning eEP = new EdgePruning(MetaBlockingConfiguration.WeightingScheme.EJS);
            EfficientEdgePruning eEP = new EfficientEdgePruning();
            if(runEP) {
            	eEP.applyProcessing(blocks);
	            double edgePruningEndTime = System.currentTimeMillis();
	
	            epTime = Double.toString((edgePruningEndTime - edgePruningStartTime) / 1000);
	            double totalComps = 0;
	            for (AbstractBlock block : blocks) {
	            	totalComps += block.getNoOfComparisons();
	            }
	            epTotalComps = Double.toString(totalComps);
	            ePEntities = Integer.toString(queryBlockIndex.blocksToEntitiesD(blocks).size());
	            epFlag = true;
	            
            }
            
        }
        //Get ids of final entities, and add back qIds that were cut from m-blocking
        Set<Integer> blockQids = new HashSet<>();
        if(epFlag)
        	blockQids = queryBlockIndex.blocksToEntitiesD(blocks);
        else
        	blockQids = queryBlockIndex.blocksToEntities(blocks);
        totalIds.addAll(blockQids);
        totalIds.addAll(qIds);
        DeduplicationExecution.qIds = qIds;
        // To find ground truth statistics
        DeduplicationExecution.blocks = blocks;

        RandomAccessReader randomAccessReader = null;
        try {
        	randomAccessReader = RandomAccessReader.open(new File(source));
		} catch (IOException e) {
			e.printStackTrace();
		}

        double comparisonStartTime = System.currentTimeMillis();
        
        // Merge queryData with dataWithLinks

        queryData = mergeMaps(queryData, dataWithLinks);
        ExecuteBlockComparisons<?> ebc = new ExecuteBlockComparisons(queryData, randomAccessReader);
        EntityResolvedTuple<?> entityResolvedTuple = ebc.comparisonExecutionAll(blocks, qIdsNoLinks, key, noOfAttributes);
        double comparisonEndTime = System.currentTimeMillis();
        double links2StartTime = System.currentTimeMillis();
        entityResolvedTuple.mergeLinks(links, tableName, firstDedup, totalIds, runLinks);
        double links2EndTime = System.currentTimeMillis();

        Integer executedComparisons = entityResolvedTuple.getComparisons();
        int matches = entityResolvedTuple.getMatches();
        int totalEntities = entityResolvedTuple.data.size();
        double jaroTime = entityResolvedTuple.getCompTime();
        double deduplicateEndTime = System.currentTimeMillis();
        double revUfCreationTime = entityResolvedTuple.getRevUFCreationTime();
        String comparisonTime = Double.toString((comparisonEndTime - comparisonStartTime) / 1000);
        String totalDeduplicationTime = Double.toString((deduplicateEndTime - deduplicateStartTime) / 1000);
        String linksTime = Double.toString(links1Time + ((links2EndTime - links2StartTime) / 1000));

//        System.out.println("Links Time\t\t" + linksTime);
//        System.out.println("Blocking Time\t\t" + String.valueOf((blockJoinEnd - blockingStartTime)/1000));
//        System.out.println("Block Purging Time\t\t" + purgingTime);
//        System.out.println("Block Filtering Time\t\t" + filterTime);
//        System.out.println("Edge Pruning Time\t\t" + epTime);
//        System.out.println("Comparison Execution Time\t\t" + comparisonTime);
//        System.out.println("Total Deduplication Time\t\t" + totalDeduplicationTime);
        // Log everything
        try {
            FileWriter logWriter = new FileWriter(dumpDirectories.getLogsDirPath(), true);
            logWriter.write(tableName + "," + queryDataSize + "," + scanTime + "," + linksTime + "," + blockJoinTime + "," + blockingTime +  "," + blocksSize + "," +
                    blockSizes + "," + blockEntities + "," + purgingBlocksSize + "," + purgingTime + "," + purgingBlockSizes + "," +
                    purgeBlockEntities + "," + filterBlocksSize + "," + filterTime + "," + filterBlockSizes + ","  + filterBlockEntities + "," +
                    epTime + "," + epTotalComps + "," + ePEntities + "," + matches + "," + executedComparisons + "," + jaroTime + "," +
                    comparisonTime + "," + revUfCreationTime + "," + totalEntities + "," + totalDeduplicationTime + "\n");
            logWriter.close();
        } catch (IOException e) {
            System.out.println("Log file creation error occurred.");
            e.printStackTrace();
        }

        
        return entityResolvedTuple;
		
	}

	private static HashMap<Integer, Object[]> getExtraData(HashMap<Integer, Object[]> dataWithLinks, Set<Integer> linkedIds, Enumerator<Object[]> originalEnumerator, int tableKey) {
		AbstractEnumerable<Object[]> comparisonEnumerable = createEnumerable((Enumerator<Object[]>) originalEnumerator, linkedIds, tableKey);
        originalEnumerator.close();
		return mergeMaps(dataWithLinks, createMap(comparisonEnumerable, tableKey));
	}

	public static Set<Integer> getLinkedIds(Integer key, Map<Integer, Set<Integer>> links, Set<Integer> qIds) {

    	Set<Integer> linkedIds = new HashSet<>();
    	Set<Set<Integer>> sublinks = links.entrySet().stream().filter(entry -> {
    		return qIds.contains(entry.getKey());
    	}).map(entry -> {
    		return entry.getValue();
    	}).collect(Collectors.toSet());
    	for (Set<Integer> sublink : sublinks) {
    		linkedIds.addAll(sublink);
    	}   	
    	linkedIds.removeAll(qIds);

    	return linkedIds;
    }

	private static String getBlockSizes(List<AbstractBlock> blocks) {
    	double maxBlockSize = 0.0;
    	double totalBlockSize = 0.0;
    	double totalComps = 0.0;
    	double avgBlockSize;

		for(AbstractBlock block : blocks) {
			double blockSize = block.getTotalBlockAssignments();
			if(blockSize > maxBlockSize) maxBlockSize = blockSize;
			totalBlockSize += blockSize;
            totalComps += block.getNoOfComparisons();

		}
		avgBlockSize = totalBlockSize/blocks.size();
		return String.valueOf(maxBlockSize) + "," + avgBlockSize + "," + totalComps;
		
	}

	private static HashMap<Integer, Object[]> mergeMaps(HashMap<Integer, Object[]> map1, HashMap<Integer, Object[]> map2){
		map1.forEach(
        	    (key, value) -> map2.put(key, value)
        	);
        return map2;
	}

    private static double storeIds(Set<Integer> qIds) {
    	double startTime = System.currentTimeMillis();
        SerializationUtilities.storeSerializedObject(qIds, dumpDirectories.getqIdsPath());
        return System.currentTimeMillis() - startTime;
    }

    private static double storeBlocks(List<AbstractBlock> blocks, String tableName) {
    	double startTime = System.currentTimeMillis();
        List<DecomposedBlock> dBlocks = (List<DecomposedBlock>) (List<? extends AbstractBlock>) blocks;
        SerializationUtilities.storeSerializedObject(dBlocks, dumpDirectories.getBlockDirPath());
        return System.currentTimeMillis() - startTime;
    }

    /**
     * @param <T>
     * @param entityResolvedTuple The tuple as created by the deduplication/join
     * @return AbstractEnumerable that combines the hashmap and the UnionFind to create the merged/fusioned data
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> AbstractEnumerable<T> mergeEntities(EntityResolvedTuple entityResolvedTuple, List<Integer> projects, List<String> fieldNames) {
//		if(entityResolvedTuple.uFind != null)
//			entityResolvedTuple.sortEntities();
//		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
//			DEDUPLICATION_EXEC_LOGGER.debug("Final Size: " + entityResolvedTuple.finalData.size());
    	entityResolvedTuple.groupEntities(projects, fieldNames);
        return entityResolvedTuple;

    }
    

    /**
     * @param enumerable Data of the table
     * @param key        Key column of the table
     * @return HashMap from key to entity
     */
    private static HashMap<Integer, Object[]> createMap(AbstractEnumerable<Object[]> enumerable, Integer key) {
        List<Object[]> entityList = enumerable.toList();
        HashMap<Integer, Object[]> entityMap = new HashMap<Integer, Object[]>();
       
        for (Object[] entity : entityList) {
            entityMap.put(Integer.parseInt(entity[key].toString()), entity);
        }
        return entityMap;
    }

    public static HashMap<Integer, Set<Integer>> loadLinks(String table) {
    	if(new File(dumpDirectories.getLinksDirPath() + table).exists() && runLinks)
    		return (HashMap<Integer, Set<Integer>>) SerializationUtilities.loadSerializedObject(dumpDirectories.getLinksDirPath() + table);
    	else  return null;
    }
    
    /**
     * @param enumerator Enumerator data
     * @param qIds       Qids to pick from enumerator
     * @param key        Key column
     * @return AbstractEnumerable filtered by ids
     */
    private static AbstractEnumerable<Object[]> createEnumerable(Enumerator<Object[]> enumerator, Set<Integer> qIds, Integer key) {
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
                            if (entityKey.equals("")) continue;
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
    
    
    
    private static void setProperties() {
		properties = loadProperties();
		if(!properties.isEmpty()) {
			runBP = Boolean.parseBoolean(properties.getProperty(BP));
            runBF = Boolean.parseBoolean(properties.getProperty(BF));
            runEP = Boolean.parseBoolean(properties.getProperty(EP));
            runLinks = Boolean.parseBoolean(properties.getProperty(LINKS));
		}
	}
	
	private static Properties loadProperties() {
		
        Properties prop = new Properties();
		try (InputStream input =  DeduplicationExecution.class.getClassLoader().getResourceAsStream(pathToPropertiesFile)) {
            // load a properties file
            prop.load(input);
                       
        } catch (IOException ex) {
            ex.printStackTrace();
        }
		return prop;
	}

}