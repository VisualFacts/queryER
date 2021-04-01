package org.imsi.queryEREngine.imsi.calcite.util;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.imsi.queryEREngine.apache.calcite.util.Sources;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvEnumerator;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvFieldType;
import org.imsi.queryEREngine.imsi.er.BlockIndex.QueryBlockIndex;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.DecomposedBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityResolvedTuple;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.BlockFiltering;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.EfficientEdgePruning;
import org.imsi.queryEREngine.imsi.er.Utilities.DumpDirectories;
import org.imsi.queryEREngine.imsi.er.Utilities.ExecuteBlockComparisons;
import org.imsi.queryEREngine.imsi.er.Utilities.MapUtilities;
import org.imsi.queryEREngine.imsi.er.Utilities.RandomAccessReader;
import org.imsi.queryEREngine.imsi.er.Utilities.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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
	private static DumpDirectories dumpDirectories = DumpDirectories.loadDirectories();
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> EntityResolvedTuple deduplicateEnumerator(Enumerable<T> enumerable, String tableName,
    		Integer key, String source, List<CsvFieldType> fieldTypes, AtomicBoolean ab) {
    	CsvEnumerator<Object[]> originalEnumerator = new CsvEnumerator(Sources.of(new File(source)), ab, fieldTypes, key);
        HashMap<Integer, Object[]> queryData = createMap((AbstractEnumerable<Object[]>) enumerable, key);
        return deduplicate(queryData, key, fieldTypes.size(), tableName, originalEnumerator, source);
    	
    }

    public static EntityResolvedTuple deduplicate(HashMap<Integer, Object[]> queryData, Integer key, Integer noOfAttributes,
			String tableName, Enumerator<Object[]> originalEnumerator, String source) {
    	boolean firstDedup = false;
    	double setPropertiesStartTime = System.currentTimeMillis();
    	//setProperties();
    	double setPropertiesTime = (System.currentTimeMillis() - setPropertiesStartTime);
    	System.out.println("Deduplicating: " + tableName);
    	double deduplicateStartTime = System.currentTimeMillis() - setPropertiesTime;
        
        // Check for links and remove qIds that have links
        double linksStartTime = System.currentTimeMillis();
        HashMap<Integer, Set<Integer>> links = loadLinks(tableName);
        if(links == null) firstDedup = true;
        Set<Integer> qIds = new HashSet<>();
        Set<Integer> totalIds = new HashSet<>();

        qIds = queryData.keySet();
        
        // Remove from data qIds with links
        if(!firstDedup) {
        	queryData.keySet().removeAll(links.keySet());
            // Clear links and keep only qIds
    		Set<Integer> linkedIds = getLinkedIds(key, links,  qIds);
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
        if (blocks.size() > 10) {
        	
        	// FILTERING
            double blockFilteringStartTime = System.currentTimeMillis();
            filterParam = 0.35;
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
            }
            
        }

        //Get ids of final entities, and add back qIds that were cut from m-blocking
        Set<Integer> blockQids = new HashSet<>();
        if(runEP)
        	blockQids = queryBlockIndex.blocksToEntitiesD(blocks);
        else
        	blockQids = queryBlockIndex.blocksToEntities(blocks);
        totalIds.addAll(blockQids);
        totalIds.addAll(qIds);
        double storeTime = storeIds(qIds);
        // To find ground truth statistics
        storeTime = storeBlocks(blocks, tableName);
        double tableScanStartTime = System.currentTimeMillis() - storeTime;
        
        RandomAccessReader randomAccessReader = null;
        try {
        	randomAccessReader = RandomAccessReader.open(new File(source));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        double tableScanEndTime = System.currentTimeMillis();
        String tableScanTime = Double.toString((tableScanEndTime - tableScanStartTime) / 1000);

       

        double comparisonStartTime = System.currentTimeMillis() - storeTime;
//        AbstractEnumerable<Object[]> comparisonEnumerable = createEnumerable((Enumerator<Object[]>) originalEnumerator, totalIds, key);
//        queryData = createMap(comparisonEnumerable, key);
        
        ExecuteBlockComparisons ebc = new ExecuteBlockComparisons(queryData, randomAccessReader);
        EntityResolvedTuple entityResolvedTuple = ebc.comparisonExecutionAll(blocks, qIdsNoLinks, key, noOfAttributes);
        double comparisonEndTime = System.currentTimeMillis();
        double links2StartTime = System.currentTimeMillis();
        entityResolvedTuple.mergeLinks(links, tableName, firstDedup, totalIds, runLinks);
        entityResolvedTuple.storeLI();
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
        // Log everything
        if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
        	DEDUPLICATION_EXEC_LOGGER.debug(tableName + "," + queryDataSize + "," + linksTime + "," + blockJoinTime + "," + blockingTime +  "," + blocksSize + "," + 
        			blockSizes + "," + blockEntities + "," + purgingBlocksSize + "," + purgingTime + "," + purgingBlockSizes + "," + 
        			purgeBlockEntities + "," + filterBlocksSize + "," + filterTime + "," + filterBlockSizes + ","  + filterBlockEntities + "," +
        			epTime + "," + epTotalComps + "," + ePEntities + "," + matches + "," + executedComparisons + "," + tableScanTime + "," + jaroTime + "," +
        			comparisonTime + "," + revUfCreationTime + "," + totalEntities + "," + totalDeduplicationTime);
        return entityResolvedTuple;
		
	}

	public static Set<Integer> getLinkedIds(Integer key, Map<Integer, Set<Integer>> links, Set<Integer> qIds) {

    	Set<Integer> totalIds = new HashSet<>();
    	Set<Set<Integer>> sublinks = links.entrySet().stream().filter(entry -> {
    		return qIds.contains(entry.getKey());
    	}).map(entry -> {
    		return entry.getValue();
    	}).collect(Collectors.toSet());
    	for (Set<Integer> sublink : sublinks) {
    		totalIds.addAll(sublink);
    	}   	
    	return totalIds;
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


    private static double storeIds(Set<Integer> qIds) {
    	Set<Integer> newSet = new HashSet<>();
    	newSet.addAll(qIds);
    	double startTime = System.currentTimeMillis();
        SerializationUtilities.storeSerializedObject(newSet, dumpDirectories.getqIdsPath());
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
    public static <T> AbstractEnumerable<T> mergeEntities(EntityResolvedTuple entityResolvedTuple) {
//		if(entityResolvedTuple.uFind != null)
//			entityResolvedTuple.sortEntities();
//		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
//			DEDUPLICATION_EXEC_LOGGER.debug("Final Size: " + entityResolvedTuple.finalData.size());
    	entityResolvedTuple.groupEntities();
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

		try (InputStream input = new FileInputStream(pathToPropertiesFile)) {
            // load a properties file
            prop.load(input);
                       
        } catch (IOException ex) {
            ex.printStackTrace();
        }
		return prop;
	}

}