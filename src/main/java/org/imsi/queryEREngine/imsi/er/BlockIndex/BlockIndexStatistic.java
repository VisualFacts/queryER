package org.imsi.queryEREngine.imsi.er.BlockIndex;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityIndex;
import org.imsi.queryEREngine.imsi.er.DataStructures.UnilateralBlock;
import org.imsi.queryEREngine.imsi.er.Utilities.DumpDirectories;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BlockIndexStatistic implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2830872461507252831L;
	private String tableName;
	private Map<String, Set<Integer>> invertedIndex;
	private Map<Integer, Set<String>> entitiesToBlocks;
	private Map<String, Integer> blocksHistogram;
	private int tableSize;
	private Map<Integer, String> tokenToIndexMap;
	private List<AbstractBlock> blocks;
	private EntityIndex entityIndex;
	
	protected double validComparisons;
	protected double averageWeight = 2.0;
	protected double totalComparisons;
	protected double meanEntitiesPerBlock;
	protected double meanComparisonsPerBlock;
	protected HashMap<Integer, Double> entitiesToComparisons;
	protected HashMap<String, Double> averageBlockWeight;
	protected double SAMPLING_PERCENT = 1;

	public BlockIndexStatistic() {
		super();
	}
	public BlockIndexStatistic(HashMap<String, Double> averageBlockWeight, double averageWeight, 
			double validComparisons, double totalComparisons, double meanEntitiesPerBlock,
							   double meanComparisonsPerBlock, int tableSize) {
		this.tableSize = tableSize;
		this.averageBlockWeight = averageBlockWeight;
		this.averageWeight = averageWeight;
		this.validComparisons = validComparisons;
		this.totalComparisons = totalComparisons;
		this.meanEntitiesPerBlock = meanEntitiesPerBlock;
		this.meanComparisonsPerBlock = meanComparisonsPerBlock;
	}

	public BlockIndexStatistic(Map<String, Set<Integer>> invertedIndex,
							   Map<Integer, Set<String>> entitiesToBlocks, String tableName) {
		this.invertedIndex = invertedIndex;
		this.entitiesToBlocks = entitiesToBlocks;
		this.tokenToIndexMap = new HashMap<>(invertedIndex.size());
		this.entitiesToComparisons = new HashMap<>();
		this.blocksHistogram = 
				invertedIndex.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, e -> Integer.valueOf(e.getValue().size())));
		this.tableName = tableName;
		this.averageBlockWeight = new HashMap<>();
		metaBlocking();
		calculateValidComparisons();
		//getCBS();
	}
	
	protected void metaBlocking() {
		List<Long> blocksSize = new ArrayList<>();
		Set<Long> uniqueComparisons = new TreeSet<>();
		for(String token : invertedIndex.keySet()) {
			long size = invertedIndex.get(token).size();
			long comps = (size*(size-1))/2;
			blocksSize.add(size);
			uniqueComparisons.add(comps);
		}
		purgeBlocks(getMaxComparisonsPerBlock(blocksSize, uniqueComparisons));
		clearEntitiesToBlocks();
		filterBlocks(0.5);
		this.blocks = parseIndex(invertedIndex);
	}
	
	private void purgeBlocks(double p) {
		Iterator<String> blockIterator = invertedIndex.keySet().iterator();
		Set<String> removedTokens = new HashSet<>();
		while(blockIterator.hasNext()) {
			String token = blockIterator.next();
			Set<Integer> block = invertedIndex.get(token);
			long size = block.size();
			long comps = (size * (size - 1))/2;
			if(comps < p) break;
			removedTokens.add(token);
		}
		invertedIndex.keySet().removeAll(removedTokens);
	}
	
	private void clearEntitiesToBlocks() {
		Set<String> tokens = invertedIndex.keySet();
		for(Set<String> blocks : entitiesToBlocks.values()) {
			blocks.retainAll(tokens);
		}
	}
	
	private void filterBlocks(double ratio) {
		Iterator<Integer> blockIterator = entitiesToBlocks.keySet().iterator();
		invertedIndex.clear();
		invertedIndex = new LinkedHashMap<String, Set<Integer>>();
		while(blockIterator.hasNext()) {
			Integer entity = blockIterator.next();
			Set<String> blocks = entitiesToBlocks.get(entity);
			int size = (int) Math.round(ratio * blocks.size());
			int i = 0;
			Iterator<String> blocksIter = blocks.iterator();
			while(i < size) {
				if(!blocksIter.hasNext()) break;
				Set<Integer> termEntities = invertedIndex.computeIfAbsent(blocksIter.next(),
						x -> new HashSet<Integer>());
				termEntities.add(entity);
				i++;
			}
		}
	}
	

	public List<AbstractBlock> parseIndex(Map<String, Set<Integer>> invertedIndex) {
		List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
		int blockIndex = 0;
		for (Entry<String, Set<Integer>> term : invertedIndex.entrySet()) {
			Set<Integer> ids = term.getValue();
			int size = ids.size();
			if (1 < size) {
				List<Integer> sample = ids.stream().collect(Collectors.toList());
				Collections.shuffle(sample);
				int new_size = (int) Math.round(size * SAMPLING_PERCENT);
				int[] idsArray = new int[new_size];
				for(int i = 0; i < new_size; i++) idsArray[i] = sample.get(i);
//				int[] idsArray = Converter.convertListToArray(term.getValue());
				UnilateralBlock uBlock = new UnilateralBlock(idsArray);
				this.tokenToIndexMap.put(blockIndex, term.getKey());
				uBlock.setBlockIndex(blockIndex);
				blockIndex++;
				blocks.add(uBlock);
			}
		}
		return blocks;
	}

	protected void getCBS() {
		if (entityIndex == null) {
			entityIndex = new EntityIndex(blocks, true);
		}
		for (AbstractBlock block : blocks) {
			double comps_below_average = 0;
			Iterator<Comparison> iterator = block.getComparisonIterator();		
			double comps = block.getNoOfComparisons();
			if(comps > 0) {
				while(iterator.hasNext()) {
					Comparison comparison = iterator.next();
					double compWeight = entityIndex.getNoOfCommonBlocks(block.getBlockIndex(), comparison);
					if(compWeight < averageWeight) comps_below_average++;
				}	
				double ratio = comps_below_average / comps;
				averageBlockWeight.put(tokenToIndexMap.get(block.getBlockIndex()), ratio);
			}
		}	
	}

	protected void calculateValidComparisons() {
		
		validComparisons = 0;
		totalComparisons = 0;
		double totalEntities = 0;
		for (AbstractBlock block : blocks) {
			Iterator<Comparison> iterator = block.getComparisonIterator();	
			totalComparisons += block.getNoOfComparisons();
			totalEntities += block.getTotalBlockAssignments();
//			while(iterator.hasNext()) {
//				Comparison comparison = iterator.next();
//				if (!entityIndex.isRepeated(block.getBlockIndex(), comparison)) {
//					validComparisons++;
//				}
//			}
			
		}
		meanComparisonsPerBlock = totalComparisons / blocks.size();
		meanEntitiesPerBlock = totalEntities / blocks.size();
	}
	
	
	public void storeStatistics() throws IOException {
		DumpDirectories dumpDirectories = new DumpDirectories();
		String blockIndexStatsDir = dumpDirectories.getBlockIndexStatsDirPath();
		File file = new File(blockIndexStatsDir + tableName + ".json");
		FileOutputStream fOut = null;

		fOut = new FileOutputStream(file);

		ObjectMapper mapper = new ObjectMapper();

		JsonGenerator jGenerator = null;
		jGenerator = mapper.getFactory().createGenerator(fOut);
		mapper.writeValue(jGenerator, this);
		jGenerator.close();
	}
	
	private static long getMaxComparisonsPerBlock(List<Long> blocksSize, Set<Long> distinctComparisonsLevel) {
		Collections.sort(blocksSize);
		
		int index = -1;
		long[] blockAssignments = new long[distinctComparisonsLevel.size()];
		long[] comparisonsLevel = new long[distinctComparisonsLevel.size()];
		long[] totalComparisonsPerLevel = new long[distinctComparisonsLevel.size()];
        for (Long size : blocksSize) {
        	long noOfComps = (size*(size-1))/2;;
            if (index == -1) {
                index++;
                comparisonsLevel[index] = noOfComps;
                blockAssignments[index] = 0;
                totalComparisonsPerLevel[index] = 0;
            } else if (noOfComps != comparisonsLevel[index]) {
                index++;
                comparisonsLevel[index] = noOfComps;
                blockAssignments[index] = blockAssignments[index-1];
                totalComparisonsPerLevel[index] = totalComparisonsPerLevel[index-1];
            }

            blockAssignments[index] += size;
            totalComparisonsPerLevel[index] += noOfComps;
        }
        
        long currentBC = 0;
        long currentCC = 0;
        long currentSize = 0;
        long previousBC = 0;
        long previousCC = 0;
        long previousSize = 0;
        int arraySize = blockAssignments.length;
        for (int i = arraySize-1; 0 <= i; i--) {
            previousSize = currentSize;
            previousBC = currentBC;
            previousCC = currentCC;

            currentSize = comparisonsLevel[i];
            currentBC = blockAssignments[i];
            currentCC = totalComparisonsPerLevel[i];

            if (currentBC * previousCC < 1.025 * currentCC * previousBC) {
                break;
            }
        }
        return previousSize;
    }
	public void setTableSize(int tableSize) {this.tableSize = tableSize; }
	public int getTableSize(int tableSize) {return  tableSize; }
	public double getAverageWeight() {
		return averageWeight;
	}
	public void setAverageWeight(double averageWeight) {
		this.averageWeight = averageWeight;
	}
	public HashMap<String, Double> getAverageBlockWeight() {
		return averageBlockWeight;
	}
	public void setAverageBlockWeight(HashMap<String, Double> averageBlockWeight) {
		this.averageBlockWeight = averageBlockWeight;
	}
	public double getValidComparisons() {
		return validComparisons;
	}
	public void setValidComparisons(double validComparisons) {
		this.validComparisons = validComparisons;
	}
	public double getTotalComparisons() {
		return totalComparisons;
	}
	public void setTotalComparisons(double totalComparisons) {
		this.totalComparisons = totalComparisons;
	}
	public HashMap<Integer, Double> getEntitiesToComparisons() {
		return entitiesToComparisons;
	}
	public void setEntitiesToComparisons(HashMap<Integer, Double> entitiesToComparisons) {
		this.entitiesToComparisons = entitiesToComparisons;
	}
	public double getMeanEntitiesPerBlock() {
		return meanEntitiesPerBlock;
	}
	public void setMeanEntitiesPerBlock(double meanEntitiesPerBlock) {
		this.meanEntitiesPerBlock = meanEntitiesPerBlock;
	}
	public double getMeanComparisonsPerBlock() {
		return meanComparisonsPerBlock;
	}
	public void setMeanComparisonsPerBlock(double meanComparisonsPerBlock) {
		this.meanComparisonsPerBlock = meanComparisonsPerBlock;
	}

	public int getTableSize() {
		return tableSize;
	}
}
