package org.imsi.queryEREngine.imsi.er.Utilities;

import java.util.ArrayList;
import java.util.Arrays;
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

import org.imsi.queryEREngine.apache.calcite.rex.RexCall;
import org.imsi.queryEREngine.apache.calcite.rex.RexLiteral;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.rex.RexVisitorImpl;
import org.imsi.queryEREngine.imsi.er.BlockIndex.BlockIndexStatistic;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityIndex;
import org.imsi.queryEREngine.imsi.er.DataStructures.UnilateralBlock;



public class TokenStatistics {

	private Map<String, Set<Integer>> bBlocks;
	private Map<Integer, Set<String>> entitiesToBlocks;
	private BlockIndexStatistic blockIndexStatistic;
	private double compsRatio;
	private String tableName;
	private Map<String, Double> averageBlockWeight;
	private Map<Integer, String> tokenToIndexMap;
	private Set<Integer> entitiesWithLinks;
	private double blockRatio;
	private double meanDiff;
	private double meanEntitiesPerBlockRatio;
	private double comparisonsRatio;
	private double meanComparisonsPerBlockRatio;
	private Double comparisons;
	

	public TokenStatistics(Map<String, Set<Integer>> invertedIndex, Map<Integer, Set<String>> entitiesToBlocks,
			BlockIndexStatistic blockIndexStatistic, Set<Integer> entitiesWithLinks, List<RexNode> conjuctions) {
		this.bBlocks = invertedIndex;
		this.entitiesToBlocks = entitiesToBlocks;
		this.blockIndexStatistic = blockIndexStatistic;
		this.entitiesWithLinks = entitiesWithLinks;
		this.comparisons = blockIndexStatistic.getTotalComparisons();
		getTokens(conjuctions);
	}

	
	private class TokenVisitor extends RexVisitorImpl<RexLiteral> {
		private String token;

		protected TokenVisitor() {
			super(true);
		}

		public String getToken() {
			return token;
		}

	
		@Override public RexLiteral visitLiteral(RexLiteral literal) {
			this.token = literal.toString();
			return literal;
		}

	}
	
	
	public boolean isNumeric(String strNum) {
	    if (strNum == null) {
	        return false;
	    }
	    try {
	        double d = Double.parseDouble(strNum);
	    } catch (NumberFormatException nfe) {
	        return false;
	    }
	    return true;
	}
	
	private void purgeBlocks(double p) {
		double purgeStart = System.currentTimeMillis();
		Iterator<String> blockIterator = bBlocks.keySet().iterator();
		Set<String> removedTokens = new HashSet<>();
		while(blockIterator.hasNext()) {
			String token = blockIterator.next();
			Set<Integer> block = bBlocks.get(token);
			long size = block.size();
			long comps = (size * (size - 1))/2;
			if(comps < p) break;
			removedTokens.add(token);
		}
		bBlocks.keySet().removeAll(removedTokens);
		double purgeEnd = System.currentTimeMillis();
		System.out.println("Time of purge: " + (purgeEnd - purgeStart)/1000);
	}
	
	public HashMap<Integer, Integer> createEntityAssignments(Map<Integer,Set<String>> source) {
	    HashMap<Integer, Integer> assignments = new HashMap<Integer, Integer>();
	    for (Entry<Integer, Set<String>> entry : source.entrySet())
	    	assignments.put(entry.getKey(), entry.getValue().size());
	    return assignments;
	}
	
	private void filterBlocks(double ratio) {
		double filterStart = System.currentTimeMillis();
		Iterator<Integer> blockIterator = entitiesToBlocks.keySet().iterator();
		bBlocks = new LinkedHashMap<String, Set<Integer>>();
		while(blockIterator.hasNext()) {
			Integer entity = blockIterator.next();
			Set<String> blocks = entitiesToBlocks.get(entity);
			int size = (int) Math.round(ratio * blocks.size());
			int i = 0;
			Iterator<String> blocksIter = blocks.iterator();
			while(i < size) {
				if(!blocksIter.hasNext()) break;
				Set<Integer> termEntities = bBlocks.computeIfAbsent(blocksIter.next(),
						x -> new HashSet<Integer>());
				termEntities.add(entity);
				i++;
			}
		}
		double filterEnd = System.currentTimeMillis();
		System.out.println("Time of filter: " + (filterEnd - filterStart)/1000);
	}
	
	private double calculateMxN(Set<Integer> entities) {
		double all_comps = 0;
		for(String token : bBlocks.keySet()) {
			Set<Integer> blockEntities = bBlocks.get(token);
			double a = blockEntities.size();
			blockEntities.removeAll(entities);
			double n = blockEntities.size();
			double m = a - n;
			double total = (m * Math.abs(n-m));
			all_comps += total;
		}
		//this.comparisons = all_comps;
		return all_comps;
	}
	
	private void prune() {
		HashMap<String, Double> averageBlockWeightPred = this.blockIndexStatistic.getAverageBlockWeight();
		this.bBlocks.values().removeIf(v -> v.size() < 1);
		compareBlockIndexes();
		getCBS();
		averageBlockWeightPred.keySet().retainAll(bBlocks.keySet());
		averageBlockWeight.keySet().retainAll(bBlocks.keySet());
		double all_comps = 0;
		double all_ents = 0;
		for(String token : averageBlockWeight.keySet()) {
			Set<Integer> blockEntities = bBlocks.get(token);
			double a = blockEntities.size();
			double comps = a * (a - 1) / 2;
			double percent = (1 - averageBlockWeight.get(token));
			//double kept_comps = (comps * (1 - percent));
			//all_comps += kept_comps;
			all_ents += a * percent;
		}
		System.out.println("Comparisons after prune: " + all_comps);
		System.out.println("Entities after prune: " + all_ents);

	}
	
	private void compareBlockIndexes() {
		long comps = 0;
		long sizes = 0;
		Set<Integer> uniqueEntities = new HashSet<>();
		for(String token : bBlocks.keySet()) {
			Set<Integer> blockEntities = bBlocks.get(token);
			uniqueEntities.addAll(blockEntities);
			long size = blockEntities.size();
			sizes += size;
			comps += (size * (size - 1)) / 2;
		}
		int size = bBlocks.size();
		if(size == 0) return;
		meanEntitiesPerBlockRatio = (sizes / bBlocks.size()) / this.blockIndexStatistic.getMeanEntitiesPerBlock();
		meanComparisonsPerBlockRatio = (comps / bBlocks.size()) / this.blockIndexStatistic.getMeanComparisonsPerBlock();
		comparisonsRatio = comps/this.blockIndexStatistic.getTotalComparisons();
		System.out.println("Mean block entities ratio: " + meanEntitiesPerBlockRatio);
		System.out.println("Mean block comparisons ratio: " + meanComparisonsPerBlockRatio);
		System.out.println("Blocks ratio: " + blockRatio);
		System.out.println("Comparisons ratio: " + comparisonsRatio);	
	}
	
	private void compareRealPredictedPrune(HashMap<String, Double> averageBlockWeight) {
		double diff = 0;
		double max = 0;
		double compsReal = 0;
		double compsPred = 0;
		double min = Double.MAX_VALUE;
		for(String token : this.averageBlockWeight.keySet()) {
			double size = bBlocks.get(token).size();
			double real = this.averageBlockWeight.get(token);
			
			double pred = averageBlockWeight.get(token);
			double abs_diff =  Math.abs(real - pred);
			double comps = size*(size-1)/2;
			compsReal += (1-real)*comps;
			compsPred += (1-pred)*comps;
			diff += abs_diff;

			if(abs_diff > max) max = abs_diff;
			if(abs_diff < min) min = abs_diff;
//			System.out.println("Real: " + Math.round(this.averageBlockWeight.get(token) * 100.0) / 100.0);
//			System.out.println("Predicted: " +  Math.round(averageBlockWeight.get(token) * 100.0) / 100.0);
//			try {
//				TimeUnit.SECONDS.sleep(1);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
			
		}
		meanDiff = diff / this.averageBlockWeight.size();
		System.out.println("Mean diff: " + meanDiff);
		System.out.println("Max diff: " + max + " Min diff: " + min);
		System.out.println("Missed comps: " + Math.abs(compsReal-compsPred));

	}

	protected void getCBS() {
		List<AbstractBlock> blocks = parseIndex(this.bBlocks);
		double averageWeight = 2.0;
		EntityIndex entityIndex = new EntityIndex(blocks, true);
		this.averageBlockWeight = new HashMap<>();
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
				this.averageBlockWeight.put(tokenToIndexMap.get(block.getBlockIndex()), ratio);
			}
		}	
	}
	
	private void pruneBlocks(Set<Integer> entities) {
		double pruneStart = System.currentTimeMillis();
		HashMap<String, Double> averageBlockWeight = this.blockIndexStatistic.getAverageBlockWeight();
		averageBlockWeight.keySet().retainAll(this.bBlocks.keySet());
		double all_comps = 0;
		double all_ents = 0;
		for(String token : averageBlockWeight.keySet()) {
			Set<Integer> blockEntities = bBlocks.get(token);
			double percent = (1 - averageBlockWeight.get(token));
			double a = blockEntities.size();
			blockEntities.removeAll(entities);
			double n = blockEntities.size();
			all_ents += n * percent;
//			double total = (m * Math.abs(n-m));
//			all_comps += (total * (1 - averageBlockWeight.get(token))) ;
			//double kept_comps = (comps * (1 - percent));
			//all_comps += kept_comps;
		}
//		System.out.println("Comparisons after prune and mxn: " + all_comps );
		System.out.println("Entities after prune and mxn: " + (all_ents + entities.size()));

		double pruneEnd = System.currentTimeMillis();
		System.out.println("Time of pruning: " + (pruneEnd - pruneStart)/1000);
	}
	
	public List<AbstractBlock> parseIndex(Map<String, Set<Integer>> invertedIndex) {
		List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
		tokenToIndexMap = new HashMap<>();
		int blockIndex = 0;
		for (Entry<String, Set<Integer>> term : invertedIndex.entrySet()) {
			Set<Integer> ids = term.getValue();
			int size = ids.size();
			if (1 < size) {
//				List<Integer> sample = ids.stream().collect(Collectors.toList());
//				Collections.shuffle(sample);
//				int new_size = (int) Math.round(size * SAMPLING_PERCENT);
//				int[] idsArray = new int[new_size];
//				for(int i = 0; i < new_size; i++) idsArray[i] = sample.get(i);
				int[] idsArray = Converter.convertListToArray(term.getValue());
				UnilateralBlock uBlock = new UnilateralBlock(idsArray);
				this.tokenToIndexMap.put(blockIndex, term.getKey());
				uBlock.setBlockIndex(blockIndex);
				blockIndex++;
				blocks.add(uBlock);
			}
		}
		return blocks;
	}
	
	private Set<Integer> getDisjunctionEntities(List<String> tokens) {
		Set<Integer> unionEntities = new HashSet<>();
		for(String token : tokens) {
			Set<Integer> entities = bBlocks.get(token);
			
			if(entities == null) continue;
			unionEntities.addAll(entities);
		}
		unionEntities.removeAll(entitiesWithLinks);
		return unionEntities;
	}
	
	private Set<Integer> getConjuctionEntities(List<List<String>> tokens) {
		Set<Set<Integer>> uniqueEntitiesSets = new HashSet<>();
		for(List<String> tokenSet : tokens) {
			Set<Integer> uniqueEntitiesSet = new HashSet<>();
			for(String token : tokenSet) {
				Set<Integer> entities = bBlocks.get(token);
				if(entities == null) continue;
				uniqueEntitiesSet.addAll(entities);
			}
			uniqueEntitiesSets.add(uniqueEntitiesSet);
		}
		Set<Integer> uniqueEntities = new HashSet<>();
		int index = 0;
		for(Set<Integer> uniqueEntitiesSet : uniqueEntitiesSets) {
			if(index == 0) uniqueEntities.addAll(uniqueEntitiesSet);
			else uniqueEntities.retainAll(uniqueEntitiesSet);
			index ++;
		}
		uniqueEntities.removeAll(entitiesWithLinks);
		return uniqueEntities;

	}

	public void getCoarseStats(Set<Integer> entities) {
		double uniqueEntitiesSize = entities.size();

		if(uniqueEntitiesSize == 0)
			this.comparisons = 0.0;
		else {
			entitiesToBlocks.keySet().retainAll(entities);
			this.comparisons = uniqueEntitiesSize * this.entitiesToBlocks.size();
		}
	}
	
	public void computeStats(Set<Integer> entities) {
		double uniqueEntitiesSize = entities.size();
		//System.out.println("Unique entities size: " + uniqueEntitiesSize);

		if(uniqueEntitiesSize == 0)
			this.comparisons = 0.0;
		else {
			
			
			Long totalComps = 0L;
			Set<String> uniqueTokens = new HashSet<>();
			Set<Long> uniqueComparisons = new TreeSet<>();
			List<Long> blocksSize = new ArrayList<>();
			Set<Integer> allEntities = new HashSet<>();
			double firstComputation = System.currentTimeMillis();
//			for(Integer entity : entities) {
//				for(String token : entitiesToBlocks.get(entity)) {
//					if(uniqueTokens.contains(token)) continue;
//					uniqueTokens.add(token);
//					long size = bBlocks.get(token).size();
//					long comps = (size*(size-1))/2;
//					totalComps += comps;
//					blocksSize.add(size);
//					uniqueComparisons.add(comps);
//	//				Set<Integer> entityIds = bBlocks.get(token);
//	//				allEntities.addAll(entityIds);	
//	
//				}
//			}
//			compsRatio =  totalComps / this.blockIndexStatistic.getTotalComparisons();
//			bBlocks.keySet().retainAll(uniqueTokens);
//			double lastComputation = System.currentTimeMillis();
//			System.out.println("Time of first calculation: " + (lastComputation - firstComputation)/1000);
//	//		System.out.println("Total comparisons: " + totalComps);
//	//		System.out.println("Total entities: " + allEntities.size());
//	//		System.out.println("Block size: " + uniqueTokens.size());
//	//		System.out.println();
//			purgeBlocks(getMaxComparisonsPerBlock(blocksSize, uniqueComparisons));
//			clearEntitiesToBlocks();
//			filterBlocks(0.35);	
//			calculateStats();
		}
	}
	
	private void clearEntitiesToBlocks() {
		double cleanStart = System.currentTimeMillis();
		Set<String> tokens = bBlocks.keySet();
		for(Set<String> blocks : entitiesToBlocks.values()) {
			blocks.retainAll(tokens);
		}
		double cleanEnd = System.currentTimeMillis();
		System.out.println("Time of cleaning: " + (cleanEnd - cleanStart)/1000);
	}
	
	private void calculateStats() {
		double start = System.currentTimeMillis();
		System.out.println("Number of Blocks: " + bBlocks.size());
		this.comparisons = 0.0;
		Set<Integer> uniqueEntities = new HashSet<>();
		for(String token : bBlocks.keySet()) {
			Set<Integer> blockEntities = bBlocks.get(token);
			uniqueEntities.addAll(blockEntities);
			long size = blockEntities.size();
			this.comparisons += (size*(size-1))/2;
		}
		System.out.println("Total Comparisons: " + this.comparisons);
		System.out.println("Total Entities: " + uniqueEntities.size());
		double end = System.currentTimeMillis();
		System.out.println("Calculation time: " + (end - start)/1000);
	}
	
	public void getTokens(List<RexNode> conjuctions){
		List<List<String>> allTokens = new ArrayList<>();
		Integer betweenBeginInt = 0;
		Integer betweenEndInt = 0;
		if(conjuctions == null) return;
		for(RexNode condition : conjuctions) {		
			String kind = condition.getKind().toString();
			final RexCall rexCall = (RexCall) condition;
			TokenVisitor tokenVisitor = new TokenVisitor();
			rexCall.accept(tokenVisitor);
			List<String> tokens = new ArrayList<>();
			String token = tokenVisitor.getToken();
			if(kind.equals("OR")) {
				List<String> disjTokens = new ArrayList<>(); 
				for(RexNode disjCondition : rexCall.getOperands()) {
					final RexCall rexCallConj = (RexCall) disjCondition;
					TokenVisitor tokenVisitorConj = new TokenVisitor();
					rexCallConj.accept(tokenVisitorConj);
					String disjToken = tokenVisitorConj.getToken();
					disjToken = disjToken.replace("%", "").replace(":VARCHAR", "").replace("'", "").toLowerCase();
					disjTokens.add(disjToken);
				}
				getCoarseStats(getDisjunctionEntities(disjTokens));
			}
			else {
				switch(kind) {
					case("LIKE"):
						token = token.replace("%", "").replace("'", "").toLowerCase();
						//if its more than 1 tokens
						if(token.contains(" ")) 
							tokens.addAll(Arrays.stream(token.split(" ")).collect(Collectors.toList()));
						else
							tokens.add(token);
						break;
					case("GREATER_THAN"):
						token = token.replace("'", "").toLowerCase();
						if(isNumeric(token)) 
							betweenBeginInt = Integer.parseInt(token) + 1;
						break;
					case("GREATER_THAN_OR_EQUAL"):
						token = token.replace("'", "").toLowerCase();
						if(isNumeric(token)) 
							betweenBeginInt = Integer.parseInt(token);
						break;
					case("LESS_THAN"):
						token = token.replace("'", "").toLowerCase();
						if(isNumeric(token)) 
							betweenEndInt = Integer.parseInt(token) - 1;
						break;
					case("LESS_THAN_OR_EQUAL"):
						token = token.replace("'", "").toLowerCase();
						if(isNumeric(token)) 
							betweenEndInt = Integer.parseInt(token);
						break;
					case("EQUALS"):
						token = token.replace("'", "").replace(":VARCHAR", "").toLowerCase();
						if(token.contains(" ")) 
							tokens.addAll(Arrays.stream(token.split(" ")).collect(Collectors.toList()));
						else
							tokens.add(token);
						break;
					case("OR"):
						System.out.println(token);				
						break;
					default:
						token = token.replace("'", "").replace(":varchar", "").toLowerCase();
						if(token.contains(" ")) 
							tokens.addAll(Arrays.stream(token.split(" ")).collect(Collectors.toList()));
						else
							tokens.add(token);
						break;
				}
				if(!tokens.isEmpty()) allTokens.add(tokens);
			}
		}
		List<String> tokens = new ArrayList<>();

		if(betweenEndInt != 0 && betweenBeginInt != 0) {
			for(int i = betweenBeginInt; i <= betweenEndInt; i ++) {
				tokens.add(Integer.toString(i));
			}
		}
		else if(betweenEndInt != 0 && betweenBeginInt == 0) {
			for(int i = 0; i <= betweenEndInt; i ++) {
				tokens.add(Integer.toString(i));
			}
		}
		else if(betweenEndInt == 0 && betweenBeginInt != 0) {
			for(int i = betweenBeginInt; i <= 100; i ++) {
				tokens.add(Integer.toString(i));
			}
		}
		if(!tokens.isEmpty()) allTokens.add(tokens);
		getCoarseStats(getConjuctionEntities(allTokens));
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

	public Double getComparisons() {
		return comparisons;
	}

	public void setComparisons(Double comparisons) {
		this.comparisons = comparisons;
	}
	
	
}
