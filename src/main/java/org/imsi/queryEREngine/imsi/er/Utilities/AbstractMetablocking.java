package org.imsi.queryEREngine.imsi.er.Utilities;

import org.apache.commons.lang.ArrayUtils;
import org.imsi.queryEREngine.imsi.er.DataStructures.*;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.AbstractEfficiencyMethod;
import org.imsi.queryEREngine.imsi.er.Utilities.MetaBlockingConfiguration.WeightingScheme;

import java.util.*;


public abstract class AbstractMetablocking extends AbstractEfficiencyMethod {

    protected double totalBlocks;
    protected double validComparisons;
    protected double[] comparisonsPerBlock;
    protected double[] redundantCPE;
    protected double[] comparisonsPerEntity;
    
    protected EntityIndex entityIndex;
    protected final WeightingScheme weightingScheme;
    
    public AbstractMetablocking(String description, WeightingScheme scheme) {
        super(description + " + " + scheme);
        weightingScheme = scheme;
    }
    
    protected DecomposedBlock getDecomposedBlock (boolean cleanCleanER, List<Integer> entities1, List<Integer> entities2) {
        int[] entityIds1 = Converter.convertListToArray(entities1);
        int[] entityIds2 = Converter.convertListToArray(entities2);
        return new DecomposedBlock(cleanCleanER, entityIds1, entityIds2);
    }

    protected UnilateralBlock getUnilateralBlock (boolean cleanCleanER, List<Integer> entities1, List<Integer> entities2) {
        int[] entityIds1 = Converter.convertListToArray(entities1);
        int[] entityIds2 = Converter.convertListToArray(entities2);
        int[] entityIds = ArrayUtils.addAll(entityIds1,entityIds2);
        return new UnilateralBlock(entityIds);
    }


    protected DecomposedBlock getDecomposedBlock (boolean cleanCleanER, Iterator<Comparison> iterator) {
        final List<Integer> entities1 = new ArrayList<Integer>();
        final List<Integer> entities2 = new ArrayList<Integer>();
        while (iterator.hasNext()) {
            Comparison comparison = iterator.next();
            entities1.add(comparison.getEntityId1());
            entities2.add(comparison.getEntityId2());
        }
        int[] entityIds1 = Converter.convertListToArray(entities1);
        int[] entityIds2 = Converter.convertListToArray(entities2);
        return new DecomposedBlock(cleanCleanER, entityIds1, entityIds2);
    }
    
    protected void getStatistics(List<AbstractBlock> blocks) {
        if (entityIndex == null) {
            entityIndex = new EntityIndex(blocks);
        }
        
        validComparisons = 0;
        totalBlocks = blocks.size();
        redundantCPE = new double[entityIndex.getNoOfEntities()];
        comparisonsPerBlock = new double[(int)(totalBlocks + 1)];
        comparisonsPerEntity = new double[entityIndex.getNoOfEntities()];
        for (AbstractBlock block : blocks) {
            comparisonsPerBlock[block.getBlockIndex()] = block.getNoOfComparisons();
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                int entityId2 = comparison.getEntityId2()+entityIndex.getDatasetLimit();
                
                redundantCPE[comparison.getEntityId1()]++;
                redundantCPE[entityId2]++;
                if (!entityIndex.isRepeated(block.getBlockIndex(), comparison)) {
                    validComparisons++;
                    comparisonsPerEntity[comparison.getEntityId1()]++;
                    comparisonsPerEntity[entityId2]++;
                }
            }
        }
    }

    protected void getStatistics(List<AbstractBlock> blocks, Set<Integer> qIds) {
        if (entityIndex == null) {
            entityIndex = new EntityIndex(blocks);
        }

//        validComparisons = 0;
        totalBlocks = blocks.size();
//        redundantCPE = new double[entityIndex.getNoOfEntities()];
//        comparisonsPerBlock = new double[(int)(totalBlocks + 1)];
//        comparisonsPerEntity = new double[entityIndex.getNoOfEntities()];
//        for (AbstractBlock block : blocks) {
//            comparisonsPerBlock[block.getBlockIndex()] = block.getNoOfComparisons();
//            QueryComparisonIterator iterator = block.getQueryComparisonIterator(qIds);
//            HashSet<Comparison> uComp = new HashSet<>();
//            while (iterator.hasNext()) {
//                Comparison comparison = iterator.next();
//
//                if (comparison.getEntityId1() == comparison.getEntityId2()) continue;
//                if (comparison.getEntityId1() > comparison.getEntityId2())
//                    comparison = new Comparison(false, comparison.getEntityId2(), comparison.getEntityId1());
//
//                if (uComp.contains(comparison)) continue;
//
//                uComp.add(comparison);
//
//                int entityId2 = comparison.getEntityId2()+entityIndex.getDatasetLimit();
//
//                redundantCPE[comparison.getEntityId1()]++;
//                redundantCPE[entityId2]++;
//                validComparisons++;
//                comparisonsPerEntity[comparison.getEntityId1()]++;
//                comparisonsPerEntity[entityId2]++;
////                if (!entityIndex.isRepeated(block.getBlockIndex(), comparison)) {
////                    validComparisons++;
////                    comparisonsPerEntity[comparison.getEntityId1()]++;
////                    comparisonsPerEntity[entityId2]++;
////                }
//            }
//        }
    }
    
    protected void getValidComparisons(List<AbstractBlock> blocks) {
    	initializeEntityIndex(blocks);
        
        validComparisons = 0;
   
        for (AbstractBlock block : blocks) {
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();     
                if (!entityIndex.isRepeated(block.getBlockIndex(), comparison)) {
                    validComparisons++;    
                }
            }
        }
		System.out.println("Valid comps real: " + validComparisons);
    }
    
    protected void initializeEntityIndex(List<AbstractBlock> blocks) {
    	 if (entityIndex == null) {
             entityIndex = new EntityIndex(blocks);
         }
    }

    protected double getWeight(int blockIndex, Comparison comparison) {
        switch (weightingScheme) {
            case ARCS:
                final List<Integer> commonIndices = entityIndex.getCommonBlockIndices(blockIndex, comparison);
                if (commonIndices == null) {
                    return -1;
                }

                double totalWeight = 0;
                for (Integer index : commonIndices) {
                    totalWeight += 1.0 / comparisonsPerBlock[index];
                }
                return totalWeight;
            case CBS:
//                double cb = entityIndex.getNoOfCommonBlocks(blockIndex, comparison);
//               if(Double.isNaN(cb)) System.err.println(cb);
                return entityIndex.getNoOfCommonBlocks(blockIndex, comparison);
            case ECBS:
                double commonBlocks = entityIndex.getNoOfCommonBlocks(blockIndex, comparison);
                if (commonBlocks < 0) {
                    return commonBlocks;
                }
                double f = Math.log10(totalBlocks / entityIndex.getNoOfEntityBlocks(comparison.getEntityId1(), 0)) * Math.log10(totalBlocks / entityIndex.getNoOfEntityBlocks(comparison.getEntityId2(), comparison.isCleanCleanER()?1:0));
//                               if(Double.isNaN(f) || Double.isNaN(commonBlocks) || Double.isNaN(f*commonBlocks)) System.err.println(f+"   "+commonBlocks);
//                System.err.println(commonBlocks * );
                return commonBlocks * Math.log10(totalBlocks / entityIndex.getNoOfEntityBlocks(comparison.getEntityId1(), 0)) * Math.log10(totalBlocks / entityIndex.getNoOfEntityBlocks(comparison.getEntityId2(), comparison.isCleanCleanER()?1:0));
            case JS:
                double commonBlocksJS = entityIndex.getNoOfCommonBlocks(blockIndex, comparison);
                if (commonBlocksJS < 0) {
                    return commonBlocksJS;
                }
                return commonBlocksJS / (entityIndex.getNoOfEntityBlocks(comparison.getEntityId1(), 0) + entityIndex.getNoOfEntityBlocks(comparison.getEntityId2(), comparison.isCleanCleanER()?1:0) - commonBlocksJS);
            case EJS:
                double commonBlocksEJS = entityIndex.getNoOfCommonBlocks(blockIndex, comparison);
                if (commonBlocksEJS < 0) {
                    return commonBlocksEJS;
                }

                double probability = commonBlocksEJS / (entityIndex.getNoOfEntityBlocks(comparison.getEntityId1(), 0) + entityIndex.getNoOfEntityBlocks(comparison.getEntityId2(), comparison.isCleanCleanER()?1:0) - commonBlocksEJS);
                return probability * Math.log10(validComparisons / comparisonsPerEntity[comparison.getEntityId1()]) * Math.log10(validComparisons / comparisonsPerEntity[comparison.isCleanCleanER()?comparison.getEntityId2()+entityIndex.getDatasetLimit():comparison.getEntityId2()]);
        }

        return -1;
    }
    
    public void resetEntityIndex() {
        entityIndex = null;
    }
}