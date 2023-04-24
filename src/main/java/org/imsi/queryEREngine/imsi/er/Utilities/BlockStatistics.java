package org.imsi.queryEREngine.imsi.er.Utilities;


import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.univocity.parsers.csv.CsvWriter;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.BilateralBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.DataStructures.DecomposedBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.GroundTruthEntityIndex;
import org.imsi.queryEREngine.imsi.er.DataStructures.IdDuplicates;
import org.imsi.queryEREngine.imsi.er.DataStructures.UnilateralBlock;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.ComparisonRefinement.AbstractDuplicatePropagation;



public class BlockStatistics implements Constants {

    private double pc;
    private double pq;
    
    private int noOfD1Entities;
    private int noOfD2Entities;
    private int detectedDuplicates;
    
    private final AbstractDuplicatePropagation abstractDP;
    private final List<AbstractBlock> blocks;
    private GroundTruthEntityIndex entityIndex;
    private FileWriter csvWriter;
    
    public BlockStatistics(List<AbstractBlock> bl, AbstractDuplicatePropagation adp, FileWriter csvWriter) {
        abstractDP = adp;
        abstractDP.resetDuplicates();
        blocks = bl;
        this.csvWriter = csvWriter;
    }

    public double[] applyProcessing() throws IOException {
        System.out.println("No of blocks\t:\t" + blocks.size());
    	csvWriter.append(Integer.toString(blocks.size()) + ",");
        double[] values = new double[3];
        if (blocks.isEmpty()) {
            values[0] = 0;
            values[1] = 0;
            values[2] = 0;
        } else {
            double totalComparisons = getComparisonsCardinality();
            if (blocks.get(0) instanceof DecomposedBlock) {
                getDecomposedBlocksEntities(totalComparisons);
            } else {
                entityIndex = new GroundTruthEntityIndex(blocks, abstractDP.getDuplicates());
                getEntities();
            }
            getBlockingCardinality();
            if (blocks.get(0) instanceof DecomposedBlock) {
                getDuplicatesOfDecomposedBlocks(totalComparisons);
            } else {
                getDuplicatesWithEntityIndex(totalComparisons);
            }
        
            values[0] = pc;
            values[1] = pq;
            values[2] = totalComparisons;
        }
        return values;
    }

    private boolean areCooccurring(boolean cleanCleanER, IdDuplicates pairOfDuplicates) {
        int[] blocks1 = entityIndex.getEntityBlocks(pairOfDuplicates.getEntityId1(), 0);
        if (blocks1 == null) {
            return false;
        }
        
        int[] blocks2 = entityIndex.getEntityBlocks(pairOfDuplicates.getEntityId2(), cleanCleanER?1:0);
        if (blocks2 == null) {
            return false;
        }
        
        int noOfBlocks1 = blocks1.length;
        int noOfBlocks2 = blocks2.length;
        for (int i = 0; i < noOfBlocks1; i++) {
            for (int j = 0; j < noOfBlocks2; j++) {
                if (blocks2[j] < blocks1[i]) {
                    continue;
                }

                if (blocks1[i] < blocks2[j]) {
                    break;
                }

                if (blocks1[i] == blocks2[j]) {
                    return true;
                }
            }
        }
        
        return false;
    }

    private void getBilateralBlockingCardinality() throws IOException {
        //System.out.println("\n\nGetting bilateral BC...");

        double d1BlockAssignments = 0;
        double d2BlockAssignments = 0;
        for (AbstractBlock block : blocks) {
            BilateralBlock bilBlock = (BilateralBlock) block;
            d1BlockAssignments += bilBlock.getIndex1Entities().length;
            d2BlockAssignments += bilBlock.getIndex2Entities().length;
        }

        System.out.println("Average block\t:\t" + d1BlockAssignments / blocks.size() + "-" + d2BlockAssignments / blocks.size());
        System.out.println("iBC_1\t:\t" + d1BlockAssignments / noOfD1Entities);
        System.out.println("iBC_2\t:\t" + d2BlockAssignments / noOfD2Entities);
        System.out.println("oBC\t:\t" + ((d1BlockAssignments + d2BlockAssignments) / (noOfD1Entities + noOfD2Entities)));
        
        
    }

    private void getBlockingCardinality() throws IOException {
        if (blocks.get(0) instanceof BilateralBlock) {
            getBilateralBlockingCardinality();
        } else if (blocks.get(0) instanceof UnilateralBlock) {
            getUnilateralBlockingCardinality();
        } /*else if (blocks.get(0) instanceof DecomposedBlock) {
            getDecomposedBlockingCardinality();
        }*/
    }
    
    private void getDecomposedBlockingCardinality () throws IOException {
        DecomposedBlock deBlock = (DecomposedBlock) blocks.get(0);
        if (deBlock.isCleanCleanER()) {
            double d1BlockAssignments = 0;
            double d2BlockAssignments = 0;
            for (AbstractBlock block : blocks) {
                DecomposedBlock bilBlock = (DecomposedBlock) block;
                d1BlockAssignments += bilBlock.getEntities1().length;
                d2BlockAssignments += bilBlock.getEntities2().length;
            }

            System.out.println("Average block\t:\t" + d1BlockAssignments / blocks.size() + "-" + d2BlockAssignments / blocks.size());
            System.out.println("iBC_1\t:\t" + d1BlockAssignments / noOfD1Entities);
            System.out.println("iBC_2\t:\t" + d2BlockAssignments / noOfD2Entities);
            System.out.println("oBC\t:\t" + ((d1BlockAssignments + d2BlockAssignments) / (noOfD1Entities + noOfD2Entities)));
            csvWriter.append(d1BlockAssignments / blocks.size() + "-" + d2BlockAssignments / blocks.size() +
            		Double.toString(d1BlockAssignments / blocks.size()) + "," +
    				Double.toString(d1BlockAssignments / noOfD1Entities) + "," +
            		Double.toString(d1BlockAssignments / noOfD2Entities) + "," +
					Double.toString(((d1BlockAssignments + d2BlockAssignments) / (noOfD1Entities + noOfD2Entities))) + ","); 
        } else {
            double blockAssignments = 0;
            for (AbstractBlock block : blocks) {
                DecomposedBlock uniBlock = (DecomposedBlock) block;
                blockAssignments += uniBlock.getTotalBlockAssignments();
            }

            System.out.println("Average block\t:\t" + blockAssignments / blocks.size());
            System.out.println("BC\t:\t" + blockAssignments / noOfD1Entities);
        }
    }

    private double getComparisonsCardinality() throws IOException {
        //System.out.println("\n\nGetting comparisons cardinality...");

        double aggregateCardinality = 0;
        double blockAssignments = 0;
        for (AbstractBlock block : blocks) {
            aggregateCardinality += block.getNoOfComparisons();
            blockAssignments += block.getTotalBlockAssignments();
        }

        //System.out.println("Aggregate cardinality\t:\t" + aggregateCardinality);
        //System.out.println("CC\t:\t" + (blockAssignments / aggregateCardinality));

        csvWriter.append(Double.toString(aggregateCardinality) + "," + Double.toString(blockAssignments / aggregateCardinality) + ",");
        return aggregateCardinality;
    }

    private void getDecomposedBlocksEntities(double totalComparisons) throws IOException {
        DecomposedBlock deBlock = (DecomposedBlock) blocks.get(0);
        final Set<Integer> entitiesD1 = new HashSet<Integer>((int)totalComparisons);
        if (deBlock.isCleanCleanER()) {
            final Set<Integer> entitiesD2 = new HashSet<Integer>((int)totalComparisons);
            for (AbstractBlock block : blocks) {
                ComparisonIterator iterator = block.getComparisonIterator();
                while (iterator.hasNext()) { 
                    Comparison comparison = iterator.next();
                    entitiesD1.add(comparison.getEntityId1());
                    entitiesD2.add(comparison.getEntityId2());
                }
            }
            noOfD1Entities = entitiesD1.size();
            noOfD2Entities = entitiesD2.size();
            //System.out.println("Entities in blocks\t:\t" + (noOfD1Entities+noOfD2Entities));
            csvWriter.append(String.valueOf(noOfD1Entities+noOfD2Entities));
        } else {
            for (AbstractBlock block : blocks) {
                ComparisonIterator iterator = block.getComparisonIterator();
                while (iterator.hasNext()) {

                    Comparison comparison = iterator.next();
                    entitiesD1.add(comparison.getEntityId1());
                    entitiesD1.add(comparison.getEntityId2());
                }
            }
            noOfD1Entities = entitiesD1.size();
            System.out.println("Entities in blocks\t:\t" + noOfD1Entities);
            csvWriter.append(String.valueOf(noOfD1Entities+noOfD2Entities) + ",");

        }
    }
    
    public int getDetectedDuplicates() {
        return detectedDuplicates;
    }
    
    private void getDuplicatesOfDecomposedBlocks(double totalComparisons) throws IOException {
        System.out.println("\n\nGetting duplicates...");

        for (AbstractBlock block : blocks) {
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                abstractDP.isSuperfluous(iterator.next());
            }

        }

        detectedDuplicates = abstractDP.getNoOfDuplicates();
        pc = ((double)abstractDP.getNoOfDuplicates()) / abstractDP.getExistingDuplicates();
        pq = abstractDP.getNoOfDuplicates() / totalComparisons;
        System.out.println("Detected duplicates\t:\t" + abstractDP.getNoOfDuplicates());
        System.out.println("PC\t:\t" + pc);
        System.out.println("PQ\t:\t" + pq);
        csvWriter.append(abstractDP.getNoOfDuplicates() + "," + pc + "," + pq + ",");
        
    }
    
    private void getDuplicatesWithEntityIndex(double totalComparisons) throws IOException {
        //System.out.println("\n\nGetting duplicates...");

        double noOfDuplicates = 0;
        boolean cleanCleanER = blocks.get(0) instanceof BilateralBlock;
        for (IdDuplicates pairOfDuplicates : abstractDP.getDuplicates()) {
            if (areCooccurring(cleanCleanER, pairOfDuplicates)) {
                noOfDuplicates++;
            } 
        }
        
        detectedDuplicates = (int) noOfDuplicates;
        pc = noOfDuplicates / abstractDP.getExistingDuplicates();
        pq = noOfDuplicates / totalComparisons;
        System.out.println("Detected duplicates\t:\t" + noOfDuplicates);
        System.out.println("PC\t:\t" + pc);
        System.out.println("PQ\t:\t" + pq);
        csvWriter.append(Double.toString(noOfDuplicates) + "," + 
        		Double.toString(pc) + "," + Double.toString(pq) + ",");

    }

    private void getEntities() throws IOException {
        if (blocks.get(0) instanceof UnilateralBlock) {
            Set<Integer> distinctEntities = new HashSet<Integer>();
            for (AbstractBlock block : blocks) {
                UnilateralBlock uBlock = (UnilateralBlock) block;
                for (int entityId : uBlock.getEntities()) {
                    distinctEntities.add(entityId);
                }
            }
        
            noOfD1Entities = distinctEntities.size();
            //System.out.println("Total entities\t:\t" + entityIndex.getNoOfEntities());
            //System.out.println("Entities in blocks\t:\t" + noOfD1Entities);
            //System.out.println("Singleton entities\t:\t" + (entityIndex.getNoOfEntities()-noOfD1Entities));
            
            csvWriter.append(Integer.toString(entityIndex.getNoOfEntities()) + "," +
            					Integer.toString(noOfD1Entities) + "," + 
            		Integer.toString(entityIndex.getNoOfEntities()-noOfD1Entities) + ",");

        } else {
            Set<Integer> distinctEntitiesD1 = new HashSet<Integer>();
            Set<Integer> distinctEntitiesD2 = new HashSet<Integer>();
            for (AbstractBlock block : blocks) {
                BilateralBlock bBlock = (BilateralBlock) block;
                for (int entityId : bBlock.getIndex1Entities()) {
                    distinctEntitiesD1.add(entityId);
                }
                for (int entityId : bBlock.getIndex2Entities()) {
                    distinctEntitiesD2.add(entityId);
                }
            }
            
            noOfD1Entities = distinctEntitiesD1.size();
            noOfD2Entities = distinctEntitiesD2.size();
            System.out.println("Total entities D1\t:\t" + entityIndex.getDatasetLimit());
            System.out.println("Singleton entities D1\t:\t" + (entityIndex.getDatasetLimit()-noOfD1Entities));
            System.out.println("Total entities D2\t:\t" + (entityIndex.getNoOfEntities()-entityIndex.getDatasetLimit()));
            System.out.println("Singleton entities D2\t:\t" + (entityIndex.getNoOfEntities()-entityIndex.getDatasetLimit()-noOfD2Entities));
            System.out.println("Entities in blocks\t:\t" + (noOfD1Entities+noOfD2Entities));
        }
    }

    public void getPerformance(List<AbstractBlock> blockCollection, AbstractDuplicatePropagation adp) {
        System.out.println("\n\nGetting performance...");

        double comparisons = 0;
        for (AbstractBlock block : blockCollection) {
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                comparisons++;
                Comparison comparison = iterator.next();
                adp.isSuperfluous(comparison);
            }
        }

        System.out.println("Detected duplicates\t:\t" + adp.getNoOfDuplicates());
        System.out.println("Executed comparisons\t:\t" + comparisons);
    }

    private void getUnilateralBlockingCardinality() throws IOException {
        //System.out.println("\n\nGetting unilateral BC...");

        double blockAssignments = 0;
        for (AbstractBlock block : blocks) {
            UnilateralBlock uniBlock = (UnilateralBlock) block;
            blockAssignments += uniBlock.getTotalBlockAssignments();
        }

        //System.out.println("Average block\t:\t" + blockAssignments / blocks.size());
        //System.out.println("BC\t:\t" + blockAssignments / noOfD1Entities);
        csvWriter.append(Double.toString(blockAssignments / blocks.size()) + "," +
				Double.toString(blockAssignments / noOfD1Entities) + ",");

    }
}