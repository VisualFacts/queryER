/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */

package org.imsi.queryEREngine.imsi.er.EfficiencyLayer.BlockRefinement;


import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.AbstractEfficiencyMethod;

import java.util.Iterator;
import java.util.List;



public class BlockSizePurging extends AbstractEfficiencyMethod {
    
    private final int maxEntities;
    
    public BlockSizePurging(int maxEnt) {
        super("Block Size Purging");
        maxEntities = maxEnt;
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        Iterator blocksIterator = blocks.iterator();
        while (blocksIterator.hasNext()) {
            AbstractBlock currentBlock = (AbstractBlock) blocksIterator.next();
            if(currentBlock.isProtected()){
                System.err.println("Block is protected "+currentBlock.getNoOfComparisons());

                continue;
            }
            if (maxEntities < currentBlock.getTotalBlockAssignments()) {
                blocksIterator.remove();
            } 
        }
    }    
}