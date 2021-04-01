package org.imsi.queryEREngine.imsi.er.BlockBuilding;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.Utilities.Constants;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class Utilities implements Constants {

   
    public static Set<String> getCombinationsFor(List<String> sublists, int sublistLength) {
        if (sublistLength == 0 || sublists.size() < sublistLength) {
            return new HashSet<>();
        }

        List<String> remainingElements = new ArrayList<>(sublists);
        String lastSublist = remainingElements.remove(sublists.size() - 1);

        final Set<String> combinationsExclusiveX = getCombinationsFor(remainingElements, sublistLength);
        final Set<String> combinationsInclusiveX = getCombinationsFor(remainingElements, sublistLength - 1);

        final Set<String> resultingCombinations = new HashSet<>();
        resultingCombinations.addAll(combinationsExclusiveX);
        if (combinationsInclusiveX.isEmpty()) {
            resultingCombinations.add(lastSublist);
        } else {
            combinationsInclusiveX.stream().forEach((combination) -> {
                resultingCombinations.add(combination + lastSublist);
            });
        }
        return resultingCombinations;
    }

    public static Set<String> getExtendedSuffixes(int minimumLength, String blockingKey) {
        final Set<String> suffixes = new HashSet<>();
        suffixes.add(blockingKey);
        if (minimumLength <= blockingKey.length()) {
            for (int nGramSize = blockingKey.length() - 1; minimumLength <= nGramSize; nGramSize--) {
                int currentPosition = 0;
                final int length = blockingKey.length() - (nGramSize - 1);
                while (currentPosition < length) {
                    String newSuffix = blockingKey.substring(currentPosition, currentPosition + nGramSize);
                    suffixes.add(newSuffix);
                    currentPosition++;
                }
            }
        }
        return suffixes;
    }

    public static double getJaccardSimilarity(int[] tokens1, int[] tokens2) {
        double commonTokens = 0.0;
        int noOfTokens1 = tokens1.length;
        int noOfTokens2 = tokens2.length;
        for (int i = 0; i < noOfTokens1; i++) {
            for (int j = 0; j < noOfTokens2; j++) {
                if (tokens2[j] < tokens1[i]) {
                    continue;
                }

                if (tokens1[i] < tokens2[j]) {
                    break;
                }

                if (tokens1[i] == tokens2[j]) {
                    commonTokens++;
                }
            }
        }
        return commonTokens / (noOfTokens1 + noOfTokens2 - commonTokens);
    }

    public static List<String> getNGrams(int n, String blockingKey) {
        final List<String> nGrams = new ArrayList<>();
        if (blockingKey.length() < n) {
            nGrams.add(blockingKey);
        } else {
            int currentPosition = 0;
            final int length = blockingKey.length() - (n - 1);
            while (currentPosition < length) {
                nGrams.add(blockingKey.substring(currentPosition, currentPosition + n));
                currentPosition++;
            }
        }
        return nGrams;
    }

    public static Set<String> getSuffixes(int minimumLength, String blockingKey) {
        final Set<String> suffixes = new HashSet<>();
        if (blockingKey.length() < minimumLength) {
            suffixes.add(blockingKey);
        } else {
            int limit = blockingKey.length() - minimumLength + 1;
            for (int i = 0; i < limit; i++) {
                suffixes.add(blockingKey.substring(i));
            }
        }
        return suffixes;
    }


    public static void purgeBlocksByAssignments(int maxAssignments, List<AbstractBlock> blocks) {
        Iterator<AbstractBlock> blocksIterator = blocks.iterator();
        while (blocksIterator.hasNext()) {
            AbstractBlock block = blocksIterator.next();
            if (maxAssignments < block.getTotalBlockAssignments()) {
                blocksIterator.remove();
            }
        }
    }
}