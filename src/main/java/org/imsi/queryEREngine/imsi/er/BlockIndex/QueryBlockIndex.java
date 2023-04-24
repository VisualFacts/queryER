package org.imsi.queryEREngine.imsi.er.BlockIndex;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.imsi.queryEREngine.imsi.er.DataStructures.*;
import org.imsi.queryEREngine.imsi.er.Utilities.Converter;
import org.imsi.queryEREngine.imsi.er.Utilities.DumpDirectories;
import org.imsi.queryEREngine.imsi.er.Utilities.SerializationUtilities;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class QueryBlockIndex extends BlockIndex {

    protected Set<Integer> qIds;

    private static List<String> queryTokens;

    public QueryBlockIndex() {
        this.qIds = new HashSet<>();
    }

    public QueryBlockIndex(String tableName) {
        this();
        blockIndexStatistic = loadBlockIndexStatistic(tableName);
    }

    public BlockIndexStatistic
            loadBlockIndexStatistic(String tableName){
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(new File(dumpDirectories.getBlockIndexStatsDirPath() + tableName + ".json"),
                    BlockIndexStatistic.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<AbstractBlock> parseIndex(Map<String, Set<Integer>> invertedIndex, Set<Integer> qIds) {
        List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
        for (Map.Entry<String, Set<Integer>> term : invertedIndex.entrySet()) {
            if (1 < term.getValue().size()) {
                boolean isProtected = false;
                if(queryTokens.contains(term.getKey().trim())){

                    System.err.println("Protected terms: " + term.getKey());
                    isProtected = true;
                }


                int[] idsArray = Converter.convertListToArray(term.getValue());
                UnilateralBlock uBlock = new UnilateralBlock(idsArray, qIds);
                uBlock.setProtected(isProtected);
                blocks.add(uBlock);
            }
        }
        invertedIndex.clear();
        return blocks;
    }

    public void createBlockIndex(HashMap<Integer, Object[]> queryData, Integer key) {
        // Get project results from previous enumerator
        for (Object[] row : queryData.values()) {
            Object[] currentLine = (Object[]) row;
            Integer fields = currentLine.length;
            String entityKey = currentLine[key].toString();
            if (entityKey.contentEquals("")) continue;
            EntityProfile eP = new EntityProfile(currentLine[key].toString()); // 0 is id, must put this in schema catalog
            qIds.add(Integer.valueOf(entityKey));
            int index = 0;
            while (index < fields) {
                if (index != key) {
                    eP.addAttribute(index, currentLine[index].toString());
                    ;
                }
                index++;
            }
            this.entityProfiles.add(eP);

        }
    }


    public void createBlockIndex(HashMap<Integer, Object[]> queryData, Integer key, List<String> tokens) {
        List<String> queryTokens = new ArrayList<>();

//        queryTokens.add("powershot");
//        queryTokens.add("digital");
        queryTokens.addAll(tokens);
        this.queryTokens= queryTokens;
        // Get project results from previous enumerator
        for (Object[] row : queryData.values()) {
            Object[] currentLine = (Object[]) row;
            Integer fields = currentLine.length;
            String entityKey = currentLine[key].toString();
            if (entityKey.contentEquals("")) continue;
            EntityProfile eP = new EntityProfile(currentLine[key].toString()); // 0 is id, must put this in schema catalog
            qIds.add(Integer.valueOf(entityKey));
            int index = 0;
            while (index < fields) {
                if (index != key) {
                    eP.addAttribute(index, currentLine[index].toString());
                    ;
                }
                index++;
            }
            this.entityProfiles.add(eP);

        }
    }


    public <T> void createBlockIndex(List<T> dataset, Integer key) {
        // Get project results from previous enumerator
        for (T row : dataset) {
            Object[] currentLine = (Object[]) row;
            Integer fields = currentLine.length;
            String entityKey = currentLine[key].toString();
            if (entityKey.contentEquals("")) continue;
            EntityProfile eP = new EntityProfile(currentLine[key].toString()); // 0 is id, must put this in schema catalog
            qIds.add(Integer.valueOf(entityKey));
            int index = 0;
            while (index < fields) {
                if (index != key) {
                    eP.addAttribute(index, currentLine[index].toString());
                    ;
                }
                index++;
            }
            this.entityProfiles.add(eP);

        }
    }

    public void buildQueryBlocks() {
        this.invertedIndex = indexEntities(0, entityProfiles);
    }

    @SuppressWarnings("unchecked")
    public List<AbstractBlock> joinBlockIndices(String name, boolean doER) {
        if (doER) {
            DumpDirectories dumpDirectories = new DumpDirectories();
            final Map<String, Set<Integer>> bBlocks = (Map<String, Set<Integer>>) SerializationUtilities
                    .loadSerializedObject(dumpDirectories.getBlockIndexDirPath() + name + "InvertedIndex");
            bBlocks.keySet().retainAll(this.invertedIndex.keySet());
            return parseIndex(bBlocks, qIds);
        } else return new ArrayList<>();
    }

    public Set<Integer> blocksToEntities(List<AbstractBlock> blocks) {
        Set<Integer> joinedEntityIds = new HashSet<>();
//        if (blocks.get(0) instanceof UnilateralBlock) {
            for (AbstractBlock block : blocks) {
                UnilateralBlock uBlock = (UnilateralBlock) block;
                int[] entities = uBlock.getEntities();
                joinedEntityIds.addAll(Arrays.stream(entities).boxed().collect(Collectors.toSet()));
            }
//        } else if (blocks.get(0) instanceof BilateralBlock) {
//            for (AbstractBlock block : blocks) {
//                BilateralBlock bBlock = (BilateralBlock) block;
//                int[] entities = bBlock.getIndex1Entities();
//                int[] queryEntities = bBlock.getIndex2Entities();
//                joinedEntityIds.addAll(Arrays.stream(entities).boxed().collect(Collectors.toSet()));
//                joinedEntityIds.addAll(Arrays.stream(queryEntities).boxed().collect(Collectors.toSet()));
//            }
//        }
        return joinedEntityIds;
    }

    public Set<Integer> blocksToEntitiesD(List<AbstractBlock> blocks) {
        Set<Integer> joinedEntityIds = new HashSet<>();
        for (AbstractBlock block : blocks) {
            DecomposedBlock dBlock = (DecomposedBlock) block;
            int[] entities1 = dBlock.getEntities1();
            int[] entities2 = dBlock.getEntities2();
            joinedEntityIds.addAll(Arrays.stream(entities1).boxed().collect(Collectors.toSet()));
            joinedEntityIds.addAll(Arrays.stream(entities2).boxed().collect(Collectors.toSet()));

        }
        return joinedEntityIds;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Set<Integer>> indexEntities(int sourceId, List<EntityProfile> profiles) {
        invertedIndex = new HashMap<String, Set<Integer>>();
        InputStream file = this.getClass().getClassLoader().getResourceAsStream("stopwords_SER");
        //file = new File(this.getClass().getClassLoader().getResource("stopwords_SER").toExternalForm());
        HashSet<String> stopwords = (HashSet<String>) SerializationUtilities
                .loadSerializedObject(file);
        HashMap<String, Integer> tfIdf = new HashMap<>();
        for (EntityProfile profile : profiles) {
            for (Attribute attribute : profile.getAttributes()) {
                if (attribute.getValue() == null)
                    continue;
                String cleanValue = attribute.getValue().replaceAll("_", " ").trim().replaceAll("\\s*,\\s*$", "")
                        .toLowerCase();
                for (String token : cleanValue.split("[\\W_]")) {
                    if (2 < token.trim().length()) {
                        if (stopwords.contains(token.toLowerCase()))
                            continue;
                        Set<Integer> termEntities = invertedIndex.computeIfAbsent(token.trim(),
                                x -> new HashSet<Integer>());

                        termEntities.add(Integer.parseInt(profile.getEntityUrl()));
                        //int tokenCount = tfIdf.containsKey(token) ? tfIdf.get(token) : 0;
                        //tfIdf.put(token, tokenCount + 1);
                    }
                }
            }

        }
        //this.setTfIdf(tfIdf);
        return invertedIndex;
    }

    public Set<Integer> getIds() {
        return qIds;
    }


}
