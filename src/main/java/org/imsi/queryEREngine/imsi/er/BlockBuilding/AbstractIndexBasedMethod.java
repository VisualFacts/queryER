
package org.imsi.queryEREngine.imsi.er.BlockBuilding;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Attribute;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityProfile;
import org.imsi.queryEREngine.imsi.er.DataStructures.UnilateralBlock;
import org.imsi.queryEREngine.imsi.er.Utilities.Constants;
import org.imsi.queryEREngine.imsi.er.Utilities.Converter;
import org.imsi.queryEREngine.imsi.er.Utilities.SerializationUtilities;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public abstract class AbstractIndexBasedMethod extends AbstractBlockingMethod implements Constants {

    protected final boolean cleanCleanER;
    protected int sourceId;
    protected double[] noOfEntities;
    
    protected final HashMap<String, Set<Integer>> invertedIndex;
    protected List<AbstractBlock> blocks;
    protected final String[] entitiesPath;
    protected final String[] indexPath;
    protected final List<EntityProfile>[] entityProfiles;

    public AbstractIndexBasedMethod(String description, List<EntityProfile>[] profiles) {
        super(description);
        entitiesPath = null;
        indexPath = null;
        blocks = new ArrayList<>();
        entityProfiles = profiles;
        noOfEntities = new double[entityProfiles.length];
        invertedIndex = new HashMap<>();
        if (entityProfiles.length == 2) {
            cleanCleanER = true;
        } else {
            cleanCleanER = false;
        }
    }

    public AbstractIndexBasedMethod(String description, String[] entities, String[] index) {
        super(description);
        entitiesPath = entities;
        indexPath = index;
        blocks = new ArrayList<>();
        entityProfiles = new List[entitiesPath.length];
        noOfEntities = new double[entitiesPath.length];
        invertedIndex = new HashMap<>();
        if (entitiesPath.length == 2) {
            cleanCleanER = true;
        } else {
            cleanCleanER = false;
        }
    }

    @Override
    public List<AbstractBlock> buildBlocks() {
        return parseIndex(this.invertedIndex);
    }

    public void buildIndex() {
        List<EntityProfile> entityProfiles = getProfiles();
        indexEntities(entityProfiles);
        noOfEntities[sourceId] = entityProfiles.size();
    }


    protected abstract Set<String> getBlockingKeys(String attributeValue);

    public double getBruteForceComparisons() {
        if (noOfEntities.length == 1) {
            return noOfEntities[0] * (noOfEntities[0] - 1) / 2;
        }
        return noOfEntities[0] * noOfEntities[1];
    }

    protected List<EntityProfile> getProfiles() {
        if (entitiesPath != null) {
            entityProfiles[sourceId] = loadEntities(entitiesPath[sourceId]);
        }
        return entityProfiles[sourceId];
    }

    public double getTotalNoOfEntities() {
        if (noOfEntities.length == 1) {
            return noOfEntities[0];
        }
        return noOfEntities[0] + noOfEntities[1];
    }

    protected void indexEntities(List<EntityProfile> entities) {
    	for (EntityProfile profile : entities) {
    		for (Attribute attribute : profile.getAttributes()) {
    			getBlockingKeys(attribute.getValue()).stream().filter((key) -> (0 < key.trim().length())).forEach((key) -> {
    				Set<Integer> termEntities = this.invertedIndex.computeIfAbsent(key.trim(),
    						x -> new HashSet<Integer>());
    				termEntities.add(Integer.parseInt(profile.getEntityUrl()));
    			});
    		}

    	}
    }
    
    public static List<AbstractBlock> parseIndex(Map<String, Set<Integer>> invertedIndex) {
		final List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
		for (Entry<String, Set<Integer>> term : invertedIndex.entrySet()) {
			if (1 < term.getValue().size()) {
				int[] idsArray = Converter.convertListToArray(term.getValue());
				UnilateralBlock uBlock = new UnilateralBlock(idsArray);
				blocks.add(uBlock);
			}
		}
		invertedIndex.clear();
		return blocks;
	}

    protected List<EntityProfile> loadEntities(String entitiesPath) {
        return (List<EntityProfile>) SerializationUtilities.loadSerializedObject(entitiesPath);
    }

	public HashMap<String, Set<Integer>> getInvertedIndex() {
		return this.invertedIndex;
	}
    
	
}
