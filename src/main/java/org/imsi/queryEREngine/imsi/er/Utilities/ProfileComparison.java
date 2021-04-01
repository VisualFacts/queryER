package org.imsi.queryEREngine.imsi.er.Utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.imsi.queryEREngine.imsi.er.DataStructures.Attribute;

public class ProfileComparison {

	private static int getTranspositions(List<Character> source, List<Character> target) {
		if (source.isEmpty() || target.isEmpty() || (source.size() != target.size())) {
			return -1;
		}
		int transpositions = 0;
		for (int i = 0; i < source.size(); i++) {
			if (!source.get(i).equals(target.get(i)))
				transpositions++;
		}
		transpositions /= 2.0f;
		return transpositions;
	}

	private static List<Character> getCommonCharacters(final String string1, final String string2,
			final int distanceSep) {
		// create a return buffer of characters
		List<Character> returnCommons = new ArrayList<Character>();
		// create a copy of string2 for processing
		char[] copy = string2.toCharArray();
		// iterate over string1
		int n = string1.length();
		int m = string2.length();
		for (int i = 0; i < n; i++) {
			char ch = string1.charAt(i);
			// set boolean for quick loop exit if found
			boolean foundIt = false;
			// compare char with range of characters to either side

			for (int j = Math.max(0, i - distanceSep); !foundIt && j < Math.min(i + distanceSep, m); j++) {
				// check if found
				if (copy[j] == ch) {
					foundIt = true;
					// append character found
					returnCommons.add(ch);
					// alter copied string2 for processing
					copy[j] = (char) 0;
				}
			}
		}
		return returnCommons;
	}

	private static float getSimilarity(String string1, String string2) {

		// get half the length of the string rounded up - (this is the distance
		// used for acceptable transpositions)
		int halflen = ((Math.min(string1.length(), string2.length()))) / 2;

		// get common characters
		List<Character> common1 = getCommonCharacters(string1, string2, halflen);
		List<Character> common2 = getCommonCharacters(string2, string1, halflen);

		// check for zero in common

		// get the number of transpositions
		int transpositions = getTranspositions(common1, common2);
		if (transpositions == -1)
			return 0f;

		// calculate jaro metric
		return (common1.size() / ((float) string1.length()) + common2.size() / ((float) string2.length())
				+ (common1.size() - transpositions) / ((float) common1.size())) / 3.0f;
	}

	public static double getJaroSimilarity(Set<Attribute> profile1, Set<Attribute> profile2) {
		String string1 = "";
		String string2 = "";
		HashMap<Integer, String> at1 = new HashMap<>();
		HashMap<Integer, String> at2 = new HashMap<>();
		for (Attribute attribute1 : profile1) {
			if (attribute1.getValue() == null)
				continue;
			at1.put(attribute1.getIndex(), attribute1.getValue());
		}

		for (Attribute attribute2 : profile2) {
			if (attribute2.getValue() == null)
				continue;
			at2.put(attribute2.getIndex(), attribute2.getValue());
		}

		int total = 0;
		double mean = 0;
		double acc = 0;
		for (Integer key : at1.keySet()) {
			if (at1.get(key) == null || at1.get(key) == "" || at1.get(key).equals("[\\W_]"))
				continue;
			if (at2.get(key) == null || at2.get(key) == "" || at2.get(key).equals("[\\W_]"))
				continue;
			string1 = at1.get(key).trim().replaceAll("s*,", "").replaceAll("s*,", "").replaceAll("s*:", "");
			string2 = at2.get(key).trim().replaceAll("s*,", "").replaceAll("s*,", "").replaceAll("s*:", "");

			acc += new JaroWinklerSimilarity().apply(string1, string2);

			total++;
		}

		mean = acc / total;

		return mean;
	}

	public static double jaro(String s, String t) {
		int s_len = s.length();
		int t_len = t.length();

		if (s_len == 0 && t_len == 0)
			return 1;

		int match_distance = Integer.max(s_len, t_len) / 2 - 1;

		boolean[] s_matches = new boolean[s_len];
		boolean[] t_matches = new boolean[t_len];

		int matches = 0;
		int transpositions = 0;

		for (int i = 0; i < s_len; i++) {
			int start = Integer.max(0, i - match_distance);
			int end = Integer.min(i + match_distance + 1, t_len);

			for (int j = start; j < end; j++) {
				if (t_matches[j])
					continue;
				if (s.charAt(i) != t.charAt(j))
					continue;
				s_matches[i] = true;
				t_matches[j] = true;
				matches++;
				break;
			}
		}

		if (matches == 0)
			return 0;

		int k = 0;
		for (int i = 0; i < s_len; i++) {
			if (!s_matches[i])
				continue;
			while (!t_matches[k])
				k++;
			if (s.charAt(i) != t.charAt(k))
				transpositions++;
			k++;
		}

		return (((double) matches / s_len) + ((double) matches / t_len)
				+ ((matches - transpositions / 2.0) / matches)) / 3.0;
	}

	// Function to remove the element 
	public static Object[] removeArrayElement(Object[] oddArray,  
			int index) { 
		// delete the element at specified index and return the array 
		int size = oddArray.length;
		Object[] cleanArr = new Object[size];
		System.arraycopy(oddArray, index + 1, cleanArr, index, size - 1 - index);
		return cleanArr;
	}

	public static Set<String> getDistinctTokens(Set<Attribute> nameValuePairs) {
		final Set<String> tokensFrequency = new HashSet<String>(5 * nameValuePairs.size());
		for (Attribute attribute : nameValuePairs) {
			if (attribute.getValue() == null)
				continue;
			String[] tokens = attribute.getValue().split("[\\W_]");
			tokensFrequency.addAll(Arrays.asList(tokens));
		}

		return tokensFrequency;
	}
	
	public static Set<String> getDistinctTokens(Object[] nameValuePairs) {
		final Set<String> tokensFrequency = new HashSet<String>(5 * (nameValuePairs.length - 1));
		for (Object attr : nameValuePairs) {
			if(attr != null) {
				String attrStr = attr.toString();
				if (attrStr.equals(""))
					continue;
				String[] tokens = attrStr.split("[\\W_]");
				tokensFrequency.addAll(Arrays.asList(tokens));
			}
		}

		return tokensFrequency;
	}

	public static double getJaccardSimilarity(Set<Attribute> profile1, Set<Attribute> profile2) {
		final Set<String> tokenizedProfile1 = getDistinctTokens(profile1);
		final Set<String> tokenizedProfile2 = getDistinctTokens(profile2);

		final Set<String> allTokens = new HashSet<String>(tokenizedProfile1);
		allTokens.addAll(tokenizedProfile2);

		tokenizedProfile1.retainAll(tokenizedProfile2);
		return ((double) tokenizedProfile1.size()) / allTokens.size();
	}
	
	public static double getJaccardSimilarity(Object[] profile1, Object[] profile2, Integer keyIndex) {
		Object[] profile1Cleaned = removeArrayElement(profile1, keyIndex);
		Object[] profile2Cleaned = removeArrayElement(profile2, keyIndex);

		final Set<String> tokenizedProfile1 = getDistinctTokens(profile1Cleaned);
		final Set<String> tokenizedProfile2 = getDistinctTokens(profile2Cleaned);

		final Set<String> allTokens = new HashSet<String>(tokenizedProfile1);
		allTokens.addAll(tokenizedProfile2);

		tokenizedProfile1.retainAll(tokenizedProfile2);
		return ((double) tokenizedProfile1.size()) / allTokens.size();
	}

	public static double getJaroSimilarity(Object[] entity1, Object[] entity2, Integer keyIndex) {
		// TODO Auto-generated method stub
		String string1 = "";
		String string2 = "";
		HashMap<Integer, String> at1 = new HashMap<>();
		HashMap<Integer, String> at2 = new HashMap<>();
		Integer length = entity1.length;
		int index = 0;
		
		while (index < length) {
			if(index != keyIndex) {
				if (entity1[index].equals("")) {
					index += 1;
					continue;
				}
				at1.put(index, entity1[index].toString());
			}
			index += 1;
		}
		length = entity2.length;
		index = 0;
		while (index < length) {
			if(index != keyIndex) {
				if (entity2[index].equals("")) {
					index += 1;
					continue;
				}
				at2.put(index, entity2[index].toString());
			}
			index += 1;
		}
		int total = 0;
		double mean = 0;
		double acc = 0;
		for (Integer key : at1.keySet()) {
			if (at1.get(key) == null || at1.get(key) == "" || at1.get(key).equals("[\\W_]"))
				continue;
			if (at2.get(key) == null || at2.get(key) == "" || at2.get(key).equals("[\\W_]"))
				continue;
			string1 = at1.get(key).trim().replaceAll("s*,", "").replaceAll("s*,", "").replaceAll("s*:", "");
			string2 = at2.get(key).trim().replaceAll("s*,", "").replaceAll("s*,", "").replaceAll("s*:", "");

			acc += new JaroWinklerSimilarity().apply(string1, string2);

			total++;
		}

		mean = acc / total;
		return mean;
		
	}

	

	

}