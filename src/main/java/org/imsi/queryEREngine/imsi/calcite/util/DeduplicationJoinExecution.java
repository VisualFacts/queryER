package org.imsi.queryEREngine.imsi.calcite.util;

import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.imsi.queryEREngine.apache.calcite.util.Sources;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvEnumerator;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvFieldType;
import org.imsi.queryEREngine.imsi.er.BlockIndex.QueryBlockIndex;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityResolvedTuple;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.BlockFiltering;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.EfficientEdgePruning;
import org.imsi.queryEREngine.imsi.er.Utilities.ExecuteBlockComparisons;
import org.imsi.queryEREngine.imsi.er.Utilities.UnionFind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A dirty right/left join takes as input one entityResolvedTuples and an enumerable.
 * The entityResolvedTuple contains the data + a hashset that for each of the entities
 * describes its duplicates as found by the comparison execution. It is a hashjoin, that hashes the
 * smallest table by the join column and the big table by its id.
 * <p>
 * We enumerate the second hash and for each entity (id only maps to one entity) we get the entities from the
 * right that join with it. Then for both the left and right entities we get their duplicates and perform the
 * left product of these two sets joining all of the entities.
 * During these process we keep track of the already visited ids of each table so we don't perform the same join
 * twice.
 */
public class DeduplicationJoinExecution {

	protected static final Logger DEDUPLICATION_EXEC_LOGGER = LoggerFactory.getLogger(DeduplicationExecution.class);
	
	/**
	 * Executes the algorithm of the dirty right join.
	 * Can deduplicate the table immediately after the scan or by getting the dirty matches first to
	 * minimize the right table entities.
	 *
	 * @param <TSource>            Can be used to make the Object[] type of the entities generic
	 * @param <TRight>             Can be used to make the Object[] type of the entities generic
	 * @param <TKey>               Can be used to make the Integer type of the entities generic
	 * @param <TResult>            Can be used to make the Object[] type of the entities generic
	 * @param <TLeft>             Can be used to make the Object[] type of the entities generic
	 * @param left                Left data tuple
	 * @param right                Right data enumerable
	 * @param leftKeySelector     Function that gets key column from entity of left
	 * @param rightKeySelector     Function that gets key column from entity of right
	 * @param resultSelector       Function that generates the merged entities after the join
	 * @param keyRight             Right table key position
	 * @param rightTableName       Right table Name
	 * @param rightTableSize       Right table column length
	 * @param comparer             Used for joins
	 * @param generateNullsOnLeft  Used for joins
	 * @param generateNullsOnRight Used for joins
	 * @param predicate            Used for joinss
	 * @return EntityResolvedTuple that contains the joined entities and their union find data
	 * @throws IOException 
	 */
	@SuppressWarnings("rawtypes")
	public static <TSource, TRight, TKey, TResult, TLeft> EntityResolvedTuple dirtyRightJoin(EntityResolvedTuple left,
			Enumerable<Object[]> right,
			Function1<Object[], TKey> leftKeySelector,
			Function1<Object[], TKey> rightKeySelector,
			Function2<Object[], Object[], Object[]> resultSelector,
			String sourceRight,
			List<CsvFieldType> fieldTypes,
			Integer keyRight,
			String rightTableName,
			Integer rightTableSize,
			EqualityComparer<TKey> comparer,
			boolean generateNullsOnLeft, boolean generateNullsOnRight,
			Predicate2<Object[], Object[]> predicate
			) {

		HashMap<Integer, Object[]> filteredData = new HashMap<>();

		filteredData = getDirtyMatches((Enumerable<Object[]>) Linq4j.asEnumerable(left.finalData),
				right, leftKeySelector, rightKeySelector, resultSelector, comparer,
				generateNullsOnLeft, generateNullsOnRight, predicate, keyRight);
		AtomicBoolean ab = new AtomicBoolean();
		ab.set(false);
		CsvEnumerator<Object[]> originalEnumerator = new CsvEnumerator(Sources.of(new File(sourceRight)),
				ab, identityList(rightTableSize), keyRight);


		//Deduplicate the dirty table
		EntityResolvedTuple entityResolvedTuple = DeduplicationExecution.deduplicate(filteredData, keyRight, rightTableSize,
				rightTableName, originalEnumerator, sourceRight);


		EntityResolvedTuple joinedEntityResolvedTuple  =
				deduplicateJoin(left, entityResolvedTuple, leftKeySelector, rightKeySelector,
						resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate);
		joinedEntityResolvedTuple.getAll();
		return joinedEntityResolvedTuple;
	}

	/**
	 * Executes the algorithm of the dirty left join.
	 * Can deduplicate the table immediately after the scan or by getting the dirty matches first to
	 * minimize the left table entities.
	 *
	 * @param <TSource>           Can be used to make the Object[] type of the entities generic
	 * @param <TRight>            Can be used to make the Object[] type of the entities generic
	 * @param <TKey>              Can be used to make the Integer type of the entities generic
	 * @param <TResult>           Can be used to make the Object[] type of the entities generic
	 * @param <TLeft>            Can be used to make the Object[] type of the entities generic
	 * @param left               Left data tuple
	 * @param right               Right data enumerable
	 * @param leftKeySelector    Function that gets key column from entity of left
	 * @param rightKeySelector    Function that gets key column from entity of right
	 * @param resultSelector      Function that generates the merged entities after the join
	 * @param keyleft             left table key position
	 * @param leftTableName       left table Name
	 * @param leftTableSize       left table column length
	 * @param comparer            Used for joins
	 * @param generateNullsOnLeft Used for joins
	 * @param generateNullsOnleft Used for joins
	 * @param predicate           Used for joinss
	 * @return EntityResolvedTuple that contains the joined entities and their union find data
	 * @throws IOException 
	 */
	@SuppressWarnings("rawtypes")
	public static <TSource, TRight, TKey, TResult, TLeft> EntityResolvedTuple dirtyLeftJoin(Enumerable<Object[]> left,
			EntityResolvedTuple<Object[]> right,
			Function1<Object[], TKey> leftKeySelector,
			Function1<Object[], TKey> rightKeySelector,
			Function2<Object[], Object[], Object[]> resultSelector,
			String sourceLeft,
			List<CsvFieldType> fieldTypes,
			Integer keyLeft,
			String leftTableName,
			Integer leftTableSize,
			EqualityComparer<TKey> comparer,
			boolean generateNullsOnLeft, boolean generateNullsOnRight,
			Predicate2<Object[], Object[]> predicate
			) {


		HashMap<Integer, Object[]> filteredData = new HashMap<>();
		filteredData = getDirtyMatches(Linq4j.asEnumerable(right.finalData), left,
				rightKeySelector, leftKeySelector, resultSelector, comparer,
				generateNullsOnRight, generateNullsOnLeft, predicate, keyLeft);
		AtomicBoolean ab = new AtomicBoolean();
		ab.set(false);
		CsvEnumerator<Object[]> originalEnumerator = new CsvEnumerator(Sources.of(new File(sourceLeft)),
				ab, identityList(leftTableSize), keyLeft);

		// Deduplicate the dirty table
		EntityResolvedTuple entityResolvedTuple = DeduplicationExecution.deduplicate(filteredData, keyLeft, leftTableSize,
				leftTableName, originalEnumerator, sourceLeft);
		// Reverse the right, left structure
		EntityResolvedTuple  joinedEntityResolvedTuple =
				deduplicateJoin(entityResolvedTuple, right, leftKeySelector, rightKeySelector,
						resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate);
		joinedEntityResolvedTuple.getAll();
		return joinedEntityResolvedTuple;
	}

	/**
	 * Clean join performs the join algorithm on two cleaned datasets
	 */
	@SuppressWarnings("rawtypes")
	public static <TSource, TRight, TKey, TResult, TLeft> EntityResolvedTuple cleanJoin(EntityResolvedTuple<Object[]> left,
			EntityResolvedTuple<Object[]> right,
			Function1<Object[], TKey> leftKeySelector,
			Function1<Object[], TKey> rightKeySelector,
			Function2<Object[], Object[], Object[]> resultSelector,
			EqualityComparer<TKey> comparer,
			boolean generateNullsOnLeft, boolean generateNullsOnRight,
			Predicate2<Object[], Object[]> predicate
			) {

		EntityResolvedTuple joinedEntityResolvedTuple =
				deduplicateJoin(left, right, leftKeySelector, rightKeySelector,
						resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate);
		joinedEntityResolvedTuple.getAll();

		return joinedEntityResolvedTuple;
	}


	/**
	 * Implements a faux-join only to get the entities that match for the hashing table.
	 * This way we can perform deduplication on a subset of the data.
	 *
	 * @param <TSource>
	 * @param <TRight>
	 * @param <TKey>
	 * @param left                Left data enumerable
	 * @param right                Right data enumerable
	 * @param leftKeySelector     Function that gets key column from entity of left
	 * @param rightKeySelector     Function that gets key column from entity of right
	 * @param resultSelector       Function that generates the merged entities after the join
	 * @param comparer             Used for joins
	 * @param generateNullsOnLeft  Used for joins
	 * @param generateNullsOnRight Used for joins
	 * @param predicate            Used for joinss
	 * @return
	 */
	@SuppressWarnings("unused")
	private static <TSource, TRight, TKey> HashMap<Integer, Object[]> getDirtyMatches(Enumerable<Object[]> left,
			Enumerable<Object[]> right,
			Function1<Object[], TKey> leftKeySelector,
			Function1<Object[], TKey> rightKeySelector,
			Function2<Object[], Object[], Object[]> resultSelector,
			EqualityComparer<TKey> comparer,
			boolean generateNullsOnLeft, boolean generateNullsOnRight,
			Predicate2<Object[], Object[]> predicate,
			Integer key) {

		final Enumerable<Object[]> rightToLookUp =  Linq4j.asEnumerable(right.toList());
		final Lookup<TKey, Object[]> rightLookup = rightToLookUp.toLookup(rightKeySelector);
		rightLookup.remove("");
		Enumerator<Object[]> lefts = left.enumerator();
		Enumerator<Object[]> rights = Linq4j.emptyEnumerator();

		final Set<Integer> dirtyIds = new HashSet<>();
		final HashMap<Integer, Object[]> dirtyData = new HashMap<>();

		for (; ; ) {

			if (!lefts.moveNext()) {
				break;
			}
			final Object[] left2 = lefts.current();
			Enumerable<Object[]> rightEnumerable;
			if (left2 == null) {
				rightEnumerable = null;
			} else {
				final TKey leftKey = leftKeySelector.apply(left2);
				if (leftKey == null) {
					rightEnumerable = null;
				} else {
					rightEnumerable = rightLookup.get(leftKey);
					if (rightEnumerable != null) {
						try (Enumerator<Object[]> rightEnumerator =
								rightEnumerable.enumerator()) {
							while (rightEnumerator.moveNext()) {
								final Object[] right2 = rightEnumerator.current();
								try {
									String right2Key = right2[key].toString();
									if (!dirtyData.keySet().contains(Integer.parseInt(right2Key))) {
										dirtyData.put(Integer.parseInt(right2Key), right2);
									}
								}
								catch (Exception e) { continue; }
							}
						}
					}
				}
			}
		}
		return dirtyData;
	}


	/**
	 * Executes a deduplicated join, that takes as input two entity resolved tuples and returns another entity resolved join tuple
	 * The joined table gets a new column that contains the identification numbers of each row. This is needed to identify the duplicate
	 * entities in the Union Find.
	 *
	 * @param <TSource>
	 * @param <TRight>
	 * @param <TKey>
	 * @param left                Left deduplicated tuple
	 * @param right                Right deduplicated tuple
	 * @param leftKeySelector     Function that gets key column from entity of left
	 * @param rightKeySelector     Function that gets key column from entity of right
	 * @param resultSelector       Function that generates the merged entities after the join
	 * @param comparer             Used for joins
	 * @param generateNullsOnLeft  Used for joins
	 * @param generateNullsOnRight Used for joins
	 * @param predicate            Used for joins
	 * 	 * @return
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	private static <TSource, TRight, TKey> EntityResolvedTuple deduplicateJoin(EntityResolvedTuple left,
			EntityResolvedTuple right,
			Function1<Object[], TKey> leftKeySelector,
			Function1<Object[], TKey> rightKeySelector,
			Function2<Object[], Object[], Object[]> resultSelector,
			EqualityComparer<TKey> comparer,
			boolean generateNullsOnLeft, boolean generateNullsOnRight,
			Predicate2<Object[], Object[]> predicate) {

		double deduplicateJoinStartTime = System.currentTimeMillis();
		final Enumerable<Object[]> rightToLookUp = Linq4j.asEnumerable(right.finalData);
		right.finalData.forEach(row -> {
			//			Object[] r = (Object[]) row;
			//
			//			System.out.println(r[0]);
		});
		final Lookup<TKey, Object[]> rightLookup =
				comparer == null
				? rightToLookUp.toLookup(rightKeySelector)
						: rightToLookUp
						.toLookup(rightKeySelector, comparer);

				HashMap<Integer, Object[]> leftsMap = left.data;
				HashMap<Integer, Object[]> rightsMap = right.data;
				HashMap<Integer, Set<Integer>> leftMatches = left.revUF;
				HashMap<Integer, Set<Integer>> rightMatches = right.revUF;

				Set<Integer> joinedIds = new HashSet<>();
				Integer joinedId = 0; // left id to enumerate the duplicates
				joinedIds.add(joinedId);
				UnionFind joinedUFind = new UnionFind(joinedIds);
				//List<Object[]> joinedEntities = new ArrayList<>();
				HashMap<Integer, Object[]> joinedEntities = new HashMap<>();
				Set<Integer> leftCheckedIds = new HashSet<>(leftsMap.size());

				for (Integer leftId : leftsMap.keySet()) {		
					Set<Integer> leftMatchedIds = leftMatches.get(leftId);
					if(leftCheckedIds.contains(leftId)) continue;
					leftCheckedIds.addAll(leftMatchedIds);
					Set<Integer> rightJoinIds  = new HashSet<>();
					for (Integer leftMatchedId : leftMatchedIds) {
						Object[] leftCurrent = leftsMap.get(leftMatchedId);	
						TKey leftKey = leftKeySelector.apply(leftCurrent);
						Enumerable<Object[]> rightEnumerable = rightLookup.get(leftKey); // do this for each similar
						if(rightEnumerable != null) {
							try (Enumerator<Object[]> rightEnumerator =
									rightEnumerable.enumerator()) {
								while (rightEnumerator.moveNext()) {
									final Object[] rightMatched = rightEnumerator.current();
									Integer rightId = Integer.parseInt(rightMatched[0].toString());
									rightJoinIds.add(rightId);
								}
							}
						}
					}

					Set<Integer> rightCheckedIds = new HashSet<>();
					for(Integer rightJoinId : rightJoinIds) {
						Integer rightJoinedId = joinedId; 
						Set<Integer> rightMatchedIds = rightMatches.get(rightJoinId);	
						if(rightCheckedIds.contains(rightJoinId)) continue;
						rightCheckedIds.addAll(rightMatchedIds);
						for (Integer leftMatchedId : leftMatchedIds) {
							Object[] leftCurrent = leftsMap.get(leftMatchedId);
							for(Integer rightMatchedId : rightMatchedIds) {
								Object[] rightCurrent = rightsMap.get(rightMatchedId);
								Object[] joinedEntity = resultSelector.apply(leftCurrent, rightCurrent);
								joinedEntity = appendValue(joinedEntity, rightJoinedId);
								joinedEntities.put(rightJoinedId, joinedEntity);
								joinedUFind.union(joinedId, rightJoinedId);
								rightJoinedId += 1;
							}
						}
						joinedId = rightJoinedId + 1;
					}
				}

				double deduplicateJoinEndTime = System.currentTimeMillis();
				if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
					DEDUPLICATION_EXEC_LOGGER.debug(left.finalData.size() + "," + right.finalData.size() + "," + joinedEntities.size() + "," +
							(deduplicateJoinEndTime - deduplicateJoinStartTime) / 1000);
				System.out.println(left.finalData.size() + "," + right.finalData.size() + "," + joinedEntities.size() + "," +
						(deduplicateJoinEndTime - deduplicateJoinStartTime) / 1000);
				int len = joinedEntities.get(0).length;
				return new EntityResolvedTuple(joinedEntities, joinedUFind, len - 1, len);
	}

	/**
	 * @param obj
	 * @param newObj
	 * @return object array with obj appended on its end
	 */
	private static Object[] appendValue(Object[] obj, Object newObj) {
		ArrayList<Object> temp = new ArrayList<Object>(Arrays.asList(obj));
		temp.add(newObj);
		return temp.toArray();

	}


	public static List<CsvFieldType> identityList(int n) {
		List<CsvFieldType> csvFieldTypes = new ArrayList<>();
		for(int i = 0; i < n; i++) {
			csvFieldTypes.add(null);
		}
		return csvFieldTypes;
	}


}
