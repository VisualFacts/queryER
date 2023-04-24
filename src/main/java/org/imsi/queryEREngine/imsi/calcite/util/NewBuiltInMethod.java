package org.imsi.queryEREngine.imsi.calcite.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.Types;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityResolvedTuple;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;;
public enum NewBuiltInMethod {
	DEDUPLICATE_ENUM(DeduplicationExecution.class, "deduplicateEnumerator", Enumerable.class,
			String.class, Integer.class, String.class, List.class, AtomicBoolean.class, List.class),
	MERGE_ENTITIES(DeduplicationExecution.class, "mergeEntities", EntityResolvedTuple.class, List.class, List.class), 
	HASH_JOIN_DIRTY_RIGHT(DeduplicationJoinExecution.class, "dirtyRightJoin", EntityResolvedTuple.class, Enumerable.class,
			Function1.class,
			Function1.class, Function2.class,
			String.class, List.class, Integer.class, String.class, Integer.class, EqualityComparer.class,
			boolean.class, boolean.class, Predicate2.class),
	HASH_JOIN_DIRTY_LEFT(DeduplicationJoinExecution.class, "dirtyLeftJoin", Enumerable.class, EntityResolvedTuple.class, 
			Function1.class,
			Function1.class, Function2.class,
			String.class, List.class, Integer.class, String.class, Integer.class, EqualityComparer.class,
			boolean.class, boolean.class, Predicate2.class),
	HASH_JOIN_CLEAN(DeduplicationJoinExecution.class, "cleanJoin", EntityResolvedTuple.class, EntityResolvedTuple.class, 
			Function1.class,
			Function1.class, Function2.class, EqualityComparer.class,
			boolean.class, boolean.class, Predicate2.class);
	public final Method method;
	public final Constructor constructor;
	public final Field field;
	public static final ImmutableMap<Method, NewBuiltInMethod> MAP;

	static {
		final ImmutableMap.Builder<Method, NewBuiltInMethod> builder =
				ImmutableMap.builder();
		for (NewBuiltInMethod value : NewBuiltInMethod.values()) {
			if (value.method != null) {
				builder.put(value.method, value);
			}
		}
		MAP = builder.build();
	}

	NewBuiltInMethod(Method method, Constructor constructor, Field field) {
		this.method = method;
		this.constructor = constructor;
		this.field = field;
	}

	/** Defines a method. */
	NewBuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
		this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
	}

	/** Defines a constructor. */
	NewBuiltInMethod(Class clazz, Class... argumentTypes) {
		this(null, Types.lookupConstructor(clazz, argumentTypes), null);
	}

	/** Defines a field. */
	NewBuiltInMethod(Class clazz, String fieldName, boolean dummy) {
		this(null, null, Types.lookupField(clazz, fieldName));
		assert dummy : "dummy value for method overloading must be true";
	}
}
