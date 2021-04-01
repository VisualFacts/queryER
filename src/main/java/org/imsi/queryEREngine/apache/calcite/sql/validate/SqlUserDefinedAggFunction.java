/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.imsi.queryEREngine.apache.calcite.sql.validate;

import java.util.ArrayList;
import java.util.List;

import org.imsi.queryEREngine.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.function.Experimental;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataType;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataTypeFactory;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.imsi.queryEREngine.apache.calcite.schema.AggregateFunction;
import org.imsi.queryEREngine.apache.calcite.schema.FunctionParameter;
import org.imsi.queryEREngine.apache.calcite.sql.SqlAggFunction;
import org.imsi.queryEREngine.apache.calcite.sql.SqlFunctionCategory;
import org.imsi.queryEREngine.apache.calcite.sql.SqlIdentifier;
import org.imsi.queryEREngine.apache.calcite.sql.SqlKind;
import org.imsi.queryEREngine.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.imsi.queryEREngine.apache.calcite.sql.type.SqlOperandTypeInference;
import org.imsi.queryEREngine.apache.calcite.sql.type.SqlReturnTypeInference;
import org.imsi.queryEREngine.apache.calcite.sql.type.SqlTypeName;
import org.imsi.queryEREngine.apache.calcite.util.Optionality;
import org.imsi.queryEREngine.apache.calcite.util.Util;

import com.google.common.collect.Lists;

/**
 * User-defined aggregate function.
 *
 * <p>Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.</p>
 */
public class SqlUserDefinedAggFunction extends SqlAggFunction {
	public final AggregateFunction function;

	/** This field is is technical debt; see [CALCITE-2082] Remove
	 * RelDataTypeFactory argument from SqlUserDefinedAggFunction constructor. */
	@Experimental
	public final RelDataTypeFactory typeFactory;

	/** Creates a SqlUserDefinedAggFunction. */
	public SqlUserDefinedAggFunction(SqlIdentifier opName,
			SqlReturnTypeInference returnTypeInference,
			SqlOperandTypeInference operandTypeInference,
			SqlOperandTypeChecker operandTypeChecker, AggregateFunction function,
			boolean requiresOrder, boolean requiresOver,
			Optionality requiresGroupOrder, RelDataTypeFactory typeFactory) {
		super(Util.last(opName.names), opName, SqlKind.OTHER_FUNCTION,
				returnTypeInference, operandTypeInference, operandTypeChecker,
				SqlFunctionCategory.USER_DEFINED_FUNCTION, requiresOrder, requiresOver,
				requiresGroupOrder);
		this.function = function;
		this.typeFactory = typeFactory;
	}

	@Override public List<RelDataType> getParamTypes() {
		List<RelDataType> argTypes = new ArrayList<>();
		for (FunctionParameter o : function.getParameters()) {
			final RelDataType type = o.getType(typeFactory);
			argTypes.add(type);
		}
		return toSql(argTypes);
	}

	private List<RelDataType> toSql(List<RelDataType> types) {
		return Lists.transform(types, this::toSql);
	}

	private RelDataType toSql(RelDataType type) {
		if (type instanceof RelDataTypeFactoryImpl.JavaType
				&& ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass()
				== Object.class) {
			return typeFactory.createTypeWithNullability(
					typeFactory.createSqlType(SqlTypeName.ANY), true);
		}
		return JavaTypeFactoryImpl.toSql(typeFactory, type);
	}

	@Override
	@SuppressWarnings("deprecation")
	public List<RelDataType> getParameterTypes(
			final RelDataTypeFactory typeFactory) {
		return Lists.transform(function.getParameters(),
				parameter -> parameter.getType(typeFactory));
	}

	@Override
	@SuppressWarnings("deprecation")
	public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
		return function.getReturnType(typeFactory);
	}
}
