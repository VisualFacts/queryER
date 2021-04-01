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
package org.imsi.queryEREngine.imsi.calcite.adapter.csv;

import org.apache.calcite.linq4j.Enumerator;
import org.imsi.queryEREngine.apache.calcite.adapter.java.JavaTypeFactory;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataType;
import org.imsi.queryEREngine.apache.calcite.sql.type.SqlTypeName;
import org.imsi.queryEREngine.apache.calcite.util.Pair;
import org.imsi.queryEREngine.apache.calcite.util.Source;
import org.imsi.queryEREngine.imsi.er.KDebug;
import org.imsi.queryEREngine.imsi.er.Utilities.RandomAccessReader;

import com.univocity.parsers.common.processor.RowListProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/** Enumerator that reads from a CSV file.
 *
 * @param <E> Row type
 */
public class CsvEnumerator<E> implements Enumerator<E> {

	public static String getCallerClassName() {
		StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
		for (int i = 1; i < stElements.length; i++) {
			StackTraceElement ste = stElements[i];
			if (!ste.getClassName().equals(KDebug.class.getName()) && ste.getClassName().indexOf("java.lang.Thread") != 0)
				return ste.getClassName(); 
		} 
		return null;
	}

	private  CsvParser parser;
	private  AtomicBoolean cancelFlag;
	private int key = 0;
	
	private E current;
	private List<CsvFieldType> fieldTypes;
	public List<String> fieldNames;

	private Source source;

	public CsvEnumerator(Source source, AtomicBoolean cancelFlag,
			List<CsvFieldType> fieldTypes, int key) {
		this(source, cancelFlag, fieldTypes, identityList(fieldTypes.size()), key);
	}

	public CsvEnumerator(Source source, AtomicBoolean cancelFlag,
			List<CsvFieldType> fieldTypes, Integer[] fields, int key) {
		this.cancelFlag = cancelFlag;
		this.source = source;
		this.cancelFlag = cancelFlag;
		this.fieldTypes = fieldTypes;
		this.key = key;
		try {
			this.parser = openCsv(source);
			this.parser.parseNext(); // skip header row
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	/** Deduces the names and types of a table's columns by reading the first line
	 * of a CSV file. */
	static RelDataType deduceRowType(JavaTypeFactory typeFactory, Source source,
			List<CsvFieldType> fieldTypes) {
		final List<RelDataType> types = new ArrayList<>();
		final List<String> names = new ArrayList<>();
		try {
			CsvParser parser = openCsv(source);
			String[] strings = parser.parseNext();
			if (strings == null) {
				strings = new String[]{"EmptyFileHasNoColumns:boolean"};
			}
			for (String string : strings) {
				final String name;
				final CsvFieldType fieldType;
				final int colon = string.indexOf(':');
				if (colon >= 0) {

					name = string.substring(0, colon);
					String typeString = string.substring(colon + 1);
					fieldType = CsvFieldType.of(typeString);
					if (fieldType == null) {
						System.out.println("WARNING: Found unknown type: "
								+ typeString + " in file: " + source.path()
								+ " for column: " + name
								+ ". Will assume the type of column is string");
					}
				} else {
					name = string;
					fieldType = null;
				}
				final RelDataType type;
				if (fieldType == null) {
					type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
				} else {
					type = fieldType.toType(typeFactory);
				}
				names.add(name);
				types.add(type);
				if (fieldTypes != null) {
					fieldTypes.add(fieldType);
				}
			}
		} catch (IOException e) {
			// ignore
		}
		if (names.isEmpty()) {
			names.add("line");
			types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
		}
		return typeFactory.createStructType(Pair.zip(names, types));
	}

	public static CsvParser openCsv(Source source) throws IOException {
		// The settings object provides many configuration options
		CsvParserSettings parserSettings = new CsvParserSettings();
		//You can configure the parser to automatically detect what line separator sequence is in the input
		parserSettings.setNullValue("");
		parserSettings.setEmptyValue("");
		parserSettings.setDelimiterDetectionEnabled(true);
		CsvParser parser = new CsvParser(parserSettings);
		parser.beginParsing(new File(source.path()), Charset.forName("US-ASCII"));
		return parser;
	}


	@Override
	public E current() {
		return current;
	}

	@Override
	public boolean moveNext() {
		for (;;) {
			long rowOffset = parser.getContext().currentChar() - 1;
			final String[] strings = parser.parseNext();
			if (strings == null) {
				current = null;
				return false;
			}
			strings[0] = Long.toString(rowOffset);
			current = (E) strings;
			return true;

		}

	}

	@Override
	public void reset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {

	}

	/** Returns an array of integers {0, ..., n - 1}. */
	public static Integer[] identityList(int n) {
		Integer[] integers = new Integer[n];
		for (int i = 0; i < n; i++) {
			integers[i] = i;
		}
		return integers;
	}

	public AtomicBoolean getCancelFlag() {
		return this.cancelFlag;
	}


	public Source getSource() {
		return this.source;
	}

	public List<CsvFieldType> getFieldTypes() {

		return fieldTypes;
	}




}

// End CsvEnumerator.java
