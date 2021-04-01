package org.imsi.queryEREngine.imsi.er.Utilities;

import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 *
 * @author G.A.P. II
 */

public interface Constants {
	int SUBJECT = 1;
	int PREDICATE = 2;
	int OBJECT = 3;

	double MINIMUM_ATTRIBUTE_SIMILARITY_THRESHOLD = 1E-11;

	NumberFormat twoDigitsDouble = new DecimalFormat("#0.00");
	NumberFormat fourDigitsDouble = new DecimalFormat("#0.0000");

	String ATTRIBUTE_CLUSTER_PREFIX = "#$!cl";
	String ATTRIBUTE_CLUSTER_SUFFIX = "!$#";
	String BLANK_NODE_BEGINNING = "_:";
	String DEPTH_ONE_INFIX_DELIMITER = "+";
	String INFIX_DELIMITER = "++++";
	String INFIX_FIELD_TITLE = "Infix";
	String INFIX_REG_EX_DELIMITER = "\\+\\+\\+\\+";
	String LITERAL_BEGINNING = "\"";
	String URI_FIELD_TITLE = "URI";
	String URL_LABEL = "entityUrl";
	String VALUE_LABEL = "value";

	//for supervised meta-blocking
	double SAMPLE_SIZE = 0.05;
	int DUPLICATE = 1;
	int NON_DUPLICATE = 0;
	String MATCH = "Match";
	String NON_MATCH = "NonMatch";
}