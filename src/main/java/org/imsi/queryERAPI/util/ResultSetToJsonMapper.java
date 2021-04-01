package org.imsi.queryERAPI.util;// convenient JDBC result set to JSON array mapper

import java.math.BigDecimal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
//import org.springframework.jdbc.support.JdbcUtils;

public class ResultSetToJsonMapper
{
	
    public static List<ObjectNode> mapResultSet(ResultSet rs) throws SQLException
    {
    	ResultSetMetaData rsmd = rs.getMetaData();
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode jArray = mapper.createArrayNode();
        List<ObjectNode> jList = new ArrayList<ObjectNode>();
        ObjectNode jsonObject =null;
        int columnCount = rsmd.getColumnCount();
        while(rs.next())
        {
            jsonObject = mapper.createObjectNode();
            for (int index = 1; index <= columnCount; index++)
            {
                String column = rsmd.getColumnName(index);
                Object value = rs.getObject(column);
                if (value == null)
                {
                    jsonObject.put(column, "");
                } else if (value instanceof Integer) {
                    jsonObject.put(column, (Integer) value);
                } else if (value instanceof String) {
                    jsonObject.put(column, (String) value);
                } else if (value instanceof Boolean) {
                    jsonObject.put(column, (Boolean) value);
                } else if (value instanceof Date) {
                    jsonObject.put(column, ((Date) value).getTime());
                } else if (value instanceof Long) {
                    jsonObject.put(column, (Long) value);
                } else if (value instanceof Double) {
                    jsonObject.put(column, (Double) value);
                } else if (value instanceof Float) {
                    jsonObject.put(column, (Float) value);
                } else if (value instanceof BigDecimal) {
                    jsonObject.put(column, (BigDecimal) value);
                } else if (value instanceof Byte) {
                    jsonObject.put(column, (Byte) value);
                } else if (value instanceof byte[]) {
                    jsonObject.put(column, (byte[]) value);
                } else {
                    throw new IllegalArgumentException("Unmappable object type: " + value.getClass());
                }
            }
            jList.add(jsonObject);
//            jArray.add(jsonObject);
        };
        //System.out.println(jArray);
        return jList;
    }
    
    public static List<ObjectNode> mapCRS(ResultSet rs, int start, int end) throws SQLException
    {
    	ResultSetMetaData rsmd = rs.getMetaData();
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode jArray = mapper.createArrayNode();
        List<ObjectNode> jList = new ArrayList<ObjectNode>();
        ObjectNode jsonObject =null;
        int columnCount = rsmd.getColumnCount();
        rs.absolute(start);
        while(rs.next() && rs.getRow() < end)
        {
            jsonObject = mapper.createObjectNode();
            for (int index = 1; index <= columnCount; index++)
            {
                String column = rsmd.getColumnName(index);
                Object value = rs.getObject(column);
                if (value == null)
                {
                    jsonObject.put(column, "");
                } else if (value instanceof Integer) {
                    jsonObject.put(column, (Integer) value);
                } else if (value instanceof String) {
                    jsonObject.put(column, (String) value);
                } else if (value instanceof Boolean) {
                    jsonObject.put(column, (Boolean) value);
                } else if (value instanceof Date) {
                    jsonObject.put(column, ((Date) value).getTime());
                } else if (value instanceof Long) {
                    jsonObject.put(column, (Long) value);
                } else if (value instanceof Double) {
                    jsonObject.put(column, (Double) value);
                } else if (value instanceof Float) {
                    jsonObject.put(column, (Float) value);
                } else if (value instanceof BigDecimal) {
                    jsonObject.put(column, (BigDecimal) value);
                } else if (value instanceof Byte) {
                    jsonObject.put(column, (Byte) value);
                } else if (value instanceof byte[]) {
                    jsonObject.put(column, (byte[]) value);
                } else {
                    throw new IllegalArgumentException("Unmappable object type: " + value.getClass());
                }
            }
            jList.add(jsonObject);
//            jArray.add(jsonObject);
        };
        //System.out.println(jArray);
        return jList;
    }
}
