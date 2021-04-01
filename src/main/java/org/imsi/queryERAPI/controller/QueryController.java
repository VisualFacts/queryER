package org.imsi.queryERAPI.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.imsi.queryERAPI.util.PagedResult;
import org.imsi.queryERAPI.util.ResultSetToJsonMapper;
import org.imsi.queryEREngine.imsi.er.QueryEngine;
import org.imsi.queryEREngine.imsi.er.Utilities.DumpDirectories;
import org.imsi.queryEREngine.imsi.er.Utilities.SerializationUtilities;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import static org.springframework.http.ResponseEntity.ok;

@RestController()
@RequestMapping("/api")
@CrossOrigin
public class QueryController {

	ResultSet rs;
	CachedRowSet rowset;
	List<ObjectNode> results = null;
	DumpDirectories dumpDirectories = DumpDirectories.loadDirectories();
	String query = "";
//	@PostMapping("/query")
//	public ResponseEntity<String> query(@RequestParam(value = "q", required = true) String q,
//			@RequestParam(value = "page", required = false) int page, 
//			@RequestParam(value = "offset", required = false) int offset) throws JsonProcessingException, SQLException  {
//
//		return liResult(q, page, offset);
//		//return queryResult(q, page, offset);
//
//
//
//	}
	
	@PostMapping("/query")
	public ResponseEntity<String> query(@RequestParam(value = "q", required = true) String q) throws JsonProcessingException, SQLException  {

		return liResult(q);
		//return queryResult(q, page, offset);



	}
	public ResponseEntity<String> liResult(String q) throws SQLException, JsonProcessingException {

		ObjectMapper mapper = new ObjectMapper();
		QueryEngine qe = new QueryEngine();

		if(!this.query.contentEquals(q)) {
			rs = qe.runQuery(q);		
			if(rs != null) {
				HashMap<Integer, Set<Integer>> LI = (HashMap<Integer, Set<Integer>>) SerializationUtilities.loadSerializedObject(dumpDirectories.getLiFilePath());
				return ok(mapper.writeValueAsString(LI));
			}
			
		}

		
		return null;
	}
	
	public ResponseEntity<String> queryResult(String q, int page, int offset) throws SQLException, JsonProcessingException {
		page +=1;

		ObjectMapper mapper = new ObjectMapper();
		QueryEngine qe = new QueryEngine();

		if(!this.query.contentEquals(q)) {
			rs = qe.runQuery(q);		
			RowSetFactory factory = RowSetProvider.newFactory();
			rowset = factory.createCachedRowSet();			 
			rowset.populate(rs);
			this.query = q;
		}

		int end = rowset.size();
		int pages = (int) Math.floor(end / offset) + 1;
		
		int resultOffset = offset * page;
		int startOffset = resultOffset - offset;
		if(page == pages) {
			startOffset = offset * (page - 1);
			resultOffset = end;
			
		}
		if(resultOffset < offset || offset == -1) {
			startOffset = 1;
			resultOffset = end;
		}
		if(startOffset == 0) startOffset = 1;
		results = ResultSetToJsonMapper.mapCRS(rowset, startOffset, resultOffset);

		return ok(mapper.writeValueAsString(new PagedResult(pages, results, end)));
	}

}
