package org.imsi.queryEREngine.imsi.er.Utilities;

import org.apache.commons.io.FileUtils;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class DumpDirectories implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 8378846365677820437L;
	private String dataDirPath;
	private String logsDirPath;
	private  String blockDirPath;
	private  String blockIndexDirPath;
	private  String groundTruthDirPath;
	private  String tableStatsDirPath;
	private  String blockIndexStatsDirPath;
	private  String linksDirPath;
	private String liFilePath;
	private  String qIdsPath;
	private String vetiPath;

	public DumpDirectories() {
		super();
	}

	public static DumpDirectories loadDirectories() {

		if(new File("dumpMap.json").exists()) {
			ObjectMapper objectMapper = new ObjectMapper();  
			try {
				return objectMapper.readValue(new File("dumpMap.json"),	DumpDirectories.class);

			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return new DumpDirectories("");
	}
	public DumpDirectories(String dumpPath){
		dataDirPath = dumpPath;
		logsDirPath = dumpPath + "/logs/";
		blockDirPath = dumpPath + "/blocks/";
		blockIndexDirPath = dumpPath + "/blockIndex/";
		groundTruthDirPath = dumpPath + "/groundTruth/";
		tableStatsDirPath = dumpPath + "/tableStats/tableStats/";
		blockIndexStatsDirPath = dumpPath + "/tableStats/blockIndexStats/";
		linksDirPath = dumpPath + "/links/";
		qIdsPath = dumpPath + "/qIds/";
		vetiPath = dumpPath + "/veti/";
		liFilePath = dumpPath + "/LI";
	}

	public  void generateDumpDirectories() throws IOException {
		File dataDir = new File(dataDirPath);
		File logsDir = new File(logsDirPath);
		File blockDir = new File(blockDirPath);
		File blockIndexDir = new File(blockIndexDirPath);
		File groundTruthDir = new File(groundTruthDirPath);
		File tableStatsDir = new File(tableStatsDirPath);
		File blockIndexStats = new File(blockIndexStatsDirPath);
		File linksDir = new File(linksDirPath);
		File vetiDir = new File(vetiPath);
		if(!dataDir.exists()) {
			FileUtils.forceMkdir(dataDir); //create directory
		}
		if(!logsDir.exists()) {
			FileUtils.forceMkdir(logsDir); //create directory
		}
		if(!blockIndexDir.exists()) {
			FileUtils.forceMkdir(blockIndexDir); //create directory
		}
		if(!groundTruthDir.exists()) {
			FileUtils.forceMkdir(groundTruthDir); //create directory
		}
		if(!tableStatsDir.exists()) {
			FileUtils.forceMkdir(tableStatsDir); //create directory
		}
		if(!blockIndexStats.exists()) {
			FileUtils.forceMkdir(blockIndexStats); //create directory
		}
		if(!linksDir.exists()) {
			FileUtils.forceMkdir(linksDir); //create directory
		}
		if(!vetiDir.exists()) {
			FileUtils.forceMkdir(vetiDir); //create directory
		}

	}

	public void storeDumpMap() throws IOException {
		File file = new File("dumpMap.json");
		FileOutputStream fOut = null;

		fOut = new FileOutputStream(file);

		ObjectMapper mapper = new ObjectMapper();

		JsonGenerator jGenerator = null;
		jGenerator = mapper.getFactory().createGenerator(fOut);
		mapper.writeValue(jGenerator, this);
		jGenerator.close();
	}
	
	public String getDataDirPath() {
		return dataDirPath;
	}

	public String getLogsDirPath() {
		return logsDirPath;
	}

	public String getBlockDirPath() {
		return blockDirPath;
	}

	public String getBlockIndexDirPath() {
		return blockIndexDirPath;
	}

	public String getGroundTruthDirPath() {
		return groundTruthDirPath;
	}

	public String getTableStatsDirPath() {
		return tableStatsDirPath;
	}

	public String getBlockIndexStatsDirPath() {
		return blockIndexStatsDirPath;
	}

	public String getLinksDirPath() {
		return linksDirPath;
	}
	public String getqIdsPath() {
		return qIdsPath;
	}

	public String getVetiPath() {
		return vetiPath;
	}
	
	public String getLiFilePath() {
		return liFilePath;
	}
	
}
