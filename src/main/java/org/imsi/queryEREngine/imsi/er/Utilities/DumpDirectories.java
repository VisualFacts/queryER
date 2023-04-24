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

public class DumpDirectories {
	/**
	 *
	 */
	private static  String dataDirPath;
	private static String logsDirPath;
	private static String blockDirPath;
	private static String blockIndexDirPath;
	private static String groundTruthDirPath;
	private static String tableStatsDirPath;
	private static String blockIndexStatsDirPath;
	private static String linksDirPath;
	private static String similaritiesDirPath;
	private static String liFilePath;
	private static String qIdsPath;
	private static String offsetsDirPath;
	private static File dataDir;
	private static File logsDir;
	private static File blockDir;
	private static File blockIndexDir;
	private static File groundTruthDir;
	private static File tableStatsDir;
	private static File blockIndexStats;
	private static File linksDir;
	private static File qIdsDir;
	private static File similaritiesDir;
	private static File offsetsDir;

	public DumpDirectories() {
		super();
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
		similaritiesDirPath = dumpPath + "/links/";
		qIdsPath = dumpPath + "/qIds/";
		liFilePath = dumpPath + "/LI/";
		offsetsDirPath = dumpPath + "/offsets/";
		dataDir = new File(dataDirPath);
		logsDir = new File(logsDirPath);
		blockDir = new File(blockDirPath);
		blockIndexDir = new File(blockIndexDirPath);
		groundTruthDir = new File(groundTruthDirPath);
		tableStatsDir = new File(tableStatsDirPath);
		blockIndexStats = new File(blockIndexStatsDirPath);
		linksDir = new File(linksDirPath);
		qIdsDir = new File(qIdsPath);
		similaritiesDir = new File(similaritiesDirPath);
		offsetsDir = new File(offsetsDirPath);
	}

	public  void generateDumpDirectories() throws IOException {

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
		if(!qIdsDir.exists()) {
			FileUtils.forceMkdir(qIdsDir); //create directory
		}
		if(!blockIndexStats.exists()) {
			FileUtils.forceMkdir(blockIndexStats); //create directory
		}
		if(!linksDir.exists()) {
			FileUtils.forceMkdir(linksDir); //create directory
		}
		if(!similaritiesDir.exists()) {
			FileUtils.forceMkdir(similaritiesDir); //create directory
		}
		if(!blockDir.exists()) {
			FileUtils.forceMkdir(blockDir); //create directory
		}
		if(!offsetsDir.exists()) {
			FileUtils.forceMkdir(offsetsDir); //create directory
		}
	}

	public String getDataDirPath() {
		return dataDirPath;
	}

	public String getLogsDirPath() {
		return logsDirPath + "logs" + String.valueOf(logsDir.list().length) + ".csv";
	}

	public String getNewLogsDirPath() {
		return logsDirPath + "logs" + String.valueOf(logsDir.list().length + 1) + ".csv";
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
		return qIdsPath + "qIds" + String.valueOf(qIdsDir.list().length + 1);
	}

	public String getOffsetsDirPath() {
		return offsetsDirPath;
	}

	public String getLiFilePath() {
		return liFilePath;
	}

}
