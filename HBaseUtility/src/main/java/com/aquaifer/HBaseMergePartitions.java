package com.aquaifer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size.Unit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseMergePartitions {
	public static final String ANSI_RESET = "\u001B[0m";
	public static final String ANSI_RED = "\u001B[31m";
	public static final String ANSI_CYAN = "\u001B[36m";

	public static void main(String[] args) throws IOException, InterruptedException {
		if (args == null || args.length != 2) {
			System.out.println(ANSI_RED);
			throw new IllegalArgumentException(
					"Utility expects two argument - <table name> <merge adjacent{true/false}>");
		}
		System.out.println(ANSI_RED);
		System.out.println("PLEASE NOTE THAT\n YOU MAY NOT SEE LESS REGION'S JUST AFTER COMPLETION OF THIS UTILITY, AS MERGE IS ASYNCHRONOUS OPERATION");
		System.out.println(" DO NOT PERFORM REGION MERGE OPERATIONS ON THE PHOENIX SLATED TABLES.THIS MIGHT AFFECT THE REGION BOUNDARIES AND PRODUCE INCORRECT QUERY RESULTS.");
		System.out.println(ANSI_RESET);
		System.out.print("Utility will start in 30 Seconds...");
		int j = 0;
		while(j < 15) {
			Thread.sleep(2000);
			System.out.print(".");
			j++;
		}
		System.out.println();
		
		// List of Tables
		TableName tableName = TableName.valueOf(args[0]);

		boolean doMerge = Boolean.valueOf(args[1]);
		
		if(!doMerge) {
			System.out.println(ANSI_RED);
			throw new UnsupportedOperationException("'FALSE' not supported. Only adjacent regions can be merged.");
		}
		System.out.print(ANSI_RESET);
		
		Configuration config = HBaseConfiguration.create();
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(config);

			Admin admin = connection.getAdmin();
			// All Regions of a Table
			List<RegionInfo> tableRegions = admin.getRegions(tableName);
			Map<String, Double> regionMetrics = getTableRegionMetrics(admin, tableName);
			
			System.out.println(ANSI_CYAN);
			System.out.println("Region Metrics before Merge");
			System.out.println(regionMetrics);

			System.out.println("Total Regions for Table " + tableName + " equals " + tableRegions.size());
			System.out.print(ANSI_RESET);

			while (tableRegions.size() > 0) {
				// Get the Region and remove it from List
				RegionInfo info1 = tableRegions.get(0);
				tableRegions.remove(info1);

				RegionInfo adjacentInfo1 = null;
				// Find adjacent region. If found break the loop and remove element from list
				for (int i = 0; i < tableRegions.size(); i++) {
					if (info1.isAdjacent(tableRegions.get(i))) {
						adjacentInfo1 = tableRegions.get(i);
						break;
					}
				}

				// if adjacent is found then check
				// if it is Degenerate, Split, offline, MetaRegion
				if (adjacentInfo1 != null) {
					tableRegions.remove(adjacentInfo1);
					if (checkRegionsbeforeMerge(info1, adjacentInfo1, regionMetrics)) {
						System.out.println(ANSI_CYAN);
						System.out.println("Merging  [" + info1.getRegionNameAsString() + "] and ["+ adjacentInfo1.getRegionNameAsString() + "]");
						System.out.print(ANSI_RESET);
						admin.mergeRegionsAsync(info1.getEncodedNameAsBytes(),adjacentInfo1.getEncodedNameAsBytes(), false);

					} else {
						System.out.println(ANSI_RED);
						System.err.println("Validation Fails. Skipping the merge for [" + info1.getRegionNameAsString()+ "] and [" + adjacentInfo1.getRegionNameAsString() + "]");
						System.out.print(ANSI_RESET);
					}
				}

			}
			
			List<RegionInfo> tableRegionsafterMerge = admin.getRegions(tableName);
			Map<String, Double> regionMetricsafterMerge = getTableRegionMetrics(admin, tableName);
			System.out.println(ANSI_CYAN);
			System.out.println("Region Metrics after Merge");
			System.out.println(regionMetricsafterMerge);
			System.out.println("Total Regions for Table after Merge " + tableName + " equals " + tableRegionsafterMerge.size());
			System.out.print(ANSI_RESET);
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

	private static boolean checkRegionsbeforeMerge(RegionInfo info1, RegionInfo info2,
			Map<String, Double> regionMetrics) {
		boolean result = true;
		if (info1.isDegenerate() || info2.isDegenerate()) {
			result = false;
		} else if (info1.isMetaRegion() || info2.isMetaRegion()) {
			result = false;
		} else if (info1.isSplit() || info2.isSplit()) {
			result = false;
		} else {
			String reg1 = info1.getRegionNameAsString();
			String reg2 = info2.getRegionNameAsString();
			double reg1Size = regionMetrics.getOrDefault(reg1, 0.0);
			double reg2Size = regionMetrics.getOrDefault(reg2, 0.0);
			double sum = reg1Size + reg2Size;
			System.out.println(ANSI_CYAN);
			System.out.println("Region [" + reg1 + "] has size [" + reg1Size + "], and Region [" + reg2 + "] has size ["
					+ reg2Size + "]. Sum equals [" + sum + "]");
			System.out.print(ANSI_RESET);
			// max size of store
			if (sum > 10.0) {
				result = false;
			}
		}

		return result;
	}

	private static Map<String, Double> getTableRegionMetrics(Admin admin, TableName tableName) throws IOException {
		Map<String, Double> outMap = new HashMap<String, Double>();
		Collection<ServerName> servers = admin.getRegionServers();
		Iterator<ServerName> itr = servers.iterator();
		while (itr.hasNext()) {
			ServerName sn = itr.next();
			List<RegionMetrics> metrics = admin.getRegionMetrics(sn, tableName);
			for (int i = 0; i < metrics.size(); i++) {
				RegionMetrics met = metrics.get(i);
				double sizeGB = met.getStoreFileSize().get(Unit.GIGABYTE);
				String encodedRegionName = met.getNameAsString();
				outMap.put(encodedRegionName, sizeGB);
			}
		}

		return outMap;
	}

}
