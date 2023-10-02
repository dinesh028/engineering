package com.aquaifer.mymr;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Bulk Delete Column Qualifiers.
 * 
 * It takes following arguments - 
 * 1) Table Name
 * 2) Column Family
 * 3) Start Time in milliseconds for Scan
 * 4) End Time in milliseconds for Scan
 * 5) Max Time in milliseconds, Such that data below the time should be deleted.
 * 
 * It is assumed that Column Qualifier Value is a JSON consisting "eventTimestamp" field which is in format like 2022-10-14T17:42:59Z. This is converted to epoch time in milliseconds and compared
 * with Max Time to mark for delete.
 * 
 *
 * @author dinesh sachdev
 *
 */
public class DeleteHBaseColumns {

	/*One may need to set -
	 * 	export HADOOP_CONF_DIR= consisting of Hadoop & HBase xml configurations.
	 *  export HADOOP_CLASSPATH = which may include ':' separated Jar files needed by MR program
	*/
	private Logger LOG = LoggerFactory.getLogger(getClass());

	public static int run(String[] arg0) throws Exception {
		String tableName = arg0[0];
		String columnQualifier = arg0[1];
		String scanMinTimeStr = arg0[2];
		String scanMaxTimeStr = arg0[3];
		String deleteDataMaxTimeStr = arg0[4];

		long min = Long.parseLong(scanMinTimeStr);
		long max = Long.parseLong(scanMaxTimeStr);

		Configuration configuration = HBaseConfiguration.create();
		
		configuration.set("delete.max", deleteDataMaxTimeStr);

		configuration.set("mapred.map.tasks.speculative.execution", "false");
		configuration.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = new Job(configuration, "Bulk Delete HBase Column Values");
		job.setJarByClass(DeleteHBaseColumns.class);

		Scan scan = new Scan();

		scan.setCacheBlocks(false);

		scan.setCaching(100);

		scan.setTimeRange(min, max);

		scan.addFamily(Bytes.toBytes(columnQualifier));

		/*
		 * if (arg0.length == 5) { System.out.println("Setting Scanner Limit"); int
		 * limit = Integer.parseInt(arg0[4]); scan.setLimit(limit); }
		 */

		TableMapReduceUtil.initTableMapperJob(tableName, scan, BulkDeleteColumnValueMapper.class,
				ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName, null, job);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * Take 5 arguments - Name of Table, ColumnFamily, Scan Minimum, & Scan Maximum
	 * Time-stamp, Max TimeStamp ( Delete data older then this time) And a 5th
	 * optional argument - Limit (ms) for delete
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try {
			run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

class BulkDeleteColumnValueMapper extends TableMapper<ImmutableBytesWritable, Put> {

	private Logger LOG = LoggerFactory.getLogger(getClass());

	long max = 0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		// String minTimeStr = conf.get("delete.min");
		String maxTimeStr = conf.get("delete.max");
		// min = Long.parseLong(minTimeStr);
		max = Long.parseLong(maxTimeStr);
		//LOG.info("Delete data older then maximum timestamp: " + max);
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		Put p = resultToPut(key, value);
		if (!p.isEmpty()) {
			context.write(key, p);
		}
	}

	private Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
		Put put = new Put(key.get());

		// cf, cq, timestamp,value
		Iterator<Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> itr = result.getMap().entrySet()
				.iterator();

		// Outer Loop ( For multiple Column families)
		while (itr.hasNext()) {
			Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry1 = itr.next();
			byte[] cf = entry1.getKey();
			Iterator<Entry<byte[], NavigableMap<Long, byte[]>>> itr1 = entry1.getValue().entrySet().iterator();
			// Inner Loop ( For multiple Columns under a family)
			while (itr1.hasNext()) {
				Entry<byte[], NavigableMap<Long, byte[]>> entry2 = itr1.next();
				byte[] cq = entry2.getKey();
				Iterator<Entry<Long, byte[]>> itr2 = entry2.getValue().entrySet().iterator();
				// Inner Loop ( For Multiple versions)
				while (itr2.hasNext()) {
					Entry<Long, byte[]> entry3 = itr2.next();
					long timestamp = entry3.getKey();
					byte[] value = entry3.getValue();

					KeyValue kv = new KeyValue(key.get(), cf, cq, timestamp, KeyValue.Type.DeleteColumn, value);

					// if (min != 0 && max != 0 && timestamp >= min && timestamp <= max) {
					// Mark this Cell (KeyValue) for delete
					// System.out.println("Mark for Delete: " + kv);
					// put.add(kv);
					// } else {

					JsonParser parser = new JsonParser();
					JsonObject obj = parser.parse(new String(value)).getAsJsonObject();
					JsonElement eventTimestampElem = obj.get("eventTimestamp");

					if (eventTimestampElem != null) {
						String eventTimestamp = eventTimestampElem.getAsString();
						long evenTimeMs = Instant.parse(eventTimestamp).toEpochMilli();

						// September 26, 2017 2:42:27 AM
						long lowerTimestamp = 1506393747000l;

						if (max != 0 && evenTimeMs >= lowerTimestamp && evenTimeMs <= max) {
							//LOG.info("EventTimestamp: " + evenTimeMs + " Mark for Delete: " + kv);
							put.add(kv);
						} else {
							//LOG.warn("WARN: Skipping as this timestamp is out of range. Key: " + kv);
						}
						// }
					} else {
						//LOG.warn("WARN: Did not find eventTimestamp in message: " + kv);
					}

				}
			}

		}

		return put;
	}
}
