package com.test.mymr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 * Bulk Delete Column Qualifiers
 *
 * @author dinesh sachdev
 *
 */
public class DeleteHBaseColumns {

 public static int run(String[] arg0) throws Exception {
  Configuration configuration = HBaseConfiguration.create();
  configuration.set("mapred.map.tasks.speculative.execution", "false");
  configuration.set("mapred.reduce.tasks.speculative.execution", "false");
  HTable hbaseTable = new HTable(configuration, "NS:test_ds");
  Job job = new Job(configuration, "Bulk Delete HBase Column Values");
  job.setJarByClass(DeleteHBaseColumns.class);

  Scan scan = new Scan();

  scan.setCacheBlocks(false);

  scan.setCaching(1000);

  /**
   * Scanning rows for that column
   */
  scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("_0"));

  TableMapReduceUtil.initTableMapperJob("NS:test_ds", scan, BulkDeleteColumnValueMapper.class,
    ImmutableBytesWritable.class, Put.class, job);
  TableMapReduceUtil.initTableReducerJob("NS:test_ds", null, job);
  job.setNumReduceTasks(0);
  // HFileOutputFormat2.configureIncrementalLoad(job, hbaseTable);
  // String outputPath = "/tmp/bulkDeleteColumnValues";

  // FileOutputFormat.setOutputPath(job, new Path(outputPath));

  return job.waitForCompletion(true) ? 0 : 1;

 }

 public static void main(String[] args) {
  try {
   run(args);
  } catch (Exception e) {
   // TODO Auto-generated catch block
   e.printStackTrace();
  }
 }

}

class BulkDeleteColumnValueMapper extends TableMapper<ImmutableBytesWritable, Put> {

 @Override
 protected void map(ImmutableBytesWritable key, Result value,
   Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
   throws IOException, InterruptedException {
  context.write(key, resultToPut(key, value));
 }

 private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
  Put put = new Put(key.get());
  for (KeyValue keyValue : result.list()) {
   put.add(new KeyValue(key.get(),keyValue.getFamily(), keyValue.getQualifier(), HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn));
  }
  return put;
 }
 /*
  * @Override protected void map(ImmutableBytesWritable key, Result value,
  * Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable,
  * KeyValue>.Context context) throws IOException, InterruptedException { for
  * (KeyValue keyValue : value.list()) { context.write(key, new (key.get(),
  * keyValue.getFamily(), keyValue.getQualifier(), HConstants.LATEST_TIMESTAMP,
  * KeyValue.Type.DeleteColumn)); Put p = new Put(row) } }
  */
}