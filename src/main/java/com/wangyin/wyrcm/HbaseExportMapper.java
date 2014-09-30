package com.wangyin.wyrcm;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class HbaseExportMapper extends
		TableMapper<Text, Text> {

	public static void initJob(String table, Scan scan,
			Class<? extends TableMapper> mapper, Job job) throws IOException {
		TableMapReduceUtil.initTableMapperJob(table, scan, mapper,
				Text.class, Text.class, job);
	}

	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		Text k = new Text();
		k.set(key.get(), key.getOffset(), key.getLength());
		StringBuilder val_sb = new StringBuilder();
		int i = 1;
		for (KeyValue kv : value.list()) {
			String qualifier = new String(kv.getQualifier());
			if (qualifier.equalsIgnoreCase("isrisk")) {
				k.set(new String(kv.getValue()));
				continue;
			}
			val_sb.append(i).append(":").append(new String(kv.getValue())).append(" ");
			i++;
//			val_sb.append(new String(kv.getFamily()))
//			   .append("_")
//			   .append(new String(kv.getQualifier()))
//			   .append(":")
//               .append(new String(kv.getValue()))
//               .append(" ");
		}
		
		Text v = new Text(val_sb.deleteCharAt(val_sb.length()-1).toString());
		context.write(k, v);
	}
}