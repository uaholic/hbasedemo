package com.gyq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseDML {
	Configuration conf=null;
	Connection conn =null;
	@Before
	public void init() throws IOException {
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "gyq1:2181,gyq2:2181,gyq3:2181");
		conn = ConnectionFactory.createConnection(conf);
	}
	
	/**
	 * 增
	 * 改(put覆盖)
	 * @throws IOException 
	 */
	@Test
	public void put() throws IOException {
//		获取一个操作指定表的table对象 进行dml操作
		Table table = conn.getTable(TableName.valueOf("user_info"));
//		构造要插入的数据为一个put类型的对象
		Put put = new Put(Bytes.toBytes("001"));
		put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("张三"));
		put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
		put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"), Bytes.toBytes("北京"));
		
		Put put2 = new Put(Bytes.toBytes("002"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("李四"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("21"));
		put2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"), Bytes.toBytes("上海"));
		
		List<Put> puts=new ArrayList<>();
		puts.add(put);
		puts.add(put2);
//		插入表
		table.put(puts);
		table.close();
		conn.close();
		
	}
	
	/**
	 * 删
	 * @throws IOException 
	 */
	@Test
	public void delete() throws IOException {
		Table table = conn.getTable(TableName.valueOf("user_info"));
//		根据所要删除的行键穿件delete对象
		Delete delete1 = new Delete(Bytes.toBytes("001"));
		Delete delete2 = new Delete(Bytes.toBytes("002"));
//		设置具体需要删除的列
		delete2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"));
		List<Delete> deletes=new ArrayList<>();
		deletes.add(delete1);
		deletes.add(delete2);
		table.delete(deletes);
		table.close();
		conn.close();
	}
}
