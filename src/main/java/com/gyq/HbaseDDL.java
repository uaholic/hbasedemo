package com.gyq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

public class HbaseDDL {
	Configuration conf=null;
	Connection conn =null;
	@Before
	public void init() throws IOException {
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "gyq1:2181,gyq2:2181,gyq3:2181");
		conn = ConnectionFactory.createConnection(conf);
	}
	
	/**
	 * 创建表
	 * @throws IOException 
	 */
	@Test
	public void createTable() throws IOException {
		Admin admin = conn.getAdmin();
//		创建表描述对象
		HTableDescriptor desc=new HTableDescriptor(TableName.valueOf("user_info"));
//		创建列族描述对象
		HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("base_info");
		hColumnDescriptor1.setMaxVersions(3);//设置列族中最大存储的版本数  默认 1
		HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("extra_info");
//		添加列族
		desc.addFamily(hColumnDescriptor1);
		desc.addFamily(hColumnDescriptor2);
//		创建表
		admin.createTable(desc);
		admin.close();
		conn.close();
	}
	/**
	 * 删除表
	 * @throws IOException
	 */
	@Test
	public void dropTable() throws IOException {
//		创建admin对象
		Admin admin = conn.getAdmin();
//		停用表
		admin.disableTable(TableName.valueOf("user_info"));
//		删除表
		admin.deleteTable(TableName.valueOf("user_info"));
		admin.close();
		conn.close();
	}
	
	/**
	 * 修改表
	 * @throws IOException 
	 */
	@Test
	public void modifyTable() throws IOException {
		Admin admin = conn.getAdmin();
//		从已有表中获取表描述对象
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));
//		创建一个列族描述对象
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
//		为列族对象设置一个布隆过滤器
		hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL);
//		添加列族到表描述对象
		tableDescriptor.addFamily(hColumnDescriptor);
//		根据表描述对象修改表
		admin.modifyTable(TableName.valueOf("user_info"), tableDescriptor);
		admin.close();
		conn.close();
	}
}
