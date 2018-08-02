package com.gyq;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseDQL {
	Configuration conf=null;
	Connection conn =null;
	@Before
	public void init() throws IOException {
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "gyq1:2181,gyq2:2181,gyq3:2181");
		conn = ConnectionFactory.createConnection(conf);
	}
	
	/**
	 * 查询一行
	 * @throws IOException 
	 */
	@Test
	public void get() throws IOException {
		Table table = conn.getTable(TableName.valueOf("user_info"));
//		根据所要查询的行键创建get对象
		Get get = new Get(Bytes.toBytes("001"));
//		get.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
		Result result = table.get(get);
		//通过result直接取值
		/*byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
		System.out.println(new String(value));*/
//		遍历整行结果中所有的kv单元格
		CellScanner cellScanner = result.cellScanner();
		while(cellScanner.advance()) {
			Cell cell = cellScanner.current();
			byte[] rowArray = cell.getRowArray();//cell所属行键的字节数组
			byte[] familyArray = cell.getFamilyArray();//列族名的字节数组
			byte[] qualifierArray = cell.getQualifierArray();//列名的字节数组
			byte[] valueArray = cell.getValueArray();//value的字节数组
			//取出的字节数组中含有附加信息，需要通过内容部分的起始位置和内容长度 将字节数组转换
			System.out.println("行键："+new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
			System.out.println("列族名："+new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
			System.out.println("列名："+new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength()));
			System.out.println("value："+new String(valueArray,cell.getValueOffset(),cell.getValueLength()));
		}
		table.close();
		conn.close();
	}
	
	/**
	 * 按行键范围查询
	 * @throws IOException 
	 */
	@Test
	public void getList() throws IOException {
		Table table = conn.getTable(TableName.valueOf("user_info"));
//		根绝所要查询的数据范围创建scan对象
//		包含其实位置不包含结束位置，如果想要查询到结束位置行键对应的数据可以在行键后面拼接一个不可见字符(\000)
//		Scan scan = new Scan(Bytes.toBytes("001"), Bytes.toBytes("002"));
		Scan scan = new Scan(Bytes.toBytes("001"), Bytes.toBytes("002\000"));
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iter = scanner.iterator();
		while(iter.hasNext()) {
			Result result = iter.next();
			CellScanner cellScanner = result.cellScanner();
			while(cellScanner.advance()) {
				Cell cell = cellScanner.current();
				byte[] rowArray = cell.getRowArray();//cell所属行键的字节数组
				byte[] familyArray = cell.getFamilyArray();//列族名的字节数组
				byte[] qualifierArray = cell.getQualifierArray();//列名的字节数组
				byte[] valueArray = cell.getValueArray();//value的字节数组
				//取出的字节数组中含有附加信息，需要通过内容部分的起始位置和内容长度 将字节数组转换
				System.out.println("行键："+new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
				System.out.println("列族名："+new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
				System.out.println("列名："+new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength()));
				System.out.println("value："+new String(valueArray,cell.getValueOffset(),cell.getValueLength()));
			}
			System.out.println("----------------------------------");
		}
		table.close();
		conn.close();
	}
}
