package com.bds18a;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class CRUDOpHbase {

	
	Configuration conf = HBaseConfiguration.create();
	
	public static void main(String[] args) throws Exception {
		CRUDOpHbase hCRUD = new CRUDOpHbase() ;
		String tableName = "Company" ;
		String[] columnFamilies = {"details"} ;
		hCRUD.createTable(tableName, columnFamilies);
		System.out.println("Running till here.....");
		//First Record Insertion
		String rowKey1 = "Comp01" ;
		System.out.println("Inserting Records for 1st company....:");
		hCRUD.insert(tableName, rowKey1, "details", "Name", "Infosys Ltd");
		hCRUD.insert(tableName, rowKey1, "details", "Address", "Bangalore, KA");
		hCRUD.insert(tableName, rowKey1, "details", "Ranking", "IND2");
		hCRUD.insert(tableName, rowKey1, "details", "TotalEmployees", "150000");
		
		//Second Record Insertion
		String rowKey2 = "Comp02" ;
		System.out.println("Inserting Records for 2nd company....:");
		hCRUD.insert(tableName, rowKey2, "details", "Name", "Tata Consultancy Services");
		hCRUD.insert(tableName, rowKey2, "details", "Address", "Mumbai, MH");
		hCRUD.insert(tableName, rowKey2, "details", "Ranking", "IND1");
		hCRUD.insert(tableName, rowKey2, "details", "TotalEmployees", "175000");
		
		System.out.println("Details for the 1st company are:");
		hCRUD.getRecord(tableName, rowKey1);
		
		System.out.println("Details for the 2nd company are:");
		hCRUD.getRecord(tableName, rowKey2);
		
		//Display All Records
		System.out.println("Details for all the companies:");
		hCRUD.display(tableName);
	}

	public void createTable (String tableName, String[] columnFamilies) 
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin (conf) ;
		HTableDescriptor tableDesc = new HTableDescriptor(Bytes.toBytes(tableName)) ;
		
		for (int i =0 ; i< columnFamilies.length ; i++) {
			tableDesc.addFamily(new HColumnDescriptor(columnFamilies[i]));
		}
		if (admin.tableExists(tableName)) {
			System.out.println("Table Already Exists !");
		}
		else {
			admin.createTable(tableDesc);
			System.out.println("Table :" + tableName + " Created Successfully.");
		}
	}
	
	public void insert(String tableName, String rowKey, String colFamily, String qualifier,String value)
		throws Exception {
		try {
			HTable table = new HTable(conf, tableName) ;
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(colFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value)) ;
			table.put(put);
			System.out.println("Table : " + tableName + " is Inserted with rowkey : " + rowKey );
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	public void getRecord (String tableName, String rowKey) 
		throws IOException {
		HTable table = new HTable(conf, tableName) ;
		Get get = new Get(Bytes.toBytes(rowKey));
		Result rslt = table.get(get) ;
		for (KeyValue kv : rslt.raw() ){
			System.out.print(new String(kv.getRow()) + "\t");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + "\t");
			System.out.println(new String(kv.getValue()) );
		}
		
	}
	
	public void display (String tableName) 
			throws IOException {
			HTable table = new HTable(conf, tableName) ;
			Scan scan = new Scan();
			ResultScanner rslt = table.getScanner(scan) ;
			for (Result rs : rslt){
				for (KeyValue kv : rs.raw() ){
					System.out.print(new String(kv.getRow()) + "\t");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + "\t");
					System.out.println(new String(kv.getValue()) );
				}
			}
			
		}
}
