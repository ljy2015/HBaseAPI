

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseAPIDemo {
	
	private static Configuration conf =HBaseConfiguration.create();

	//配置信息
	static{
		conf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.27.233");
		conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
		
	}
	public static void main(String[] args) throws Exception {

		//create table
//		 String tablename="test_table2";
//		 String colfamilyname="test_fml";
		// createTable(conf,tablename,colfamilyname);
		//
		// //addrow
		// String colname="liujiyu2";
		// addRow(tablename,"rowkey",colfamilyname,colname,"123");
		
		//list-tables
		//getAllTables(conf);
		
		//gettableinfo
		getAllData(conf, "nihao") ;
		
		//删除记录
//		deleteRecord(conf,"test_table2", "rowkey");
	}
	
    // Hbase表中记录信息的删除  
    @SuppressWarnings("deprecation")
	public static boolean deleteRecord(Configuration conf,String table, String key) throws IOException {  
        HTablePool tp = new HTablePool(conf, 1000);  
        Delete de = new Delete(key.getBytes());  
        try {  
        	tp.getTable(table).delete(de); 
        	System.out.println("删除记录" + key + "正常！！！"); 
            return true;  
        } catch (IOException e) {  
            System.out.println("删除记录" + key + "异常！！！");  
            return false;  
        }finally{
        	tp.close();
        }  
    }  
	
	
    // 显示所有数据，通过HTable Scan类获取已有表的信息  
    public static void getAllData(Configuration conf,String tableName) throws Exception {  
        HTable table = new HTable(conf, tableName);  
        Scan scan = new Scan();  
        ResultScanner rs = table.getScanner(scan);  
        for (Result r : rs) {  
            for (KeyValue kv : r.raw()) {  
                System.out.println( Bytes.toString(kv.getKey())+"--->"
                        + Bytes.toString(kv.getValue()));  
            }  
        }  
    }  
	
    // 添加一条数据
    @SuppressWarnings("deprecation")
	public static void addRow(String tableName, String row,
            String columnFamily, String column, String value) throws Exception {
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(row));// 指定行,也就是键值
        // 参数分别:列族、列、值
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(value));
        table.put(put);
        table.close();
    }
	
	//create table
	private static void createTable(Configuration conf ,String tablename ,String colfamilyname)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		HBaseAdmin ha=new HBaseAdmin(conf);
		TableName name= TableName.valueOf(Bytes.toBytes(tablename));//表名
		HTableDescriptor desc =new HTableDescriptor(name);
		
		HColumnDescriptor family=new HColumnDescriptor(Bytes.toBytes(colfamilyname));//列簇
		desc.addFamily(family);
		ha.createTable(desc);
		ha.close();
	}
	
	// Hbase获取所有的表信息  
    public static List getAllTables(Configuration conf) throws MasterNotRunningException, ZooKeeperConnectionException, IOException { 
    	
		HBaseAdmin ad=new HBaseAdmin(conf);
        List<String> tables = null;  
        if (ad != null) {  
            try {  
                HTableDescriptor[] allTable = ad.listTables();  
                if (allTable.length > 0)  
                    tables = new ArrayList<String>();  
                for (HTableDescriptor hTableDescriptor : allTable) {  
                    tables.add(hTableDescriptor.getNameAsString());  
                    System.out.println(hTableDescriptor.getNameAsString());  
                }  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
        }  
        return tables;  
    }  
	
	
}
