

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

	//������Ϣ
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
		
		//ɾ����¼
//		deleteRecord(conf,"test_table2", "rowkey");
	}
	
    // Hbase���м�¼��Ϣ��ɾ��  
    @SuppressWarnings("deprecation")
	public static boolean deleteRecord(Configuration conf,String table, String key) throws IOException {  
        HTablePool tp = new HTablePool(conf, 1000);  
        Delete de = new Delete(key.getBytes());  
        try {  
        	tp.getTable(table).delete(de); 
        	System.out.println("ɾ����¼" + key + "����������"); 
            return true;  
        } catch (IOException e) {  
            System.out.println("ɾ����¼" + key + "�쳣������");  
            return false;  
        }finally{
        	tp.close();
        }  
    }  
	
	
    // ��ʾ�������ݣ�ͨ��HTable Scan���ȡ���б����Ϣ  
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
	
    // ���һ������
    @SuppressWarnings("deprecation")
	public static void addRow(String tableName, String row,
            String columnFamily, String column, String value) throws Exception {
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(row));// ָ����,Ҳ���Ǽ�ֵ
        // �����ֱ�:���塢�С�ֵ
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
		TableName name= TableName.valueOf(Bytes.toBytes(tablename));//����
		HTableDescriptor desc =new HTableDescriptor(name);
		
		HColumnDescriptor family=new HColumnDescriptor(Bytes.toBytes(colfamilyname));//�д�
		desc.addFamily(family);
		ha.createTable(desc);
		ha.close();
	}
	
	// Hbase��ȡ���еı���Ϣ  
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
