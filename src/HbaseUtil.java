import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil {

	private String rootDir;
	private String zkServer;
	private String port;

	private Configuration conf;
	private HConnection hConn = null;

	public HbaseUtil(String rootDir, String zkServer, String port) {
		this.rootDir = rootDir;
		this.zkServer = zkServer;
		this.port = port;

		conf = HBaseConfiguration.create();// ��ȡĬ��������Ϣ
		conf.set("hbase.rootdir", rootDir);
		conf.set("hbase.zookeeper.quorum", zkServer);
		conf.set("hbase.zookeeper.property.clientPort", port);

		try {
			hConn = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	public void createTable(String tableName, List<String> cols) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				throw new IOException("table exists!");
			} else {
				HTableDescriptor tabledesc = new HTableDescriptor(tableName);
				for (String col : cols) {
					HColumnDescriptor coldesc = new HColumnDescriptor(col);
					coldesc.setCompressionType(Algorithm.GZ);// ���õ�ѹ����ʽ
					coldesc.setDataBlockEncoding(DataBlockEncoding.DIFF);// ����DIFF�ı��뷽ʽ
					tabledesc.addFamily(coldesc);// ����д�
				}
				admin.createTable(tabledesc);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void saveData(String tableName, List<Put> puts) {
		try {
			HTableInterface table = hConn.getTable(tableName);
			table.put(puts);
			table.setAutoFlush(false);
			table.flushCommits();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Result getData(String tableName, String rowkey) {
		try {
			HTableInterface table = hConn.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowkey));
			return table.get(get);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	// ��˳�����
	public void printResult(Result rs) {
		// old API
		// String rowkey=Bytes.toString(rs.getRow()); //active rowkey
		// Cell[] cells=rs.rawCells();
		// for(Cell cell:cells){
		// String family=Bytes.toString(cell.getFamily());
		// String qualifier=Bytes.toString(cell.getQualifier());
		// String value=Bytes.toString(cell.getValue());
		// System.out.println("rowkey->" + rowkey + "\tfamily->"+family +
		// "\tqualifier->"+qualifier + "\tvalue->" + value);
		// }
		if (rs.isEmpty()) {
			System.out.println("result is empty!");
			return;
		}
		// new API and print Map of families to all versions of its qualifiers
		// and values.
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> temps = rs
				.getMap();
		String rowkey = Bytes.toString(rs.getRow()); // actain rowkey
		System.out.println("rowkey->" + rowkey);
		for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> temp : temps
				.entrySet()) {
			System.out.print("\tfamily->" + Bytes.toString(temp.getKey()));
			for (Entry<byte[], NavigableMap<Long, byte[]>> value : temp
					.getValue().entrySet()) {
				System.out.print("\tcol->" + Bytes.toString(value.getKey()));
				for (Entry<Long, byte[]> va : value.getValue().entrySet()) {
					System.out.print("\tvesion->" + va.getKey());
					System.out.print("\tvalue->"
							+ Bytes.toString(va.getValue()));
					System.out.println();
				}
			}
		}
	}

	public void hbaseScan(String tableName) {
		Scan scan = new Scan();
		scan.setCaching(1000);// ���û������ݵ�����
		try {

			HTableInterface table = hConn.getTable(tableName);

			ResultScanner result = table.getScanner(scan);
			for (Result res : result) {
				printResult(res);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// ��ԱȽ����Ĺ�����
	public void hbaseCompareFilter(String tableName, Filter filter) {
		Scan scan = new Scan();
		scan.setCaching(1000);// ���û������ݵ�����
		scan.setFilter(filter);
		try {

			HTableInterface table = hConn.getTable(tableName);

			ResultScanner result = table.getScanner(scan);
			for (Result res : result) {
				printResult(res);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// ���ר�õĹ�����
	public void hbaseDedicatedFilter(String tableName, PageFilter filter,
			Long recoldNum) {
		int pagecount = 0;
		byte[] lastrow = null;
		try {
			HTableInterface table = hConn.getTable(tableName);
			// ���з�ҳ����
			while (++pagecount > 0) {
				System.out.println("PageCount= " + pagecount);
				Scan scan = new Scan();
				scan.setFilter(filter);
				if (lastrow != null) {
					scan.setStartRow(lastrow);
				}
				ResultScanner scanner = table.getScanner(scan);
				int count = 0;
				for (Result res : scanner) {
					lastrow = res.getRow();
					if (++count > recoldNum) {
						break;
					}
					printResult(res);
				}
				if (count < recoldNum)
					break;
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	public static void main(String[] args) {
		String rootdir = "hdfs://hadoop3:8020/hbase";
		String zkServer = "hadoop3";
		String port = "2181";

		// ������
		HbaseUtil conn = new HbaseUtil(rootdir, zkServer, port);
		// ������
		// List<String> cols= new LinkedList<String>();
		// cols.add("basicinfo");
		// cols.add("moreinfor");
		//
		// conn.createTable("nihao", cols);

		// ��������
		// List<Put>puts=new LinkedList<Put>();
		// Put put1=new Put(Bytes.toBytes("Kom"));
		// put1.add(Bytes.toBytes("basicinfo"), Bytes.toBytes("age") ,
		// Bytes.toBytes("26"));
		// put1.add(Bytes.toBytes("moreinfor"), Bytes.toBytes("tell") ,
		// Bytes.toBytes("3333333333"));
		//
		// Put put2=new Put(Bytes.toBytes("Jim"));
		// put2.add(Bytes.toBytes("basicinfo"), Bytes.toBytes("age") ,
		// Bytes.toBytes("28"));
		// put2.add(Bytes.toBytes("moreinfor"), Bytes.toBytes("tell") ,
		// Bytes.toBytes("22222222222"));
		//
		// puts.add(put1);
		// puts.add(put2);
		// conn.saveData("nihao", puts);

		// ��ȡ����
		Result rs = conn.getData("nihao", "789");
		conn.printResult(rs);

		// ͨ��scan��ȫ��ɨ������
		// String tablename="nihao";
		// conn.hbaseScan(tablename);

		/*
		 * //��������ʹ�� //String filterstr="Tom"; //�й����������ڱȽϹ�������
		 * //ByteArrayComparable filterName= new
		 * BinaryComparator(Bytes.toBytes(filterstr));
		 * 
		 * //ͨ��������ʽ�ķ�ʽ������ ByteArrayComparable filterName= new
		 * RegexStringComparator("J\\w+");
		 * 
		 * RowFilter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,
		 * filterName);
		 */
		//
		// // ��ҳ������������ר�ù�������
		// PageFilter filter = new PageFilter(3);//
		// ָ��һҳ��ʾ������rowkey,�����ڷ�ҳ��ʱ������Ӧ�ö�ȡһ����¼������������ֻ��Ϊ���´��ܹ��ܺõĿ�ʼ��
		// conn.hbaseDedicatedFilter("nihao", filter,(long) 2);
	}
}
