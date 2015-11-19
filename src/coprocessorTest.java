import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import com.google.protobuf.ServiceException;

public class coprocessorTest {

	public static void main(String[] args) throws ServiceException, Throwable {

		// TODO Auto-generated method stub
		Configuration customConf = new Configuration();
		customConf.setStrings("hbase.zookeeper.quorum", "hadoop3"); // 提高RPC通信时长
		customConf.setLong("hbase.rpc.timeout", 600000); // 设置Scan缓存
		customConf.setLong("hbase.client.scanner.caching", 1000);

		Configuration conf = HBaseConfiguration.create(customConf);

		try {
			Table table = new HTable(conf, "nihao");

			final ExampleProtos.CountRequest request = ExampleProtos.CountRequest
					.getDefaultInstance();
			Map<byte[], Long> results = table.coprocessorService(
					ExampleProtos.RowCountService.class, null, null,
					new Batch.Call<ExampleProtos.RowCountService, Long>() {
						public Long call(ExampleProtos.RowCountService counter)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback = new BlockingRpcCallback<ExampleProtos.CountResponse>();
							counter.getRowCount(controller, request,
									rpcCallback);
							ExampleProtos.CountResponse response = rpcCallback
									.get();
							if (controller.failedOnException()) {
								throw controller.getFailedOn();
							}
							return (response != null && response.hasCount()) ? response
									.getCount() : 0;
						}
					});

			int occur = 0;
			int sum = 0;
			for (Entry<byte[], Long> ss : results.entrySet()) {
				sum += ss.getValue();
				occur++;
			}
			System.out.println("count=" + sum);
			System.out.println("region=" + occur);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
