package frequent_items;

import static frequent_items.H_Populate.asWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import frequent_items.H_Populate.TextArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase-version Class to calculate the frequent item sets
 */
public class H_Apriori extends Configured implements Tool {

  private static int SUPPORT = -1;
  private static int K = 1;

  /**
   * Generate the pair with size k from given Text array
   *
   * @param arr the input Text array
   * @param tmp the helper array to store current elements
   * @param start the start index
   * @param end the end index
   * @param index the current index
   * @param k the size k
   * @param result all the pairs with size k
   */
  public static void combinationK(Text[] arr, Text[] tmp, int start, int end, int index, int k, List<Text[]> result) {
    if (index == k) {
      result.add(Arrays.copyOf(tmp, tmp.length));
      return;
    }
    for (int i = start; i <= end && end - i + 1 >= k - index; i++) {
      tmp[index] = arr[i];
      combinationK(arr, tmp, i + 1, end, index + 1, k, result);
    }
  }

  /**
   * Mapper class for the first pass of A-priori algorithm
   */
  public static class Pass1Mapper extends TableMapper<TextArrayWritable, IntWritable> {
    IntWritable one = new IntWritable(1);

    /**
     * Map function to count business in HBase table
     *
     * @param row the row of HBase table
     * @param values the row from the scanned and filtered results
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     * @throws InterruptedException Check thread issues
     */
    @Override
    public void map(ImmutableBytesWritable row, Result values, Context context)
        throws IOException, InterruptedException {
      byte[] value = values.getValue(Bytes.toBytes("columns"), Bytes.toBytes("reviews"));
      TextArrayWritable businessArray = asWritable(value);
      Writable[] businesses = businessArray.get();
      for (Writable business : businesses) {
        context.write(new TextArrayWritable(new Text[] {(Text) business}), one);
      }
    }
  }

  /**
   * Mapper class for the latter pass of A-priori algorithm
   */
  public static class PassKMapper extends TableMapper<TextArrayWritable, IntWritable> {
    HashMap<TextArrayWritable, Integer> tmpCount;
    Set<Text> frequentItem;

    /**
     * Scan through the output from previous pass to get the frequent items
     *
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     */
    public void setup(Context context) throws IOException {
      tmpCount = new HashMap<>();
      frequentItem = new HashSet<>();
      Configuration conf = HBaseConfiguration.create();
      Connection connection = ConnectionFactory.createConnection(conf);
      TableName tableName = TableName.valueOf("pass" + (K - 1));
      Table table = connection.getTable(tableName);
      Scan scan = new Scan();
      scan.addColumn(Bytes.toBytes("columns"), Bytes.toBytes("count"));
      ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
        TextArrayWritable key = asWritable(result.getRow());
        Writable[] keys = key.get();
        for (Writable text : keys) {
          frequentItem.add((Text) text);
        }
      }
      scanner.close();
    }

    /**
     * Map function to count business pair in HBase table
     *
     * @param row the row of HBase table
     * @param values the row from the scanned and filtered results
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     */
    @Override
    public void map(ImmutableBytesWritable row, Result values, Context context)
        throws IOException {
      byte[] value = values.getValue(Bytes.toBytes("columns"), Bytes.toBytes("reviews"));
      TextArrayWritable businessArray = asWritable(value);
      Writable[] businesses = businessArray.get();
      Set<Text> newBasket = new HashSet<>();
      for (Writable businessWritable : businesses) {
        Text business = (Text) businessWritable;
        if (frequentItem.contains(business)) {
          newBasket.add(business);
        }
      }
      Text[] basketArr = new Text[newBasket.size()];
      basketArr = newBasket.toArray(basketArr);
      Arrays.sort(basketArr);
      Text[] tmp = new Text[K];
      List<Text[]> result = new ArrayList<>();
      combinationK(basketArr, tmp, 0, basketArr.length - 1, 0, K, result);
      for (Text[] res : result) {
        tmpCount.put(new TextArrayWritable(res), tmpCount.getOrDefault(new TextArrayWritable(res), 0) + 1);
      }
    }

    /**
     * Output the count for each business pair
     *
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     * @throws InterruptedException Check thread issues
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable count = new IntWritable();
      for (Map.Entry<TextArrayWritable, Integer> entry : tmpCount.entrySet()) {
        count.set(entry.getValue());
        context.write(entry.getKey(), count);
      }
    }
  }

  /**
   * Reducer class to calculate the total number of business pair
   */
  public static class PassReducer extends TableReducer<TextArrayWritable, IntWritable, TextArrayWritable> {

    /**
     * Reduce function to calculate the total number of business pair
     *
     * @param key the business pair
     * @param values the list of business pair's count for each mapper
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     * @throws InterruptedException Check thread issues
     */
    public void reduce(TextArrayWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Put put = new Put(WritableUtils.toByteArray(key));
      int count = 0;
      for (IntWritable val : values) {
        count += val.get();
      }
      if (count >= SUPPORT) {
        put.addColumn(Bytes.toBytes("columns"), Bytes.toBytes("count"), Bytes.toBytes(count));
        context.write(null, put);
      }
    }
  }

  /**
   * The process function to set the context for running the A-priori algorithm
   *
   * @param args { the support for A-priori algorithm }
   * @return the status of program
   * @throws IOException Check Read/Write issues
   * @throws InterruptedException Check thread issues
   * @throws ClassNotFoundException Check no class issues
   */
  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = HBaseConfiguration.create();
//    String hbaseSite = "/etc/hbaseSitese/conf/hbase-site.xml";
//    conf.addResource(new File(hbaseSite).toURI().toURL());
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    String supportStr = args[0];
    try
    {
      H_Apriori.SUPPORT = Integer.parseInt(supportStr);
    } catch (Exception e) {
      int k = 1;
      while (true) {
        String tableCheck = "pass" + k++;
        TableName tableNameCheck = TableName.valueOf(tableCheck);
        if (admin.tableExists(tableNameCheck)) {
          admin.disableTable(tableNameCheck);
          admin.deleteTable(tableNameCheck);
        } else {
          break;
        }
      }
      return 1;
    }
    boolean sysStatus = true;
    String table = "preprocess_data";
    TableName tableName = TableName.valueOf(table);
    if (admin.tableExists(tableName)) {
      // A-priori pass 1
      Job job2 = new Job(conf, "Hbase A-priori pass 1");
      job2.setJarByClass(H_Apriori.class);
      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("columns"));
      TableMapReduceUtil.initTableMapperJob("preprocess_data", scan, Pass1Mapper.class, TextArrayWritable.class, IntWritable.class, job2);
      TableName tableNamePass1 = TableName.valueOf("pass1");
      if (admin.tableExists(tableNamePass1)) {
        admin.disableTable(tableNamePass1);
        admin.deleteTable(tableNamePass1);
      }
      HTableDescriptor htd = new HTableDescriptor(tableNamePass1);
      HColumnDescriptor hcd = new HColumnDescriptor("columns");
      htd.addFamily(hcd);
      admin.createTable(htd);
      TableMapReduceUtil.initTableReducerJob("pass1", PassReducer.class, job2);
      sysStatus = job2.waitForCompletion(true);

      // A-priori pass k
      TableName tableNameK = TableName.valueOf("pass" + K);
      Table tableK = connection.getTable(tableNameK);
      ResultScanner scanner = tableK.getScanner(scan);
      int scanLen = 0;
      for (Result res : scanner) scanLen++;
      Job jobK;
      TableName tableNamePassK;
      HTableDescriptor htdK;
      HColumnDescriptor hcdK;
      while (scanLen > 0) {
        jobK = new Job(conf, "Hbase A-priori pass" + K);
        jobK.setJarByClass(H_Apriori.class);
        TableMapReduceUtil.initTableMapperJob("preprocess_data", scan, PassKMapper.class, TextArrayWritable.class, IntWritable.class, jobK);
        K++;
        tableNamePassK = TableName.valueOf("pass" + K);
        if (admin.tableExists(tableNamePassK)) {
          admin.disableTable(tableNamePassK);
          admin.deleteTable(tableNamePassK);
        }
        htdK = new HTableDescriptor(tableNamePassK);
        hcdK = new HColumnDescriptor("columns");
        htdK.addFamily(hcdK);
        admin.createTable(htdK);
        TableMapReduceUtil.initTableReducerJob("pass" + K, PassReducer.class, jobK);
        sysStatus = jobK.waitForCompletion(true);

        tableNameK = TableName.valueOf("pass" + K);
        tableK = connection.getTable(tableNameK);
        scanner = tableK.getScanner(scan);
        scanLen = 0;
        for (Result res : scanner) scanLen++;
      }
    }
    return sysStatus ? 0 : 1;
  }

  /**
   * The main function to trigger the A-priori phase of program
   *
   * @param args { the support for A-priori algorithm }
   * @throws Exception throw when exception happens
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(), new H_Apriori(), args);
    System.exit(exitCode);
  }
}
