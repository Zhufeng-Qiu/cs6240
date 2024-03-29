package frequent_items;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.*;

/**
 * The HBase class to load data from input file to HBase table
 */
public class H_Populate extends Configured implements Tool {
  private static final String DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
  private static int threshold = -1;

  /**
   * Helper function to parse Text writable array from byte array
   *
   * @param bytes the input byte array
   * @return the corresponding Text writable array
   * @throws IOException Check Read/Write issues
   */
  public static TextArrayWritable asWritable(byte[] bytes) throws IOException {
    TextArrayWritable result = new TextArrayWritable();
    DataInputStream dataIn = null;
    dataIn = new DataInputStream(new ByteArrayInputStream(bytes));
    result.readFields(dataIn);
    return result;
  }

  /**
   * Customized ArrayWritable class to store a writable array for Text type
   */
  public static class TextArrayWritable extends ArrayWritable implements WritableComparable<TextArrayWritable> {
    /**
     * Constructor to initialize an empty writable array for Text type
     */
    public TextArrayWritable() {
      super(Text.class);
    }

    /**
     * Constructor to initialize a writable array for Text type with the input array
     *
     * @param values the input Text array
     */
    public TextArrayWritable(Text[] values) {
      super(Text.class, values);
    }

    /**
     * Compare this object with given object
     *
     * @param that the object to be compared.
     * @return 0 if equal; positive if the current element of this object is larger than the current element of given object; negative in other cases
     */
    @Override
    public int compareTo(TextArrayWritable that) {
      Writable[] curArr = this.get();
      Writable[] thatArr = that.get();
      int flag = 0;
      Text curText = new Text();
      Text thatText = new Text();
      for (int i = 0; i < curArr.length; i++) {
        curText.set(curArr[i].toString());
        thatText.set(thatArr[i].toString());
        flag = curText.compareTo(thatText);
        if (flag != 0) break;
      }
      return flag;
    }
  }


  /**
   * Mapper class to read data row by row
   */
  public static class HPopulateMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text userId = new Text();
    Text businessId = new Text();

    /**
     * Map function to read user id and business id row by row
     *
     * @param key the actual offset of input file
     * @param value the content of current row
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     * @throws InterruptedException Check thread issues
     */
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] fields = value.toString().split(DELIMITER);
      if (fields.length == 2) {
        userId.set(Bytes.toBytes(fields[0].trim().replace("\"", "")));
        businessId.set(Bytes.toBytes(fields[1].trim().replace("\"", "")));
        context.write(userId, businessId);
      }
    }
  }

  /**
   * Reducer class to store user and business basket into the HBase table
   */
  public static class HPopulateReducer extends TableReducer<Text, Text, Text> {
    TextArrayWritable basket = new TextArrayWritable();

    /**
     * Reduce function to store user and business basket into the HBase table
     *
     * @param key the user id
     * @param values the list of business id
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     * @throws InterruptedException Check thread issues
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      byte[] row = Bytes.toBytes(String.valueOf(key));
      Put put = new Put(row);

      ArrayList<Text> list = new ArrayList<>();
      for (Text val : values) {
        list.add(new Text(val));
      }
      if (list.size() >= threshold) {
        Text[] arr = new Text[list.size()];
        basket.set(list.toArray(arr));
        put.addColumn(Bytes.toBytes("columns"), Bytes.toBytes("reviews"), WritableUtils.toByteArray(basket));
        context.write(null, put);
      }
    }
  }

  /**
   * The process function to set the context for loading data
   *
   * @param args { the threshold for valid user, the path of input file }
   * @return the status of program
   * @throws IOException Check Read/Write issues
   * @throws InterruptedException Check thread issues
   * @throws ClassNotFoundException Check no class issues
   */
  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = HBaseConfiguration.create();
//    String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
//    conf.addResource(new File(hbaseSite).toURI().toURL());
    String table = "preprocess_data";
    TableName tableName = TableName.valueOf(table);
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("columns");
    htd.addFamily(hcd);
    admin.createTable(htd);

    String thresholdStr = args[0];
    try
    {
      H_Populate.threshold = Integer.parseInt(thresholdStr);
    } catch (Exception e) {
      return 1;
    }

    Job job1 = new Job(conf, "HBase populate");
    job1.setJarByClass(HPopulateMapper.class);
    FileInputFormat.setInputPaths(job1, new Path(args[1]));
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setMapperClass(HPopulateMapper.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    TableMapReduceUtil.initTableReducerJob(table, HPopulateReducer.class, job1);
    job1.waitForCompletion(true);

    return job1.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * The main function to trigger the loading phase of program
   *
   * @param args the path of input file
   * @throws Exception throw when exception happens
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(), new H_Populate(), args);
    System.exit(exitCode);
  }
}
