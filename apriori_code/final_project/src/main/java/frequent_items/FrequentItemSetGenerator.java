package frequent_items;

import static frequent_items.H_Populate.asWritable;

import frequent_items.H_Populate.TextArrayWritable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ToolRunner;

/**
 * The HBase-version trigger of frequent itemset algorithm
 */
public class FrequentItemSetGenerator {

  /**
   * Check if the input string is a valid path
   *
   * @param path the input string
   * @return true if matches; false, otherwise
   */
  public static boolean isValidPath(String path) {
    try {
      Paths.get(path);
    } catch (Exception ex) {
      return false;
    }
    return true;
  }

  /**
   * The main function to trigger the HBase part and store the results into output file
   *
   * @param args { the threshold for valid user, the support for A-priori algorithm, the input file, the ouput file }
   * @throws Exception throw when exception happens
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 4 && isValidPath(args[3])) {
      Configuration conf = HBaseConfiguration.create();
      String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
      conf.addResource(new File(hbaseSite).toURI().toURL());
      ToolRunner.run(HBaseConfiguration.create(), new H_Populate(), new String[]{ args[0], args[2] });
      int exitCode = ToolRunner.run(HBaseConfiguration.create(), new H_Apriori(), new String[]{ args[1] });
      BufferedWriter writer = new BufferedWriter(new FileWriter(args[3], false));
      Connection connection = ConnectionFactory.createConnection(conf);

      int k = 1;
      TableName tableNameK = TableName.valueOf("pass" + k);
      Table tableK = connection.getTable(tableNameK);
      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("columns"));

      int scanLen = 0;
      do {
        scanLen = 0;
        ResultScanner scanner = tableK.getScanner(scan);
        for (Result res : scanner) {
          StringBuilder sb = new StringBuilder();
          sb.append("(");
          scanLen++;
          byte[] rowBytes = res.getRow();
          TextArrayWritable itemsArray = asWritable(rowBytes);
          Writable[] items = itemsArray.get();
          String[] tmp = new String[items.length];
          for (int i = 0; i < items.length; i++) {
            Text itemText = (Text) items[i];
            tmp[i] = "\"" + itemText.toString() + "\"";
          }
          sb.append(String.join(",", tmp));
          sb.append(")    ");
          byte[] valueBytes = res.getValue(Bytes.toBytes("columns"), Bytes.toBytes("count"));
          sb.append(Bytes.toInt(valueBytes));
          writer.write(sb.toString());
          writer.newLine();
        }
        k++;
        tableNameK = TableName.valueOf("pass" + k);
        tableK = connection.getTable(tableNameK);
      } while(scanLen > 0);
      writer.close();
      System.exit(exitCode);
    }
  }
}
