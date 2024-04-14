package frequent_items;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * The trigger of frequent itemset algorithm
 */
public class FrequentItemSetGenerator_no_hbase {
  private static final Logger logger = LogManager.getLogger(Apriori.class);

  /**
   * The Mapper class to collect all the frequent itemsets
   */
  public static class SummaryMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text record = new Text();
    Text dummy = new Text("dummy");

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
      String valStr = value.toString();
      if (valStr.toString().contains("[") && !valStr.toString().contains("crc")) {
        record.set(valStr);
        context.write(dummy, record);
      }
    }
  }

  /**
   * The Reducer class to output the records
   */
  public static class SummaryReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    /**
     * The reduce function to output the records
     *
     * @param key the dummy key
     * @param values the content of record
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     * @throws InterruptedException Check thread issues
     */
    public void reduce(Text key, Iterable<Text> values,
        Context context
    ) throws IOException, InterruptedException {
      for (Text val : values) {
        context.write(val, null);
      }
    }
  }

  /**
   * The main function to trigger the HBase part and store the results into output file
   *
   * @param args { the threshold for valid user, the support for A-priori algorithm, the input file, the populate folder, the intermediate folder, the output folder, the flag to indicate whether the program runs locally }
   * @throws Exception throw when exception happens
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 7) {
      Populate.process(new String[] { args[0], args[2], args[3] });
      Apriori.process(new String[] { args[1], args[3], args[4], args[6] });
      boolean IS_LOCAL = args[6].equals("true") ? true : false;

      Configuration conf = HBaseConfiguration.create();
      Job jobFinal = new Job(conf, "Summary");
      jobFinal.setJarByClass(FrequentItemSetGenerator_no_hbase.class);
      jobFinal.setOutputKeyClass(Text.class);
      jobFinal.setOutputValueClass(Text.class);
      jobFinal.setReducerClass(SummaryReducer.class);
      jobFinal.setNumReduceTasks(1);
      FileOutputFormat.setOutputPath(jobFinal, new Path(args[5]));

      if (IS_LOCAL) {
        File passFolders = new File(args[4]);
        if (!passFolders.exists()) return;
        for (String pass : passFolders.list()) {
          if (pass.startsWith("pass")) {
            MultipleInputs.addInputPath(jobFinal, new Path(String.valueOf(passFolders), pass), TextInputFormat.class, SummaryMapper.class);
          }
        }
      } else {
        String BUCKET_NAME = args[4].split("/", 4)[2];
        String PREFIX = args[4].split("/", 4)[3];
        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_1).build();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(BUCKET_NAME).withPrefix(PREFIX);
        ObjectListing objects = s3Client.listObjects(listObjectsRequest);
        Set<String> passSet = new HashSet<>();
        for (S3ObjectSummary summary: objects.getObjectSummaries()) {
          String file = summary.getKey();
          String[] prefixes = file.split("/");
          if (prefixes.length > 2 && prefixes[prefixes.length - 2].matches(".*pass[0-9]+$")) {
            passSet.add(prefixes[prefixes.length - 2]);
          }
        }
        for (String pass : passSet) {
          String passFolder = "s3://" + BUCKET_NAME + "/" + PREFIX + "/" + pass;
          logger.info("pass lists: " + passFolder);
          MultipleInputs.addInputPath(jobFinal, new Path(passFolder), TextInputFormat.class, SummaryMapper.class);
        }
      }
      int exitCode = jobFinal.waitForCompletion(true) ? 0 : 1;
      System.exit(exitCode);
    }
  }
}
