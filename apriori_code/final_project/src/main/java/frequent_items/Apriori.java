package frequent_items;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * Class to calculate the frequent item sets
 */
public class Apriori {
  private static final Logger logger = LogManager.getLogger(Apriori.class);
  private static int SUPPORT = -1;
  private static int K = 1;
  private static String INTERMEDIATE_PATH;
  private static boolean IS_LOCAL;
  private static String BUCKET_NAME = "";
  private static String PREFIX = "";

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
  public static void combinationK(String[] arr, String[] tmp, int start, int end, int index, int k, List<String[]> result) {
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
   * Check whether there is any frequent itemset in previous pass
   *
   * @param path the folder path of previous pass's output
   * @return true if there is any frequent itemset in previous pass; false, otherwise
   * @throws IOException Check Read/Write issues
   */
  public static boolean checkFrequentItems(String path) throws IOException {
    File folder = new File(path);
    boolean flag = false;
    if (folder.exists()) {
      for (String file : folder.list()) {
        if (file.startsWith("part-r")) {
          File partFile = new File(path, file);
          BufferedReader reader = new BufferedReader(new FileReader(partFile.getAbsolutePath()));
          String line = reader.readLine();

          while (line != null) {
            flag = true;
            line = reader.readLine();
            if (flag) break;
          }
          reader.close();
        }
        if (flag) break;
      }
    }
    return flag;
  }

  /**
   * Mapper class for the first pass of A-priori algorithm
   */
  public static class Pass1Mapper extends Mapper<Object, Text, Text, IntWritable> {
    Text itemsText = new Text();
    IntWritable one = new IntWritable(1);

    /**
     * Map function to count business from the populate folder
     *
     * @param key the actual offset of input file
     * @param value the content of current row
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     * @throws InterruptedException Check thread issues
     */
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      if (value.toString().contains("[")) {
        String[] keyValuePair = value.toString().split("\\[", 2);
        String[] business_ids = keyValuePair[1].replace(" ", "").replace("[", "").replace("]", "").split(",");
        for (String business : business_ids) {
          ArrayList<String> items = new ArrayList<>();
          items.add(business);
          itemsText.set(items.toString());
          context.write(itemsText, one);
        }
      }
    }
  }

  /**
   * Mapper class for the latter pass of A-priori algorithm
   */
  public static class PassKMapper extends Mapper<Object, Text, Text, IntWritable> {
    Set<String> frequentItem;
    Text items = new Text();
    IntWritable one = new IntWritable(1);

    /**
     * Scan through the output from previous pass to get the frequent items
     *
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     */
    public void setup(Context context) throws IOException {
      logger.info("map setup start");
      Configuration conf = context.getConfiguration();
      frequentItem = new HashSet<>();
      FileSystem fs;
      IS_LOCAL = conf.getBoolean("IS_LOCAL", false);
      BUCKET_NAME = conf.get("BUCKET_NAME", "neu-cs6240-us-west-1");
      try {
        if (IS_LOCAL) {
          fs = FileSystem.get(new URI("."), conf);
        } else {
          fs = FileSystem.get(new URI("s3://" + BUCKET_NAME), conf);
        }
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
      for (URI file : context.getCacheFiles()) {
        if (file.getPath().contains("part-r")) {
          Path filePath = new Path(file);
          BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
          String line = reader.readLine();
          while (line != null) {
            String[] keyValuePair = line.toString().split("\\]", 2);
            String[] keys = keyValuePair[0].replace(" ", "").replace("[", "").replace("]", "").split(",");
            for (String key : keys) {
              frequentItem.add(key);
            }
            line = reader.readLine();
          }
          reader.close();
        }
      }
      logger.info("map setup finish");
    }

    /**
     * Map function to count business pair from the populate folder
     *
     * @param key the actual offset of input file
     * @param value the content of current row
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     */
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      K = conf.getInt("K", 1);
      if (value.toString().contains("[")) {
        String[] keyValuePair = value.toString().split("\\[", 2);
        String[] business_ids = keyValuePair[1].replace(" ", "").replace("[", "").replace("]", "").split(",");
        Set<String> newBasket = new HashSet<>();
        for (String business_id : business_ids) {
          if (frequentItem.contains(business_id)) {
            newBasket.add(business_id);
          }
        }
        String[] basketArr = new String[newBasket.size()];
        basketArr = newBasket.toArray(basketArr);
        Arrays.sort(basketArr);
        String[] tmp = new String[K];
        List<String[]> result = new ArrayList<>();
        combinationK(basketArr, tmp, 0, basketArr.length - 1, 0, K, result);
        for (String[] res : result) {
          String tmpRes = Arrays.toString(res);
          items.set(tmpRes);
          context.write(items, one);
        }
      }
    }

    /**
     * Clean up the template data structure
     *
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     * @throws InterruptedException Check thread issues
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
      frequentItem = new HashSet<>();
    }
  }

  /**
   * Reducer class to calculate the total number of business pair
   */
  public static class PassReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable countWritable = new IntWritable();

    /**
     * Reduce function to calculate the total number of business pair
     *
     * @param key the business pair
     * @param values the list of business pair's count for each mapper
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     * @throws InterruptedException Check thread issues
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      SUPPORT = conf.getInt("SUPPORT", -1);
      int count = 0;
      for (IntWritable val : values) {
        count += val.get();
      }
      if (count >= SUPPORT) {
        countWritable.set(count);
        context.write(key, countWritable);
      }
    }
  }

  /**
   * The driver function to run the A-priori algorithm
   *
   * @param args { the support, the populate folder, the intermediate folder,  the flag to indicate whether the program runs locally }
   * @return the running status
   * @throws IOException Check Read/Write issues
   * @throws InterruptedException Check thread issues
   * @throws ClassNotFoundException Check no class issues
   * @throws URISyntaxException Check URI format issues
   */
  public static int process(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
    Configuration conf = new Configuration();
    String supportStr = args[0];
    String loadFolder = args[1];
    Apriori.INTERMEDIATE_PATH = args[2];
    Apriori.IS_LOCAL = args[3].equals("true") ? true : false;
    Apriori.SUPPORT = Integer.parseInt(supportStr);
    boolean sysStatus = true;

    // A-priori pass 1
    Job job2 = Job.getInstance(conf, "A-priori pass 1");
    Configuration job2Conf = job2.getConfiguration();
    job2Conf.setInt("SUPPORT", SUPPORT);
    job2.setJarByClass(Apriori.class);
    job2.setMapperClass(Pass1Mapper.class);
    job2.setReducerClass(PassReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path(loadFolder));
    FileOutputFormat.setOutputPath(job2, new Path(INTERMEDIATE_PATH, "pass1"));
    sysStatus = job2.waitForCompletion(true);

    // A-priori pass k
    File previousPass;
    Job jobK;
    boolean iterationFlag = true;
    while (iterationFlag) {
      K++;
      jobK = new Job(conf, "A-priori pass" + K);
      if (IS_LOCAL) {
        previousPass = new File(INTERMEDIATE_PATH, "pass" + (K - 1));
        for (String file : previousPass.list()) {
          if (file.contains("part-r")) {
            jobK.addCacheFile(new URI(previousPass + "/" + file));
          }
        }
      } else {
        BUCKET_NAME = INTERMEDIATE_PATH.split("/", 4)[2];
        PREFIX = INTERMEDIATE_PATH.split("/", 4)[3] + "/pass" +(K - 1);
        logger.info("BUCKET_NAME: " + BUCKET_NAME);
        logger.info("PREFIX: " + PREFIX);
        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_1).build();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(BUCKET_NAME).withPrefix(PREFIX);
        ObjectListing objects = s3Client.listObjects(listObjectsRequest);
        for (S3ObjectSummary summary: objects.getObjectSummaries()) {
          String file = summary.getKey();
          if (file.contains("part-r")) {
            logger.info("file lists: " + file);
            if (file.contains("part-r")) {
              jobK.addCacheFile(new URI("s3://" + BUCKET_NAME + "/" + file));
            }
          }
        }
      }
      jobK.setJarByClass(Apriori.class);
      Configuration jobKConf = jobK.getConfiguration();
      jobKConf.setBoolean("IS_LOCAL", IS_LOCAL);
      jobKConf.set("BUCKET_NAME", BUCKET_NAME);
      jobKConf.setInt("K", K);
      jobKConf.setInt("SUPPORT", SUPPORT);
      jobK.setMapperClass(PassKMapper.class);
      jobK.setReducerClass(PassReducer.class);
      jobK.setOutputKeyClass(Text.class);
      jobK.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(jobK, new Path(loadFolder));
      logger.info("iteration output: " + K);
      FileOutputFormat.setOutputPath(jobK, new Path(INTERMEDIATE_PATH, "pass" + K));
      sysStatus = jobK.waitForCompletion(true);
      logger.info("iteration finish: " + K);

      if (IS_LOCAL) {
        previousPass = new File(INTERMEDIATE_PATH, "pass" + K);
        iterationFlag = checkFrequentItems(previousPass.getAbsolutePath());
      } else {
        BUCKET_NAME = INTERMEDIATE_PATH.split("/", 4)[2];
        PREFIX = INTERMEDIATE_PATH.split("/", 4)[3] + "/pass" + K;
        FileSystem fs = FileSystem.get(new URI("s3://" + BUCKET_NAME), conf);
        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_1).build();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(BUCKET_NAME).withPrefix(PREFIX);
        ObjectListing objects = s3Client.listObjects(listObjectsRequest);
        iterationFlag = false;
        for (S3ObjectSummary summary: objects.getObjectSummaries()) {
          String file = summary.getKey();
          if (file.contains("part-r")) {
            Path partFile = new Path("s3://" + BUCKET_NAME + "/" + file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(partFile)));
            String line = reader.readLine();

            while (line != null) {
              iterationFlag = true;
              line = reader.readLine();
              break;
            }
            reader.close();
          }
        }
      }
    }
    return sysStatus ? 0 : 1;
  }

  /**
   * The main function to trigger the A-priori phase of program
   *
   * @param args { the support, the populate folder, the intermediate folder,  the flag to indicate whether the program runs locally }
   * @throws Exception throw when exception happens
   */
  public static void main(String[] args) throws Exception {
    int exitCode = Apriori.process(args);
    System.exit(exitCode);
  }
}
