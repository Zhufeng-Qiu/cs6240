package frequent_items;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The class to load data from input file to the populate folder
 */
public class Populate {
  private static final String DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
  private static int THRESHOLD = -1;

  /**
   * Mapper class to read data line by line
   */
  public static class PopulateMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text userId = new Text();
    Text businessId = new Text();

    /**
     * Map function to read user id and business id line by line
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
        userId.set(fields[0].trim().replace("\"", ""));
        businessId.set(fields[1].trim().replace("\"", ""));
        context.write(userId, businessId);
      }
    }
  }

  /**
   * Reducer class to store user and business basket into the populate folder
   */
  public static class PopulateReducer extends Reducer<Text, Text, Text, Text> {
    Text basket = new Text();

    /**
     * Reduce function to store user and business basket into the populate folder
     *
     * @param key the user id
     * @param values the list of business id
     * @param context the MapReduce context
     * @throws IOException Check Read/Write issues
     * @throws InterruptedException Check thread issues
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      ArrayList<Text> list = new ArrayList<>();
      for (Text val : values) {
        list.add(new Text(val));
      }
      Configuration conf = context.getConfiguration();
      THRESHOLD = conf.getInt("THRESHOLD", -1);
      if (list.size() >= THRESHOLD) {
        basket.set(list.toString());
        context.write(key, basket);
      }
    }
  }

  /**
   * The driver function to populate the origin data into the populate folder
   *
   * @param args { the threshold, the input file, the populate folder }
   * @return the running status
   * @throws IOException Check Read/Write issues
   * @throws InterruptedException Check thread issues
   * @throws ClassNotFoundException Check no class issues
   */
  public static int process(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    String thresholdStr = args[0];
    Job job1 = Job.getInstance(conf, "populate");
    job1.setJarByClass(Populate.class);
    Configuration job1Conf = job1.getConfiguration();
    job1Conf.setInt("THRESHOLD", Integer.parseInt(thresholdStr));
    job1.setMapperClass(PopulateMapper.class);
    job1.setReducerClass(PopulateReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(args[1]));
    FileOutputFormat.setOutputPath(job1, new Path(args[2]));
    boolean exitCode = job1.waitForCompletion(true);
    return exitCode ? 0 : 1;
  }

  /**
   * The main function to trigger the loading phase of program
   *
   * @param args { the threshold, the input file, the populate folder }
   * @throws Exception throw when exception happens
   */
  public static void main(String[] args) throws Exception {
    int exitCode = Populate.process(args);
    System.exit(exitCode);
  }
}
