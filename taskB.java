import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class taskB {
 
  public static class Map2 extends Mapper<Object, Text, Text, IntWritable>{
 
    private Text word = new Text();
 
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String str = value.toString();
	String array1[] = str.split(",");
	String year = array1[0];
	year = year.substring(0,4);
	int age;
	age = Integer.parseInt(array1[2]);
	str = "";
	str = str + Integer.toString(age/10) + "0 to " + Integer.toString(age/10) + "9";
	str = year + "->" + str;
	context.write(new Text(str),new IntWritable(1));
    }
  }
 
  public static class Reduce2
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
 
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Task A");
    job.setJarByClass(taskB.class);
    job.setMapperClass(Map2.class);
    job.setCombinerClass(Reduce2.class);
    job.setReducerClass(Reduce2.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}