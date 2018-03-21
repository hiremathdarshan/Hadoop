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
 
public class taskA {
 
  public static class Map1 extends Mapper<Object, Text, Text, IntWritable>{
 
    private Text word = new Text();
 
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String str = value.toString();
	String array1[] = str.split(",");
	if(array1.length == 6)
	{
	String k = array1[1] + "->" + array1[4];
	context.write(new Text(k),new IntWritable(Integer.parseInt(array1[5])));
	}
    }
  }
 
  public static class Reduce1
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
 
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      double avg = 0;
      int temp=0;
      int count = 0;
      for (IntWritable val : values) {
        temp = val.get();
        sum += temp;
        count += 1;
      }
      avg = sum/count;
      result.set((int)avg);
      context.write(key, result);
    }
  }
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Task A");
    job.setJarByClass(taskA.class);
    job.setMapperClass(Map1.class);
    job.setCombinerClass(Reduce1.class);
    job.setReducerClass(Reduce1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(4);
    conf.setNumMapTaska(8);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}