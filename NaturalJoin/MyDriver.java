import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MyDriver { 
	public static void main(String[] args) throws Exception {
		
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Natural Join");
    job.setJarByClass(MyDriver.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyMapper1.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MyMapper2.class);

    job.setReducerClass(JoinReduer.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }}
