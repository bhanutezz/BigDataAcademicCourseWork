import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MatmulDriver1step1 {
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Matrix Multiplication");
		    job.setJarByClass(MatmulDriver1step1.class);
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatmulMapper1step11.class);
		    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MatmulMapper1step12.class);

		    job.setReducerClass(MatmulReducer1step1.class);
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  
		 
		 
		 
	         }
	}

