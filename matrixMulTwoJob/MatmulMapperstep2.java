import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatmulMapperstep2 extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text keyout= new Text();
	IntWritable valueout= new IntWritable();
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
    	
    	String [] data= value.toString().split(",");
    	keyout.set(data[0]+","+data[1]);
    	valueout.set((int)Double.parseDouble(data[2]));
    	context.write(keyout,valueout);
    }
}
               
