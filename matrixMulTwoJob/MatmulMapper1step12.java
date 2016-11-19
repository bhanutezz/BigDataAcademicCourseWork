import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatmulMapper1step12  extends Mapper<LongWritable, Text, Text, Text> {
     
    	
    	Text keyout= new Text();
    	Text valueout= new Text();
    	
    	
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         	String [] data= value.toString().split(",");
        	keyout.set(data[0]);
        	valueout.set("R,"+data[1]+","+data[2]);
        	context.write(keyout,valueout);
                
        }
    }
