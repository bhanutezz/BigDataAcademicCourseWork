import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper1 extends Mapper<LongWritable, Text, Text, Text>{
	Text keyout= new Text();
	Text valueout= new Text();
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
    	
    	String [] data= value.toString().split("\t");
    	keyout.set(data[0]);
    	valueout.set("L"+data[1]+"\t"+data[2]);
    	context.write(keyout,valueout);
    }
}
