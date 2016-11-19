import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
 
public class MatmulMapper1step11 extends Mapper<LongWritable, Text, Text, Text> {
    	

     
    	
    	Text keyout= new Text();
    	Text valueout= new Text();
    	
    	
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         	String [] data= value.toString().split(",");
        	keyout.set(data[1]);
        	valueout.set("L,"+data[0]+","+data[2]);
        	context.write(keyout,valueout);
                
        }
    
    	
        
    }
 
  
   