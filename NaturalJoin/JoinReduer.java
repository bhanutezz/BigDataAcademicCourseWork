import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReduer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Text left= null;
		Text right= null;
	                                               
        for (Text v : values) {
        	if(v.toString().startsWith("L")) left=new Text(v);
        	if(v.toString().startsWith("R")) right=new Text(v);
        	
        }
        if(left!=null && right != null)
        context.write(key, new Text(left.toString().substring(1)+"\t"+right.toString().substring(1)));
        
    }
}