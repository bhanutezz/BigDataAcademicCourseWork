import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class MatmulReducer1step1 extends Reducer<Text, Text, Text, Text> {
	        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	            String[] value;
	            
	           HashMap<Integer, Double> mat1= new HashMap<Integer,Double>();
	           HashMap<Integer, Double> mat2= new HashMap<Integer,Double>();
	            
	            
	            for (Text val : values) {
	                value = val.toString().split(",");
	                if (val.toString().startsWith("L")) {
	                	
	                	mat1.put(Integer.parseInt(value[1]),Double.parseDouble(value[2]));
	                    
	                } else {
	                	mat2.put(Integer.parseInt(value[1]),Double.parseDouble(value[2]));
	                    
	                }
	            }
	            double a_ij;
	            double b_jk;
	            Text outputValue = new Text();
	            for (int a : mat1.keySet()) {
	                a_ij = mat1.get(a);
	                for (int b : mat2.keySet()) {
	                    b_jk = mat2.get(b);;
	                    outputValue.set(a + "," + b + "," +(a_ij*b_jk));
	                    context.write(null, outputValue);
	                }
	            }
	        }
	    }
	 

