/*
 * Map Reduce - Matrix multiplication in One step (one Mapper and one Reducer)
 * */
package Matrix;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class MatrixOneStep {
    //Mapper<T,T,T,T> class
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	//map(key, value, context) method
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            //read matrix dimensions from Configuration
            int m = conf.getInt(conf.get("m"),-1);
            int p = conf.getInt(conf.get("p"),-1);
            //read matrix values from input file
            String line = value.toString();
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            //formulate key-value pairs
            if (indicesAndValue[0].equals("A")) {
                for (int k = 0; k < p; k++) {
                    outputKey.set(indicesAndValue[1] + "," + k);
                    outputValue.set("A," + indicesAndValue[2] + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            } else {
                for (int i = 0; i < m; i++) {
                    outputKey.set(i + "," + indicesAndValue[2]);
                    outputValue.set("B," + indicesAndValue[1] + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }
	//Reducer(T,T,T,T) class
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        //reduce(key, Iterable<value>, context) method
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           
        	String[] value;
            HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
            HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
            //fill Matrix A and B key-values in HashMap
            for (Text val : values) {
                
            	value = val.toString().split(",");
                if (value[0].equals("A")) {
                    hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                } else {
                    hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
            }
 		//print Matrix A and B   
		for (Float value1 : hashA.values()) {
            System.out.println("Matrix A = " + value1);
		}
		for (Float value1 : hashB.values()) {
            System.out.println("Matrix B = " + value1);
		}

			//read common dimension from Configuration
			Configuration conf = context.getConfiguration();
			int n = conf.getInt(conf.get("n"),-1);
            float result = 0.0f;
            float a_ij;
            float b_jk;
            //result matrix calculation C(mxp)=A(mxn)*B(nxp)
            for (int j = 0; j < n; j++) {
                a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
                System.out.println(a_ij);
                b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                System.out.println(b_jk);
                result += a_ij * b_jk;
            }
            //write result in output file
            if (result != 0.0f) {
                context.write(null, new Text(key.toString() + "," + Float.toString(result)));
            }
        }
    }
    //Driver method
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 5) {
            System.err.println("Argument usage: hadoop jar MatrixOneStep.jar inputpath outputpath m n p");
            System.exit(2);
        }
        //read matrix dimensions from arguments
        int dimM=Integer.parseInt(args[2]);
        int dimN=Integer.parseInt(args[3]);
        int dimP=Integer.parseInt(args[4]);
        if(dimM<1 || dimN<1 || dimP<1){
            System.out.println("Invalid Matrix Dimensions");
            System.exit(1);
        }
        //set matrix dimensions A(mxn); B(nxp).
        conf.setInt("m", dimM);
        conf.setInt("n", dimN);
        conf.setInt("p", dimP);

        //create MapReduce Job
        Job job = Job.getInstance(conf, "MatrixOneStep");
        //set Jar class
        job.setJarByClass(MatrixOneStep.class);
        //set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //set mapper and reducer classes
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        Path outputPath = new Path(args[1]);
        //set input and output format classes
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //set input and output paths to read input and to write output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //deleting the output path automatically from hdfs so that we don't have delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
        //execute the job and print the verbose
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
