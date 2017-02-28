
/**
 * Created by kunalkrishna on 2/24/17.
 */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question4 {

    public static class BussinessMap extends Mapper<LongWritable, Text, Text, Text>{

        private Text mapKey = new Text();  // type of output key
        private Text mapValue = new Text(); // type of output value


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	String [] s = value.toString().split("::");
        	String address= s[1];
        	String Bus_id=s[0];

            mapKey.set(Bus_id);
            mapValue.set("::"+address);         
            if(address.contains("Stanford")){ 
            	context.write(mapKey, mapValue);
            }
            
        }

    }


    //Reducer for Business map job

    public static class BussinessReduce extends Reducer<Text,Text,Text,Text> {
   	 private Text redKey = new Text();  // type of output key
     private Text redValue = new Text(); // type of output value
 
   public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {      
	   for(Text value:values){
		   context.write(key,value);
		   break;
	   }
    }

    }

   // perform Map side join using distributed cache
   public static class DistributedCacheMap extends Mapper<LongWritable, Text, Text, Text>{

       private Text mapKey = new Text();  // type of output key
       private Text mapValue = new Text(); // type of output value

   	static String total_record = "";
   	static HashMap<String, String> map = new HashMap<String, String>();

   	@Override
   	protected void setup(Context context) throws IOException,
   			InterruptedException {

   		URI[] files = context.getCacheFiles();

   		if (files.length == 0) {
   			throw new FileNotFoundException("Distributed cache file not found");
   		}
   		FileSystem fs = FileSystem.get(context.getConfiguration());
   		FSDataInputStream in = fs.open(new Path(files[0]));
   		BufferedReader br = new BufferedReader(new InputStreamReader(in));
   		readCacheFile(br);
   	};

   	private void readCacheFile(BufferedReader br) throws IOException {
   		String line = br.readLine();
   		while (line != null) {
   			String[] fields = line.split("::");
   			map.put(fields[0].trim(), fields[1].trim());
   			line = br.readLine();
   		}
   	}
       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       	String [] s = value.toString().split("::");
       	String Bus_id=s[2];
       	String user_id=s[1];
       	String rating=s[3];
       	mapKey.set(user_id);
       	mapValue.set(rating);
       	  if(map.get(Bus_id)!=null){
       		context.write(mapKey, mapValue);
       	  }
          
       }

   }
   
   
   public static class DistributedCacheReducer extends Reducer<Text,Text,Text,Text> {
	 
	   public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {      
		   for(Text value:values){
			   context.write(key,value);
		   }
	    }

	    }
   

    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: Business <in> <out>");
            System.exit(2);
        }

        Job job_one = new Job(conf, "Get all business data in distributed cache");
        job_one.setJarByClass(Question4.class);
        job_one.setReducerClass(BussinessReduce.class);
        job_one.setMapperClass(BussinessMap.class);
        job_one.setOutputKeyClass(Text.class);
        job_one.setOutputValueClass(Text.class);
        job_one.setMapOutputKeyClass(Text.class);
        job_one.setMapOutputValueClass(Text.class);
  

        FileInputFormat.addInputPath(job_one, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job_one, new Path("Business_reducer_output"));

        job_one.waitForCompletion(true);
        
        // Below code for next job.....

        Job job_two = Job.getInstance(conf, "Get user id and thier rating who rated in standford");

		job_two.setJarByClass(Question4.class);
     
        job_two.setMapperClass(DistributedCacheMap.class);
		
		job_two.setNumReduceTasks(0);
		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(Text.class);

		job_two.addCacheFile(new URI("Business_reducer_output"+"/part-r-00000")); 
		
		FileInputFormat.addInputPath(job_two, new Path(otherArgs[1]));	
        FileOutputFormat.setOutputPath(job_two, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job_two.waitForCompletion(true) ? 0 : 1);
    }

}
 

