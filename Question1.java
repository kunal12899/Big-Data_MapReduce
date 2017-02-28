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
    import org.apache.hadoop.util.GenericOptionsParser;
	        
	public class Question1 {
	    
	    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
	    	
	    	private Text mapKey = new Text();  // type of output key
	    	private Text mapValue = new Text(); // type of output value
	    		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	 String [] s = value.toString().split("::");
	    	 String address= s[1];
	    	 //String categories =s[2].trim().substring(4,s[2].length()-1);
	    	 String categories =s[2].substring(s[2].indexOf('(')+1, s[2].indexOf(')'));
	    	 
	    	 //String [] catArray = categories.split("\\(|,|\\)|,\\',',|\\'t'");
	    	 String [] catArray = categories.split(",");
	    if(address.contains("Palo Alto")){
	    	 for(String category:catArray){
	    		 mapKey.set(category.trim());
		    	 mapValue.set("Palo Alto");
		    	 context.write(mapKey, mapValue); 
	    	 } 
	    	
	    }	 
	    	   	    	
	     }
	   }
	 
	 public static class Reduce extends Reducer<Text,Text,Text,Text> {
		 private Text redKey = new Text();  // type of output key
	     private Text redValue = new Text(); // type of output value
	 
	   public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {      
		   redValue.set(" ");
		   context.write(key, redValue);
	 }
	 }
	        
	 public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		 // get all args
		 if (otherArgs.length != 2) {
		 System.err.println("Usage: WordCount <in> <out>");
		 System.exit(2);
		 }
		 // create a job with name "wordcount"
		 Job job = new Job(conf, "Category");
		 job.setJarByClass(Question1.class);
		 job.setMapperClass(Map.class);
		 job.setReducerClass(Reduce.class);
		 // OPTIONAL :: uncomment the following line to add the Combiner
		 // job.setCombinerClass(Reduce.class);
		 // set output key type
		 job.setOutputKeyClass(Text.class);
		 // set output value type
		 job.setOutputValueClass(Text.class);
		 //set the HDFS path of the input data
		 FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		 // set the HDFS path for the output
		 FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		 //Wait till job completion
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 }
	        
	}
