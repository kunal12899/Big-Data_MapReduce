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
	        
	public class Question2 {	    
	    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	    	
	    	private Text mapKey = new Text();  // type of output key
	    	private DoubleWritable mapValue = new DoubleWritable(); // type of output value
	    	
	    		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	 String [] s = value.toString().split("::");
	    	 String bus_id= s[2];
		    	
	    		 mapKey.set(bus_id);
		    	 mapValue.set(Double.valueOf(s[3]));
		    	 context.write(mapKey, mapValue); 
	    	 } 
	    	  	   	    	
	     }
	
	 
	 public static class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		 private Text redKey = new Text();  // type of output key
	     private DoubleWritable redValue = new DoubleWritable(); // type of output value
	     int output=0;
	     private HashMap<String, Double> countmap = new HashMap<>();
	     
	     
	     // sort the value based on the value	     
	     public static HashMap sortMapByValue(HashMap<String, Double> map){
	 		List<HashMap.Entry<String,Double>> list= new LinkedList(map.entrySet());
	 		
	 		Collections.sort(list,new Comparator<HashMap.Entry<String,Double>>(){
	 		@Override
			public int compare(HashMap.Entry<String,Double> o2, HashMap.Entry<String,Double> o1) {
				// TODO Auto-generated method stub
				return(o1.getValue()).compareTo(o2.getValue());
			}
 		});
		
	 		
	 		HashMap result = new LinkedHashMap<String ,Double>();
	 		for (HashMap.Entry<String,Double> entry : list) {
	 			result.put(entry.getKey(), entry.getValue());
	 		}
	 		 		
	 		return result;
	 		}
	 		
	 	     
	 
	   public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {  
		   
		   double sum=0;
		   double counter=0;
		   for(DoubleWritable val:values){
			   sum = sum + (double)val.get();
			   counter=counter+1;
		   }
		   redValue.set(sum/counter);
		   //context.write(key,redValue);
		   countmap.put(key.toString(), sum/counter);

	 }
	   
	   @Override
       protected void cleanup(Context context) throws IOException, InterruptedException {

           HashMap sortedMap = sortMapByValue(countmap);

           int counter = 0;
           for (Object key: sortedMap.keySet()) {
               if (counter ++ == 10) {
                   break;
               }
               redValue.set(Double.valueOf(sortedMap.get(key).toString()));
               redKey.set(key.toString());
               context.write(redKey,redValue);
           }
     }
	 
	 }
	        
	 public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		 // get all args
		 if (otherArgs.length != 2) {
		 System.err.println("Usage: Find top 10 <in> <out>");
		 System.exit(2);
		 }
		 // create a job with name "wordcount"
		 Job job = new Job(conf, "Category");
		 job.setJarByClass(Question2.class);
		 job.setMapperClass(Map.class);
		 job.setReducerClass(Reduce.class);
		 // OPTIONAL :: uncomment the following line to add the Combiner
		 // job.setCombinerClass(Reduce.class);
		 // set output key type
		 job.setOutputKeyClass(Text.class);
		 // set output value type
		 job.setOutputValueClass(DoubleWritable.class);
		 //set the HDFS path of the input data
		 FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		 // set the HDFS path for the output
		 FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		 //Wait till job completion
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 }
	        
	}

