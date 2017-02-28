/**
 * Created by kunalkrishna on 2/24/17.
 */

//hadoop jar question3.jar question3.Question3 ./kunal/review.csv  ./kunal/business.csv .kunal/output3/

package question3;

import java.io.IOException;
import java.util.*;
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

public class Question3 {

    public static class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable>{

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


    //Reducer for map1 job

    public static class Reduce1 extends Reducer<Text,DoubleWritable,Text,Text> {
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
                redKey.set(key.toString()+"::");
                context.write(redKey,new Text(redValue.toString()));
            }
        }

    }

    //read business file
    public static class Map2 extends Mapper<LongWritable, Text, Text, TextPair>{

        private Text mapKey1 = new Text();  // type of output key
        private TextPair mapValue1 = new TextPair(); // type of output value
        private Text textkey=new Text();
        private Text textvalue=new Text();


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] s = value.toString().split("::");
            String bus_id= s[0];
            String address= s[1];
            String category=s[2].substring(s[2].indexOf('L'), s[2].indexOf(')')+1);

            mapKey1.set(bus_id);
            textkey.set("Address:::"+address+"::Category::"+category+"::"+"Rating::");
            textvalue.set("1");
            context.write(mapKey1, new TextPair(textkey,textvalue));
        }

    }

    public static class Map3 extends Mapper<LongWritable, Text, Text, TextPair>{

        private Text mapKey1 = new Text();  // type of output key
        private Text textkey=new Text();
        private Text textvalue=new Text();


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//	    	System.out.println(":::::::::::Testing:::::::::::::");
//	    	System.out.println(value);
//	    	System.out.println(":::::::::::Testing:::::::::::::");
            String[] s = value.toString().split("::");
            //set key value pair for map3 means bus id and rating  plus 0
            textkey.set(s[1]);
            textvalue.set("0");
            mapKey1.set(s[0]);
            context.write(mapKey1,new TextPair(textkey,textvalue));
        }

    }



    public static class Reduce extends Reducer<Text,TextPair,Text,Text> {
        private Text redKey = new Text();  // type of output key
        private Text redValue = new Text(); // type of output value

        public void reduce(Text key, Iterable<TextPair> values,Context context) throws IOException, InterruptedException {
            String dataA_str = "";
            String dataB_str = "";
            double sum=0;
            double count=0;
            boolean  flag= false;
            for (TextPair value : values) {
                if (value.second.toString().equals("0")) {
                    dataA_str = value.first.toString();
                    flag=true;
                }
//			if(value.second.toString().equals("1"))
                else{
                    dataB_str = value.first.toString();
                }
            }
            if(!flag){
                return;
            }

            String[] dataB = dataB_str.split("\t");
            String[] dataA = dataA_str.split("\t");
            String data = dataB_str +"\t" + dataA_str;
            context.write(key, new Text(data));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }

        Job job_one = new Job(conf, "Get top 10 highest rating");
        job_one.setJarByClass(Question3.class);
        job_one.setReducerClass(Reduce1.class);
        job_one.setMapperClass(Map1.class);
        job_one.setOutputKeyClass(Text.class);
        job_one.setOutputValueClass(Text.class);
        job_one.setMapOutputKeyClass(Text.class);
        job_one.setMapOutputValueClass(DoubleWritable.class);
        job_one.setSortComparatorClass(SortComparator.class);
//	 job_one.setMapOutputKeyClass(Text.class);
//	 job_one.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job_one, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job_one, new Path("1_reducer_output"));

        job_one.waitForCompletion(true);

        Job job_two = Job.getInstance(conf, "Get address and category of top 10");
        job_two.setJarByClass(Question3.class);
        job_two.setReducerClass(Reduce.class);
        job_two.setOutputKeyClass(Text.class);
        job_two.setOutputValueClass(Text.class);
        job_two.setMapOutputKeyClass(Text.class);
        job_two.setMapOutputValueClass(TextPair.class);
        job_two.setSortComparatorClass(SortComparator.class);

        MultipleInputs.addInputPath(job_two, new Path("1_reducer_output"),TextInputFormat.class, Map3.class);
        MultipleInputs.addInputPath(job_two, new Path(otherArgs[1]),TextInputFormat.class, Map2.class);


        FileOutputFormat.setOutputPath(job_two, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job_two.waitForCompletion(true) ? 0 : 1);
    }

}

