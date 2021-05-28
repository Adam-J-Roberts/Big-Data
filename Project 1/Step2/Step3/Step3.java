package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.ArrayList;

//import com.sun.org.apache.xml.internal.utils.res;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step2{

  public static class MyMapper
        extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
                String[] line = value.toString().split("\t");
                //String[] line = value.toString().split(",");
                Integer user = Integer.parseInt(line[0]);
                Integer movie = Integer.parseInt(line[1]);
				context.write(new IntWritable(user), new IntWritable(movie));
            
		    }
    }


  public static class MyReducer
        extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
            private TreeMap<Integer, ArrayList<Integer>> treemap = new TreeMap<Integer, ArrayList<Integer>>();
            String str = "";
		    public void reduce (IntWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
                
                //Setting users watched list			        
                for(IntWritable x: value){
                    //Adding user's movie list to treemap
                   Integer movie = x.get();
                   if (treemap.containsKey(key.get())){
                    ArrayList<Integer> list2 = treemap.get(key.get());
                    list2.add(movie);
                    treemap.remove(key.get());
                    treemap.put(key.get(), list2);

                  }if (!treemap.containsKey(key.get())){
                    ArrayList<Integer> list1 =  new ArrayList<Integer>();
                    list1.add(movie);
                    treemap.put(new Integer(key.get()), list1);
                  }
                }
                //Create string of watched movies
                for(ArrayList<Integer>  x: treemap.values()){
                  str = Integer.toString(x.get(0));
                  for (int i = 1; i < x.size(); i++) 
                  {
                      str = str + "," + Integer.toString(x.get(i)) ;
                  }      
                }
                // Setting users friend list
                //for

                context.write(new IntWritable(key.get()), new Text(str));

                
		    }
    }

  public static void main(String[] args) throws Exception {
  		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Temp");
		job.setJarByClass(Step2.class);
		job.setMapperClass(MyMapper.class);
		//job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

