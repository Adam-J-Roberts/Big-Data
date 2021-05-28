package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeMap;

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
                //st.split("\t");

                //String[] line = value.toString().split(",");
                Integer user = Integer.parseInt(line[0]);
                Integer movie = Integer.parseInt(line[1]);
				  context.write(new IntWritable(user), new IntWritable(movie));
            
		     }
        }


  public static class MyReducer
        extends Reducer<IntWritable, IntWritable, Text, Text> {
            private TreeMap<Integer, ArrayList<Integer>> treemap = new TreeMap<Integer, ArrayList<Integer>>();
            private TreeMap<Integer, ArrayList<String>> treemap2 = new TreeMap<Integer, ArrayList<String>>();
            String str = "";
            String str2 = "";
            String friend = "";
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

                ArrayList<Integer> currentArray;
                ArrayList<Integer> compareArray;

                //cycle current array "i"
                for(int i = 1; i < treemap.size(); i++){
                  currentArray = treemap.get(i);
                    //cycle compare array "k"
                      for(int k=1; k <= treemap.size(); k++){
                        if(i!=k){   //
                          compareArray = treemap.get(k);
                          //cylce array element in compare "l"
                          for(int l=0; l < compareArray.size(); l++){
                            if(currentArray.contains(compareArray.get(l))){
                              if (treemap2.containsKey(i)){
                                ArrayList<String> list3 = treemap2.get(i);
                                list3.add(k+":"+compareArray.get(l));
                                treemap2.remove(i);
                                treemap2.put(i, list3);
                              }
                              if (!treemap2.containsKey(i)){
                                ArrayList<String> list4 =  new ArrayList<String>();
                                list4.add(k+":"+compareArray.get(l));
                                treemap2.put(i, list4);
                              }
                            }
                          }
                        }
                      }
                }

                //Create string of friend
                for (Map.Entry<Integer, ArrayList<String>>entry : treemap2.entrySet()) {
                  str2 =  entry.getKey() + "" + entry.getValue();
                }
                //Create string of watched movies         
                for (Map.Entry<Integer, ArrayList<Integer>>entry : treemap.entrySet()) {
                  str =  entry.getKey() + "" + entry.getValue();
                }

                context.write(new Text(str), new Text(str2));

                
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
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

