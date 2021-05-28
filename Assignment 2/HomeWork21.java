package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;

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

public class HomeWork21{

  public static class MyMapper
        extends Mapper<LongWritable, Text, IntWritable, Text>{
	        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{

						String[] linevalues = value.toString().split(", ");			
						String [] arr = value.toString().split(", ", 2);
						context.write(new IntWritable(Integer.parseInt(linevalues[0])), new Text(arr[1]));
		  	}
	}


  public static class MyReducer
        extends Reducer<IntWritable, Text, IntWritable, Text> {
            public void reduce (IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
							
							for(Text val: values){
								String test = val.toString();
								context.write(key, new Text(test));
							}
		    }
    }

  public static void main(String[] args) throws Exception {
  		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Temp");
		job.setJarByClass(HomeWork21.class);
		job.setMapperClass(MyMapper.class);
		//job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
/*
 * 
 * 
 * go to folder - /usr/local/hadoop/share/hadoop/mapreduce/sortingvalues
 * 
$ export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
* 
* save SortingValues.Java here
* 
* create sortingvalues dir in /usr/local/hadoop/share/hadoop/mapreduce/sortingvalues
* 
* javac -classpath ${HADOOP_CLASSPATH} -d sortingvalues/ SortingValues.java
* 
* jar -cvf SortingValues.jar -C SortingValues/ . 
* 
* cd (go to root dir)
* /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/sortingvalues/SortingValues.jar org.apache.hadoop.ramapo.SortingValues ~/input/sortingvalues.txt ~/SortingValuesoutput


Test 4/4
Sort/Search/Maximum
*/
