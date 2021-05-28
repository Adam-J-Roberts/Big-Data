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

public class ColdestPlaceCleanup{

  public static class MyMapper
        extends Mapper<LongWritable, Text, Text, Text>{
	        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
						String[] line = value.toString().split(",");
            context.write(new Text(line[0]), new Text(value));
		    }
    }


  public static class MyReducer
        extends Reducer<Text, Text, Text, Text> {
					private int lowestTemp = 9999999;
					private String month = " ";
					private String location = " ";
          public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
						int localMax = 0;
						for(Text x: values){
							String[] line = x.toString().split(",");
							localMax = new Integer(Integer.parseInt(line[2]));
							if (localMax < lowestTemp){
								lowestTemp = localMax; 
								month = line[1];
								location = line[0];
							}
						}
						
					}					
					//context.write(key, new Text(maxTemp));
					protected void cleanup(Context context) throws IOException, InterruptedException{
						context.write(new Text(location), new Text(month + "," + Integer.toString(lowestTemp)));	
                          
					}
			}
  

  public static void main(String[] args) throws Exception {
  		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Temp");
		job.setJarByClass(ColdestPlaceCleanup.class);
		job.setMapperClass(MyMapper.class);
		//job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}