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

public class FlaggingFlags{

  public static class MyMapper
        extends Mapper<LongWritable, Text, Text, Text> {
          public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              
            String[] lineValues = value.toString().split(",");
            //key:Name value: red bool, triangle bool
            context.write(new Text(lineValues[0]), new Text(Integer.parseInt(lineValues[10]) + "," + Integer.parseInt(lineValues[24])));
        }
  }


				public static class MyReducer
				extends Reducer<Text, Text, Text, Text> {
					public void reduce (Text Key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer red;
            Integer triangle;
            for(Text value:values){ //Go through every line in the file
              String [] linevalues = value.toString().split(","); 
              red = Integer.parseInt(linevalues[0]);
              triangle = Integer.parseInt(linevalues[1]);
              
              if(red == 1 && triangle == 1){
                //only need counties.. Ignoring value
                context.write(Key, new Text(""));
              }
            }	
            //context.write(Key, new IntWritable());	
					}
				}

  public static void main(String[] args) throws Exception {
  	Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Temp");
		job.setJarByClass(FlaggingFlags.class);
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
