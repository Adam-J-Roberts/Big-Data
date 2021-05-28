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

public class Step1{

  public static class MyMapper
        extends Mapper<LongWritable, Text, IntWritable, Text>{
	        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
							
							String[] line = value.toString().split(",");
							Integer user = Integer.parseInt(line[0]);
							Integer movie = Integer.parseInt(line[1]);
              Double rating = Double.parseDouble(line[2]);

							context.write(new IntWritable(user), new Text(Integer.toString(movie) + "," + Double.toString(rating)));
            
		    }
    }


  public static class MyReducer
        extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

						public void reduce (IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			        for(Text x: value){
								String[] line = x.toString().split(",");
								//Integer user = Integer.parseInt(key);
								Integer movie = Integer.parseInt(line[0]);
								Double rating = Double.parseDouble(line[1]);
							
								if (rating > 3){
									context.write(key, new IntWritable(movie));
								}
							}
                
			       // context.write(key, value);
                
		    }
    }

  public static void main(String[] args) throws Exception {
  		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Temp");
		job.setJarByClass(Step1.class);
		job.setMapperClass(MyMapper.class);
		//job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

