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
//import sun.rmi.rmic.IndentingWriter;

public class LetterCount{
	//private static Integer count = 0;
	//private static String topLetter = "";
	  
	public static class MyMapper
			extends Mapper<LongWritable, Text, Text, IntWritable>{                  
				private final static IntWritable one = new IntWritable(1);
				private Text word = new Text();
				
				public void map(LongWritable key, Text value, Context context
								) throws IOException, InterruptedException{
										
							String[] linevalues = value.toString().split(",");
							//creates an array of input seperating by ','
							for(Integer i = 0; i<linevalues.length; ++i){	
								//passes each letter through as key:letter vallue:1
								context.write(new Text(linevalues[i]), one);
							}

				}	
			}
	public static class MyReducer
			extends Reducer<Text, IntWritable, Text, IntWritable>{
				private boolean flag = false;
				int print = 4;
				int count = 0;//move out here to prevent redeclaration.
				
				public void reduce(Text key, Iterable<IntWritable> values, Context context
									) throws IOException, InterruptedException{
								int sum = 0;
								//variable for adding keys
								for(IntWritable val : values){
									sum += val.get();
									//adds each keys vallue to sum
								}
								
								/* This statement prints all in descending order
								context.write(key, new IntWritable(sum));
								*/

								//This if statement only does top letter
								if (!flag){

									context.write(key, new IntWritable(sum));
									flag = true;
									//context.write(key,result);
								}

								/*This if statement only does top 4
								if(count<print){
								context.write(key, new IntWritable(sum));
								count++;
								//context.write(key,result);
								}*/
							
				}
	}	//context.write(key,values);	
			
	public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf, "Temp");
	  job.setJarByClass(LetterCount.class);
	  job.setMapperClass(MyMapper.class);
	  //job.setCombinerClass(MyReducer.class);
	  job.setReducerClass(MyReducer.class);
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(IntWritable.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(IntWritable.class);
	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}

