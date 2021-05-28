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

public class HeartAttack{

	private static String range = "";
	//private static IntWritable result = new IntWritable();

  public static class MyMapper
       extends Mapper<LongWritable, Text, Text, Text>{

		public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] linevalues = line.split(",");
			Integer age = 0;
			if(linevalues[0] != "?") age = Integer.parseInt(linevalues[0]);

			//create keys for output and seperate out,
			if(age < 10 ){
				range = "0-10";   
			}
			if(age > 9 && age < 20){
				range = "10 - 19";   
			}
			if(age > 19 && age < 30){
				range = "20-29";   
			}
			if(age > 29 && age < 40){
				range = "30-39";   
			}
			if(age > 39 && age < 50){
				range = "40-49";   
			}
			if(age > 49 && age < 60){
				range = "50-59";   
			}
			if(age > 59 && age < 70){
				range = "60-69";   
			}
			context.write(new Text(range), new Text(linevalues[1] + "," + linevalues[13]));
		}
  }


  public static class CardioReducer
       extends Reducer<Text, Text, Text, Text> {

    public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			Integer gender = 0;
			Integer maleCount = 0;
			Integer femaleCount = 0;
			Integer health = 0;

			for(Text value:values){ //Go through every line in the file
				String [] linevalues = value.toString().split(","); 
				gender = Integer.parseInt(linevalues[0]);
				health = Integer.parseInt(linevalues[1]);
				
				if(gender == 0){
					if(health == 1){
						femaleCount++;	
					}         
				}
				if(gender == 1){
					if(health == 1){
						maleCount++;	
					}         
				}

			}		
			//result.set(count);
			context.write(key, new Text("Male: " + maleCount + ", Female: " + femaleCount));
			
		}
 }

  public static void main(String[] args) throws Exception {
  		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Heart");
		job.setJarByClass(HeartAttack.class);
		job.setMapperClass(MyMapper.class);
		//job.setCombinerClass(MyReducer.class);
		job.setReducerClass(CardioReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
/*
 * 
 * 
 * go to folder - /usr/local/hadoop/share/hadoop/mapreduce/heartattack
 * 
$ export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
* 
* save HeartAttack.Java here
* 
* create TopMovieFinder dir in /usr/local/hadoop/share/hadoop/mapreduce/heartattack
* 
* javac -classpath ${HADOOP_CLASSPATH} -d HeartAttack/ HeartAttack.java
* 
* jar -cvf HeartAttack.jar -C HeartAttack/ .
* 
* cd (go to root dir)
* /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/heartattack/HeartAttack.jar org.apache.hadoop.ramapo.HeartAttack ~/input/patients.txt ~/HeartAttackoutput
*/
