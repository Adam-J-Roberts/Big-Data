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

public class HomeWork22{

  public static class MyMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
          public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              String[] lineValues = value.toString().split(", ");
              String range = "";
              // replace leading whitespace in the 14th index
              String income = lineValues[14];
              Integer output = 0; // 1 if over 50k
              Integer age = Integer.parseInt(lineValues[0]);

              // discern age
              if(age > 9 && age <= 19){
                  range = "16-19";
              }
              if(age > 19 && age <= 29){
                  range = "20-29";
              }
              if(age > 29 && age <= 39){
                  range = "30-39";
              }
              if(age > 39 && age <= 49){
                  range = "40-49";
              }
              if(age > 49 && age <= 59){
                  range = "50-59";
              }
              if(age > 59 && age <= 69){
                  range = "60-69";
              }
              if (age > 69 && age <= 79) {
                range = "70-79";
              }
              if (age > 79 && age <= 89) {
                range = "80-89";
              }
              if (age > 89 && age <= 99) {
                range = "90-99";
              }

              // if datapoint makes over 50k
              if (income.contains(">")) {
                output = 1;
              }

              context.write(new Text(range), new IntWritable(output));
        }
  }


				public static class MyReducer
				extends Reducer<Text, IntWritable, Text, IntWritable> {
					public void reduce (Text Key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
	
							Integer count = 0;
	
							for (IntWritable val : values)
							{
								count += val.get();
							}
							context.write(Key, new IntWritable(count));
					}
				}

  public static void main(String[] args) throws Exception {
  	Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Temp");
		job.setJarByClass(HomeWork22.class);
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