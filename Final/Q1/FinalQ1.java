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

public class FinalQ1{

  public static class MyMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
          public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              
            String[] lineValues = value.toString().split(",");
            if( lineValues[25].contains("?")){ lineValues[25] = "-1";}
            context.write(new Text(lineValues[2]), new IntWritable(Integer.parseInt(lineValues[25])));
        }
  }


	public static class MyReducer
		extends Reducer<Text, IntWritable, Text, IntWritable> {
            private IntWritable result = new IntWritable();
			public void reduce (Text Key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                
                int max = 0;
                for(IntWritable val : values){ 
                    max = Math.max(max, val.get());                   
                }
                result.set(max);
                context.write(Key, result);
            }	

		}


  public static void main(String[] args) throws Exception {
  	Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Temp");
		job.setJarByClass(FinalQ1.class);
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
