package edu.gatech.cse6242;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import java.util.Random;


public class Task4 {
public static class Mapper1
       extends Mapper<Object, Text, Text,IntWritable>{

    private IntWritable count = new IntWritable(0);
    private Text source = new Text();
	private Text target = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        source.set(itr.nextToken());
		target.set(itr.nextToken());
	  	count.set(1);
        context.write(target, count);
		context.write(source, count);
      }
    }
  }
  
  public static class Mapper2
       extends Mapper<Object, Text, Text,IntWritable>{

    private IntWritable count = new IntWritable(0);
    private Text node_n= new Text();
	private Text degree_n = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        
		node_n.set(itr.nextToken());
		degree_n.set(itr.nextToken());
	  	count.set(1);
        context.write(degree_n, count);
		//context.write(source, count);
      }
    }
  }

 
  public static class Reducer1
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum=0;
      for (IntWritable val : values) {
	    //int k = val.get();
		//if(k>maxl)
        sum+=val.get();
      }
	  
      result.set(sum);
      context.write(key, result);
    }
  }
  
   public static class Reducer2
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum=0;
      for (IntWritable val : values) {
	    //int k = val.get();
		//if(k>maxl)
        sum+=val.get();
      }
      result.set(sum);
      context.write(key,result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	Configuration conf1 = new Configuration();
	//JobConf job1 = new JobConf(Task4.class);
    //job1.setJobName("Job1");
    Job job1 = Job.getInstance(conf, "Job1");
    Random rand = new Random();
	int min =0;
	int max=100000;
    int randomNum = rand.nextInt((max - min) + 1) + min;
    String temp="intermediate"+ randomNum;
    /* TODO: Needs to be implemented */
	job1.setJarByClass(Task4.class);
    job1.setMapperClass(Mapper1.class);
    job1.setCombinerClass(Reducer1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(temp));
	//JobClient.runJob(job1);
	job1.waitForCompletion(true);
	//JobConf job2 = new JobConf(Task4.class);
    //job2.setJobName("Job2");
	Job job2= Job.getInstance(conf1, "Job2");
	 
    job2.setJarByClass(Task4.class);
    job2.setMapperClass(Mapper2.class);
    job2.setCombinerClass(Reducer2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job2, new Path(temp));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
//	JobClient.run(job2);
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
