import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MutualFriendsCount {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{
    	
        private Text word = new Text();
        private Text friendList = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitArr = value.toString().split("\t", -1);
            if (splitArr.length < 2 || splitArr[0].trim().isEmpty() || splitArr[1].trim().isEmpty()) {
            	return;
            }
            String currentUser = splitArr[0].trim();
            String[] friendListArr = splitArr[1].trim().split(",");
            int[] friendListArrInt = Arrays.stream(friendListArr).mapToInt(Integer::parseInt).toArray();
            Arrays.sort(friendListArrInt);
            String sortedFriendList = Arrays.stream(friendListArrInt).mapToObj(String::valueOf).collect(Collectors.joining(","));
            friendList.set(sortedFriendList);
            String fkey = "";
            
            for (String friend : friendListArr) {
            	if (currentUser.isEmpty() || friend.isEmpty()) {
            		continue;
            	}
            	
            	if (Integer.parseInt(currentUser) < Integer.parseInt(friend)) {
            		fkey = currentUser + "," + friend;
            	} else {
            		fkey = friend + "," + currentUser;
            	}
            	word.clear();
                word.set(fkey); // set word as each input keyword
                context.write(word, friendList); // create a pair <keyword, 1>
            }
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String[] groupedList = new String[2];
        	String[][] friendList = new String[2][];
        	ArrayList<String> resultList = new ArrayList<>();
        	
        	int index = 0, i = 0, j = 0;
        	for (Text val : values) {
        		groupedList[index] = val.toString();
        		friendList[index] = groupedList[index].split(",");
        		index++;
        	}
        	
        	while (index == 2 && i < friendList[0].length && j < friendList[1].length) {
        		if (friendList[0][i].equals(friendList[1][j])) {
        			resultList.add(friendList[0][i]);
        			i++;
        			j++;
        		} else if (Integer.parseInt(friendList[0][i]) > Integer.parseInt(friendList[1][j])) {
        			j++;
        		} else {
        			i++;
        		}
        	}
        	
        	String result = "";
        	if (resultList.isEmpty()) {
        		result = "No mutual friends";
        	} else {
        		result = String.join(",", resultList);
        	}
        	
        	context.write(key, new Text(result));
        }
    }

    public static class Map2
    	extends Mapper<LongWritable, Text, IntWritable, Text>{

		private IntWritable frequency = new IntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Task2 Mapping");
			String[] splitArr = value.toString().split("\t", -1);
		    String[] mutualFriendListArr = splitArr[1].split(",");
		    int freq = mutualFriendListArr.length; 
		    frequency.set(freq); // set word as each input keyword
	        context.write(frequency, new Text(splitArr[0]));
		}
	}
    
    public static class Reduce2
    	extends Reducer<IntWritable, Text, IntWritable, Text> {
    	private Text word = new Text();
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
		        word.set(value);
		        context.write(key, word);
		    }
		}
	}
    
    public static class IntComparator extends WritableComparator {

    	  public IntComparator() {
    	    super(IntWritable.class);
    	  }

    	  @Override
    	  public int compare(byte[] b1, int s1, int l1, byte[] b2,
    	        int s2, int l2) {
    	    Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
    	    Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
    	    return v1.compareTo(v2) * (-1);
    	  }
    }

    public static void main(String[] args) throws Exception {
    	//int exitCode = ToolRunner.run(new WordCombined(), args);  
    	//System.exit(exitCode);
		 Configuration conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        // get all args
	        if (otherArgs.length != 3) {
	            System.err.println(otherArgs[0]);
	            System.err.println(otherArgs.length);
	            System.err.println("Usage: MutualFriendsCount <in> <out>");
	            System.exit(2);
	        }
	        
	        JobControl jobControl = new JobControl("jobChain");
	        // create a job with name "wordcount"
	        Job job1 = new Job(conf, "Mutual Friendlist");
	        job1.setJarByClass(MutualFriendsCount.class);
	        job1.setMapperClass(Map.class);
	        job1.setReducerClass(Reduce.class);

	        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

	        // set output key type
	        job1.setOutputKeyClass(Text.class);
	        // set output value type
	        job1.setOutputValueClass(Text.class);
	        //set the HDFS path of the input data
	        FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
	        // set the HDFS path for the output
	        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
	        //Wait till job completion
	        int code = job1.waitForCompletion(true) ? 0 : 1;
	        System.out.println("job1 finished "+code);
	        
	        ControlledJob controlledJob1 = new ControlledJob(conf);
	        controlledJob1.setJob(job1);
	        
	        Configuration conf2 = new Configuration();
	        Job job2 = new Job(conf, "MutualFriendsCount");
	        job2.setJarByClass(MutualFriendsCount.class);
	        job2.setMapperClass(Map2.class);
	        job2.setReducerClass(Reduce2.class);
	        job2.setSortComparatorClass(IntComparator.class);
	        job2.setOutputKeyClass(IntWritable.class);
	        job2.setOutputValueClass(Text.class);
	        FileInputFormat.setInputPaths(job2, new Path(otherArgs[2]));
	        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]+"/../mfcoutput"));
	        
	        ControlledJob controlledJob2 = new ControlledJob(conf2);
	        controlledJob2.setJob(job2);
	        
	        // make job2 dependent on job1
	        controlledJob2.addDependingJob(controlledJob1); 
	        // add the job to the job control
	        jobControl.addJob(controlledJob2);
	        code = job2.waitForCompletion(true) ? 0 : 1;
	 }
    
}
