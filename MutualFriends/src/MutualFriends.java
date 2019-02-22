import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{
    	
        private Text word = new Text(); // type of output key
        private Text friendList = new Text(); // type of output key

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitArr = value.toString().split("\t", -1);
            String currentUser = splitArr[0].trim();
            String[] friendListArr = splitArr[1].trim().split(",");
            Arrays.parallelSort(friendListArr);
            friendList.set(String.join(",", friendListArr));
            String fkey = "";
            
            for (String friend : friendListArr) {
            	if (currentUser.compareTo(friend) < 0) {
            		fkey = currentUser + "," + friend;
            	} else {
            		fkey = friend + "," + currentUser;
            	}
                word.set(fkey); // set word as each input keyword
                context.write(word, friendList); // create a pair <keyword, 1>
            }
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {

        //private IntWritable result = new IntWritable();
    	Text result;
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
        		if (friendList[0][i] == friendList[1][j]) {
        			resultList.add(friendList[0][i]);
        		} else if (friendList[0][i].compareTo(friendList[1][j]) > 0) {
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
        	
        	context.write(key, new Text(key +"     "+result));
        	/*int sum = 0; // initialize the sum for each keyword
            for (Text val : values) {
                //sum += val.get();
            }
            //result.set(sum);
            context.write(key, result); // create a pair <keyword, number of occurences>*/
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println(otherArgs[0]);
            System.err.println(otherArgs.length);
            System.err.println("Usage: MutualFriends <in> <out>");
            System.exit(2);
        }

        // create a job with name "wordcount"
        Job job = new Job(conf, "mutualfriends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
