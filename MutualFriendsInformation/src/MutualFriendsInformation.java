import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

public class MutualFriendsInformation {
    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{
    	
        private Text word = new Text();
        private Text friendList = new Text();
        static HashMap<String, String> userDetails;

        public void setup(Context context) throws IOException{
			Configuration config = context.getConfiguration();
			userDetails = new HashMap<String, String>();
			String userDataPath = config.get("userDataPath");
			String user1 = config.get("user1");
			String user2 = config.get("user2");
			Path path = new Path("hdfs://localhost:9000" + userDataPath);
			FileSystem fileSystem = FileSystem.get(config);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
			String userDataInput;
			userDataInput = bufferedReader.readLine();
			while (userDataInput != null) {
				String[] tempArray = userDataInput.split(",");
				if (tempArray[0].contentEquals(user1) || tempArray[0].contentEquals(user2)) {
					if (tempArray.length == 10) {
						String relevantData = tempArray[1] + ":" + tempArray[4];
						userDetails.put(tempArray[0].trim(), relevantData);
					}
					userDataInput = bufferedReader.readLine();
				}
			}
        }
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitArr = value.toString().split("\t", -1);
            if (splitArr.length < 2 || splitArr[0].trim().isEmpty() || splitArr[1].trim().isEmpty()) {
            	return;
            }
            String currentUser = splitArr[0].trim();
            String[] friendListArr = splitArr[1].trim().split(",");
            int[] friendListArrInt = Arrays.stream(friendListArr).mapToInt(Integer::parseInt).toArray();
            Arrays.sort(friendListArrInt);
            //String sortedFriendList = Arrays.stream(friendListArrInt).mapToObj(String::valueOf).collect(Collectors.joining(","));
            Configuration config = context.getConfiguration();
			String user1 = config.get("user1");
			String user2 = config.get("user2");
            String fkey = "";
            String fList = "";
            for (int i = 0; i < friendListArrInt.length; i++) {
            	if (friendListArr[i].contentEquals(user1) || friendListArr[i].contentEquals(user2)) {
	            	int friend = friendListArrInt[i];
	            	fkey = Integer.toString(friend);
	            	fList +=  fkey+":"+userDetails.get(fkey);
	            	if (i != friendListArrInt.length-1) {
	            		fList += ",";
	            	}
            	}
            }
            friendList.set(fList);
            
            for (String friend : friendListArr) {
            	if (currentUser.isEmpty() || friend.isEmpty() || !friend.contentEquals(user1) || !friend.contentEquals(user2)) {
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
        	//ArrayList<String> resultList = new ArrayList<>();
        	String result = "[";
        	int index = 0, i = 0, j = 0, k = 0;
        	for (Text val : values) {
        		groupedList[index] = val.toString();
        		friendList[index] = groupedList[index].split(",");
        		index++;
        	}
        	
        	String si, sj;
        	while (index == 2 && i < friendList[0].length && j < friendList[1].length) {
        		si = friendList[0][i];
        		sj = friendList[1][j];
        		String []iarray = si.split(":");
        		String []jarray = sj.split(":");
        		if (iarray[0].equals(jarray[0])) {
        			if (k != 0) {
        				result += ",";
        			}
        			result += jarray[1]+":"+jarray[2];
        			//resultList.add(friendList[0][i]);
        			k++;
        			i++;
        			j++;
        		} else if (Integer.parseInt(iarray[0]) > Integer.parseInt(jarray[0])) {
        			j++;
        		} else {
        			i++;
        		}
        	}
        	
        	/*if (resultList.isEmpty()) {
        		result = "No mutual friends";
        	} else {
        		result = String.join(",", resultList);
        	}*/
        	result += "]";
        	
        	context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
    	//int exitCode = ToolRunner.run(new WordCombined(), args);  
    	//System.exit(exitCode);
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 6) {
		    System.err.println(otherArgs[0]);
		    System.err.println(otherArgs.length);
		    System.err.println("Usage: MutualFriendsInformation <in> <out> <userdetails file> <user1> <user2>");
		    System.exit(2);
		}
		conf.set("userDataPath", otherArgs[3]);
		conf.set("user1", otherArgs[4]);
		conf.set("user2", otherArgs[5]);
		JobControl jobControl = new JobControl("jobChain");
		// create a job with name "wordcount"
		Job job1 = new Job(conf, "Mutual Friendlist");
		job1.setJarByClass(MutualFriendsInformation.class);
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
	 }
    
}
