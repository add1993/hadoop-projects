import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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

public class MutualFriends {
    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{
    	
        private Text word = new Text();
        private Text friendList = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitArr = value.toString().split("\t", -1);
            if (splitArr.length < 2 || splitArr[0].trim().isEmpty() || splitArr[1].trim().isEmpty()) {
            	return;
            }
        	word.clear();
            word.set(splitArr[0]);
            friendList.set(splitArr[1]);
            context.write(word, friendList); // create a pair <keyword, 1>
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {
    	
    	static HashMap<String, String> userDetails;

        public void setup(Context context) throws IOException{
			Configuration config = context.getConfiguration();
			userDetails = new HashMap<String, String>();
			//String userdataPath = config.get("userDataPath");
			Path path = new Path("hdfs://localhost:9000" +"/user/ayush/input/userdata.txt");
			FileSystem fileSystem = FileSystem.get(config);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
			String userDataInput;
			userDataInput = bufferedReader.readLine();
			while (userDataInput != null) {
				String[] tempArray = userDataInput.split(",");
				if (tempArray.length == 10) {
					String relevantData = tempArray[3] + ":" + tempArray[9];
					userDetails.put(tempArray[0].trim(), relevantData);
				}
				userDataInput = bufferedReader.readLine();
			}
        }
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	int total = 0;
        	int count = 0;
        	LocalDate today = LocalDate.now();
    		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    		String currDate = today.format(formatter);
    		String date1[] = currDate.split("/");
        	for (Text val : values) {
        		String str = val.toString();
        		String[] split = str.split(",");
        		for (int i = 0; i < split.length; i++) {
	        		String split2[] = userDetails.get(split[i]).split(":");
	        		String date2[] = split2[1].split("/");
	        		total += (Integer.parseInt(date1[2])-Integer.parseInt(date2[2]));
	        		count++;
        		}
        	}
        	
        	String currKey = userDetails.get(key.toString());
        	double average = (double)total / count;
        	context.write(new Text(Double.toString(average)), new Text(key.toString()+":"+currKey.split(":")[0]));
        }
    }

    public static class Map2
    	extends Mapper<LongWritable, Text, DoubleWritable, Text>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitArr = value.toString().split("\t", -1);
            if (splitArr.length < 2 || splitArr[0].trim().isEmpty() || splitArr[1].trim().isEmpty()) {
            	return;
            }
	        context.write(new DoubleWritable(Double.parseDouble(splitArr[0])), new Text(splitArr[1]));
		}
	}
    
    public static class Reduce2
    	extends Reducer<DoubleWritable, Text, Text, Text> {
    	private Text word = new Text();
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			for (Text val : values) {
				String splitArr[] = val.toString().split(":");
				context.write(new Text(splitArr[0]+","+splitArr[1]+","+key.toString()), new Text());
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
	        job1.setJarByClass(MutualFriends.class);
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
	        job2.setJarByClass(MutualFriends.class);
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
