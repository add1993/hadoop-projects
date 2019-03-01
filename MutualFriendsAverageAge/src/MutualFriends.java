import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
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
    	
        private Text word = new Text("");
        private Text friendList = new Text();
        static HashMap<String, String> userDetails;
        
        public void setup(Context context) throws IOException{
			Configuration config = context.getConfiguration();
			userDetails = new HashMap<String, String>();
			String userDataPath = config.get("userDataPath");
			Path path = new Path("hdfs://localhost:9000" +userDataPath);
			FileSystem fileSystem = FileSystem.get(config);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
			String userDataInput;
			userDataInput = bufferedReader.readLine();
			while (userDataInput != null) {
				String[] tempArray = userDataInput.split(",");
				if (tempArray.length == 10) {
					String relevantData = tempArray[9];
					userDetails.put(tempArray[0].trim(), relevantData);
				}
				userDataInput = bufferedReader.readLine();
			}
        }
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitArr = value.toString().split("\t", -1);
            if (splitArr.length < 2 || splitArr[0].trim().isEmpty() || splitArr[1].trim().isEmpty()) {
            	return;
            }
        	//word.clear();
            //word.set(splitArr[0]);
            //friendList.set();
            String []friendListArr = splitArr[1].split(",");
            double total = 0.0, average;
            LocalDate today = LocalDate.now();
    		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    		String currDate = today.format(formatter);
    		String date1[] = currDate.split("/");
    		
            for (int i = 0; i < friendListArr.length; i++) {
        		String split2 = userDetails.get(friendListArr[i]);
        		String date2[] = split2.split("/");
        		total += (Integer.parseInt(date1[2])-Integer.parseInt(date2[2]));
            }
            average = total / friendListArr.length;
            context.write(word, new Text(splitArr[0]+","+Double.toString(average)));
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {

    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		HashMap<String, Double> map = new HashMap<String, Double>();
			int count = 0;
			for (Text value : values) {
				String[] splitArr = value.toString().split(",");
				if (splitArr.length == 2) {
					map.put(splitArr[0], Double.parseDouble(splitArr[1]));
				}
		        //context.write(key, word);
		    }
			//ValueComparator bvc = new ValueComparator(map);
            //TreeMap<String, Integer> sorted_map = new TreeMap<String, Integer>(bvc);
            //sorted_map.putAll(map);
			HashMap<String, Double> sorted_map = sortByComparator(map, true);
            for (HashMap.Entry<String, Double> entry : sorted_map.entrySet()) {
                if (count < 15) {
                    context.write(new Text(entry.getKey()), new Text(Double.toString(entry.getValue())));
                } else
                    break;
                count++;
            }
    	}
    }
    
    private static HashMap<String, Double> sortByComparator(HashMap<String, Double> unsortMap, final boolean order) {

        List<Entry<String, Double>> list = new LinkedList<Entry<String, Double>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Double>>() {
            public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        HashMap<String, Double> sortedMap = new LinkedHashMap<String, Double>();
        for (Entry<String, Double> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    public static class Map2
    	extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//System.out.println("Mapper 2 called");
			String[] splitArr = value.toString().split("\t", -1);
            if (splitArr.length < 2 || splitArr[0].trim().isEmpty() || splitArr[1].trim().isEmpty()) {
            	return;
            }
            //System.out.println("Map 2 splitArr[0]="+splitArr[0]+" splitArr[1]="+splitArr[1]);
	        context.write(new Text(splitArr[0]), new Text(splitArr[1]));
		}
	}
    
    public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
    	static HashMap<String, String> userDetails;
        public void setup(Context context) throws IOException{
			Configuration config = context.getConfiguration();
			userDetails = new HashMap<String, String>();
			String userDataPath = config.get("userDataPath");
			Path path = new Path("hdfs://localhost:9000" +userDataPath);
			FileSystem fileSystem = FileSystem.get(config);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
			String userDataInput;
			userDataInput = bufferedReader.readLine();
			while (userDataInput != null) {
				String[] tempArray = userDataInput.split(",");
				if (tempArray.length == 10) {
					String relevantData = tempArray[3]+","+tempArray[4]+","+tempArray[5];
					userDetails.put(tempArray[0].trim(), relevantData);
				}
				userDataInput = bufferedReader.readLine();
			}
        }
        
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitArr = value.toString().split("\t", -1);
	        if (splitArr.length < 2 || splitArr[0].trim().isEmpty() || splitArr[1].trim().isEmpty()) {
	        	return;
	        }
	        //System.out.println("Map 3 splitArr[0]="+splitArr[0]+" splitArr[1]="+splitArr[1]);
	        context.write(new Text(splitArr[0]), new Text(userDetails.get(splitArr[0])+"\t"+splitArr[1]));
		}
	}
    
    public static class Reduce2
    	extends Reducer<Text, Text, Text, Text> {
    	private Text word = new Text();
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//System.out.println("Reducer");
			for (Text val : values) {
				System.out.println(val.toString());
				//String splitArr[] = val.toString().split(":");
				context.write(val, new Text());
			}
		}
	}

    public static void main(String[] args) throws Exception {
    	//int exitCode = ToolRunner.run(new WordCombined(), args);  
    	//System.exit(exitCode);
		 Configuration conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        // get all args
	        if (otherArgs.length != 4) {
	            System.err.println(otherArgs[0]);
	            System.err.println(otherArgs.length);
	            System.err.println("Usage: MutualFriendsAverage <in> <out>");
	            System.exit(2);
	        }
	        conf.set("userDataPath", otherArgs[3]);
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
	        
	        //ControlledJob controlledJob1 = new ControlledJob(conf);
	        //controlledJob1.setJob(job1);
	        
	        Configuration conf2 = new Configuration();
	        Job job2 = new Job(conf, "MutualFriendsAverage");
	        job2.setJarByClass(MutualFriends.class);
	        //job2.setMapperClass(Map2.class);
	        job2.setOutputKeyClass(Text.class);
	        job2.setOutputValueClass(Text.class);
	        MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), TextInputFormat.class, Map2.class);
	        MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), TextInputFormat.class, Map3.class);
	        job2.setReducerClass(Reduce2.class);
	        
	        //FileInputFormat.setInputPaths(job2, new Path(otherArgs[2]));
	        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]+"/../mfaverage"));
	        
	        //ControlledJob controlledJob2 = new ControlledJob(conf2);
	        //controlledJob2.setJob(job2);
	        
	        // make job2 dependent on job1
	        //controlledJob2.addDependingJob(controlledJob1); 
	        // add the job to the job control
	        //jobControl.addJob(controlledJob2);
	        code = job2.waitForCompletion(true) ? 0 : 1;
	 }
    
}
