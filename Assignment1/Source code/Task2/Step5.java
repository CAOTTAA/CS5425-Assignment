package task2;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//

public class Step5 {
	public static class Step5_FilterSortMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag;
        @Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
        	 FileSplit split = (FileSplit) context.getInputSplit();
             flag = split.getPath().getParent().getName();// dataset
		}

		@Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(values.toString());
            Text k = new Text(tokens[0]);
            Text v = new Text();
            if(flag.equalsIgnoreCase("tmp")) {
            	v.set("H,"+tokens[1]+","+tokens[2]);
            }
            if(flag.equalsIgnoreCase("step4_2")) {
            	v.set("R,"+tokens[1]+","+tokens[2]);
            }
            context.write(k, v);
        }

        
	}
	public static class Step5_FilterSortReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	Set<String> user_history = new HashSet<String>();
        	HashMap<String,Float> recommendation = new HashMap<String,Float>();
           
           for(Text item : values) {
        	   String[] tokens = Recommend.DELIMITER.split(item.toString());
        	   if(tokens[0].equalsIgnoreCase("H")) {
        		  user_history.add(tokens[1]);
        	   }
        	   if(tokens[0].equalsIgnoreCase("R")) {
         		  recommendation.put(tokens[1],Float.parseFloat(tokens[2]));
         	   }
           }
           for(String item: user_history) {
        	   recommendation.remove(item);
           }
           List<Entry<String,Float>> list = new LinkedList<Entry<String,Float>>();
   		   list=SortHashMap.sortHashMap(recommendation);
   		   StringBuffer recommend_val = new StringBuffer();
   		   recommend_val.append("The user id is: " + key.toString()+"\n");
   		   for(Entry<String,Float> ilist : list){
   			recommend_val.append(ilist.getKey());
   			recommend_val.append("\t");
   			recommend_val.append(ilist.getValue());
   			recommend_val.append("\n");
   		   }		
   		   if(key.toString().equalsIgnoreCase("953"))
   			   context.write(key, new Text(recommend_val.toString()));
        }
    }
	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
		//get configuration info
		Configuration conf = Recommend.config();
		// I/O path
		Path input1 = new Path(path.get("Step5Input1"));
		Path input2 = new Path(path.get("Step5Input2"));
		Path output = new Path(path.get("Step5Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step5.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step5_FilterSortMapper.class);
        job.setReducerClass(Step5_FilterSortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input1,input2);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
	}
}

