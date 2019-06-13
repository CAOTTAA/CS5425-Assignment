package task2;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step4_2 {
	public static class Step4_RecommendMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        	
        	String[] tokens = Pattern.compile("[\t]").split(values.toString());
            Text k = new Text(tokens[0]);
            Text v = new Text(tokens[1]);
            context.write(k, v);
        }
    }

    public static class Step4_RecommendReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          
        	Map<String,Float> item_scores = new HashMap<String,Float>();
        	for(Text item : values) {
         	   String[] tokens = Pattern.compile("[,]").split(item.toString());
         	   String item_id = tokens[0];
         	   Float scores = Float.parseFloat(tokens[1]);
         	   if(item_scores.containsKey(item_id)) {
         		   item_scores.put(item_id, scores + item_scores.get(item_id));
         	   }else {
         		  item_scores.put(item_id, scores);
         	   }
            }
        	Iterator<String> iter = item_scores.keySet().iterator();
        	while(iter.hasNext()) {
        		String item_id = iter.next();
        		float scores = item_scores.get(item_id);
        		Text v = new Text(item_id+","+scores);
        		context.write(key,v);
        	}
            
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
    	//get configuration info
		Configuration conf = Recommend.config();
		// get I/O path
		Path input = new Path(path.get("Step4_2Input"));
		Path output = new Path(path.get("Step4_2Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step4_2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_2.Step4_RecommendMapper.class);
        job.setReducerClass(Step4_2.Step4_RecommendReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
}
}

