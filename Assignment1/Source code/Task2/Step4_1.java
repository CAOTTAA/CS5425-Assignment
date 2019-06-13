package task2;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

public class Step4_1 {
	public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {

        // you can solve the co-occurrence Matrix/left matrix and score matrix/right matrix separately
		private String flag;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// data set
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            Text k = new Text(tokens[0]);
            Text v = new Text();
            if(flag.equalsIgnoreCase("step3_1")) {
            	v.set("U,"+tokens[1]+","+tokens[2]);
            }
            if(flag.equalsIgnoreCase("step3_2")) {
            	v.set("I,"+tokens[1]+","+tokens[2]);
            }
            context.write(k, v);
        }

    }

    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	Map<String,Float> user_scores = new HashMap<String,Float>();
        	Map<String,Integer> item_conoccurrence_num = new HashMap<String,Integer>();
           
           
           for(Text item : values) {
        	   String[] tokens = Recommend.DELIMITER.split(item.toString());
        	   if(tokens[0].equalsIgnoreCase("U")) {
        		   user_scores.put(tokens[1],Float.valueOf(tokens[2]));
        	   }
        	   if(tokens[0].equalsIgnoreCase("I")) {
        		   item_conoccurrence_num.put(tokens[1],Integer.valueOf(tokens[2]));
        	   }
           }
           Iterator<String> iter = user_scores.keySet().iterator();
           while(iter.hasNext()) {
        	   String user_id = iter.next();
        	   float scores = user_scores.get(user_id);
        	   Iterator<String> iter_item = item_conoccurrence_num.keySet().iterator();
        	   while(iter_item.hasNext()) {
        		   String item_id = iter_item.next();
        		   int num = item_conoccurrence_num.get(item_id);
        		   float res = num * scores;
        		   Text k = new Text(user_id);
				   Text v = new Text(item_id + "," + res);
				   context.write(k, v);
        	   }
           }
           
           
           
    }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
    	//get configuration info
		Configuration conf = Recommend.config();
		// get I/O path
		Path input1 = new Path(path.get("Step4_1Input1"));
		Path input2 = new Path(path.get("Step4_1Input2"));
		Path output = new Path(path.get("Step4_1Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step4_1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_1.Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_1.Step4_AggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input1, input2);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);

}
}

