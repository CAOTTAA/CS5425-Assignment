package wordCount;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 

public class CommonWords {
	
	//Remove Stopwords
    public static class TokenizerWCMapper extends
            Mapper<Object, Text, Text, IntWritable> {
 
        Set<String> stopwords = new HashSet<String>();
 
        @Override
        protected void setup(Context context) {
            try {
                Path path = new Path("hdfs://master:9000/stopwords.txt");
            	FileSystem fs = FileSystem.get(new URI(path.toString()),context.getConfiguration());
            	BufferedReader br = new BufferedReader(new InputStreamReader(
                        fs.open(path)));
                String word = null;
                while ((word = br.readLine()) != null) {	
                    stopwords.add(word);
                   
                }
                
            } catch (IOException | URISyntaxException e) {
                e.printStackTrace();
            }
            
        }
 
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
 
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

        	StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            	String token = itr.nextToken();
            	token = tokenClean(token);
                word.set(token);
                if (stopwords.contains(word.toString()) || token.isEmpty() || token.length() == 1)
                    continue;
                context.write(word, one);
            }
        }
        
        public String tokenClean(String token) {
        	int length = token.length();
        	int start = 0;
        	int end = length-1;
        	for(; start < length; start++) {
        	char c = token.charAt(start);
        	if((c >= 'A'&& c<='Z')||(c >='a' && c<='z')) {
        		break;
        		}
        	}
        	
        	for(; end >= 0; end--) {
            	char c = token.charAt(end);
            	if((c >= 'A'&& c<='Z')||(c >='a' && c<='z')) {
            		break;
            	}
            }
        	
        	if(start > end) {
        		return "";
        	}
        	return token.substring(start, end + 1).toLowerCase();
        	
        }
    }
 
    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable count = new IntWritable();
 
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }
    
    public static class Mapper1 extends Mapper<Text, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable frequency  = new IntWritable();
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
        	word.set(key.toString());
        	frequency.set(Integer.parseInt(value.toString()));
        	context.write(word, frequency);
        }
    }

 	//Mapper2
    public static class Mapper2 extends Mapper<Text, Text, Text, IntWritable> {
    	private Text word = new Text();
        private IntWritable frequency  = new IntWritable();
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
        	word.set(key.toString());
        	frequency.set(Integer.parseInt(value.toString()));
        	context.write(word, frequency);
        }
    }
     
    //Get the number of common words reduce
    public static class CountCommonReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable commoncount = new IntWritable();
 
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
               
        	int count = 0;
        	int init = Integer.MAX_VALUE;
        	for(IntWritable item : values) {
        		count++;
        		init = Math.min(init, item.get());
        	}
        	
        	if(2 == count) {
        	  commoncount.set(init);
        	  context.write(key, commoncount);
        	 }
            }
        }
    
    
    
    
    
    //sort the result
    public static class SortMapper extends Mapper<Text, Text, IntWritable, Text> {
        private IntWritable count = new IntWritable();
        private Text word = new Text();
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
        	word.set(key.toString());
        	count.set(Integer.valueOf(value.toString()));
        	context.write(count,word);
        }
    }
     
    public static class SortReducer extends
            Reducer<IntWritable, Text, IntWritable, Text> {
    	private int rank = 0;
        public void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
                
        	    if(rank >= 15)
        	    	return;
        	    for(Text text : values) {
        	    	if(rank < 15) {
        	    		context.write(key,text);
        	    		rank++;
        	    	}
        	    	
        	    }
            }
      }
    
     // rewrite the key sort comparator, to make sure the IntWritable as key, the order is reverse
     public static class ReverseIntWritableComparator extends WritableComparator {
        public ReverseIntWritableComparator() {
          super(IntWritable.class);
        }
        
        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
          int thisValue = readInt(b1, s1);
          int thatValue = readInt(b2, s2);
          return (thisValue<thatValue ? 1 : (thisValue==thatValue ? 0 : -1));
        }
      }
     
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 6) {
            System.err
                    .println("Usage: CommonWords <input1> <output1> <input2> <output2> "
                            + "<output3> <output4>");
            System.exit(2);
        }
        
        
        
        Job job1 = new Job(conf, "WordCount1");
        job1.setJarByClass(CommonWords.class);
        job1.setMapperClass(TokenizerWCMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        job1.waitForCompletion(true);
 
        Job job2 = new Job(conf, "WordCount2");
        job2.setJarByClass(CommonWords.class);
        job2.setMapperClass(TokenizerWCMapper.class);
        job2.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
        job2.waitForCompletion(true);
 
        Job job3 = new Job(conf, "Count words in common");
        job3.setJarByClass(CommonWords.class);
        job3.setReducerClass(CountCommonReducer.class);
        MultipleInputs.addInputPath(job3, new Path(otherArgs[1]),
                KeyValueTextInputFormat.class, Mapper1.class);
        MultipleInputs.addInputPath(job3, new Path(otherArgs[3]),
                KeyValueTextInputFormat.class, Mapper2.class);
        
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));
        job3.waitForCompletion(true);
        
        
        
        Job job4 = new Job(conf, "sort");
        job4.setJarByClass(CommonWords.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setMapperClass(SortMapper.class);
        job4.setReducerClass(SortReducer.class);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Text.class);
        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setNumReduceTasks(1);
        job4.setSortComparatorClass(ReverseIntWritableComparator.class);
        FileInputFormat.addInputPath(job4, new Path(otherArgs[4]));
        FileOutputFormat.setOutputPath(job4, new Path(otherArgs[5]));
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
        
    }

}