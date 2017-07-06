// get inverted index

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class IIdx {
    public static class IIdxMapper extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();
        private FileSplit split;
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit)context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            while(itr.hasMoreTokens()) {
            	String tmp_str = itr.nextToken().replaceAll("[\\pP‘’“”]", "").toLowerCase();
            	if(tmp_str.equals("")) {
            		continue;
            	}
                keyInfo.set(tmp_str);
                valueInfo.set(split.getPath().getName().toString());
                context.write(keyInfo, valueInfo);
            }
        }
        
    }
    
    public static class IIdxReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> wordidx = new HashMap<String, Integer>();
            String word = key.toString();
            String doclist = new String();
            int wordcount = 0;
            for(Text val : values) {
            	String filename = val.toString();
            	if(wordidx.containsKey(filename)) {
            		wordidx.put(filename, wordidx.get(filename) + 1);
            	} else {
            		wordidx.put(filename, 1);
            	}
            }
            Iterator<Entry<String, Integer>> iter = wordidx.entrySet().iterator();
	            while (iter.hasNext()) {
	            HashMap.Entry entry = (HashMap.Entry) iter.next();
	            doclist += (entry.getKey() + ":" + entry.getValue().toString() + ";");
	            wordcount += (int) entry.getValue();
            }
	        if(wordcount <= 500) {
		        result.set(doclist);
		        context.write(key, result);
	        }
        	
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2) {
            System.err.println("Usage: IIdx <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "IIdx");
        job.setJarByClass(IIdx.class);
        
        job.setMapperClass(IIdxMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        // job.setCombinerClass(IIdxCombiner.class);
        job.setReducerClass(IIdxReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
