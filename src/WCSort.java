// sort the output of WordCount

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WCSort {
	private static class MyLongWritable implements WritableComparable<MyLongWritable> {
        public long val;

        public MyLongWritable() {
        }

        public MyLongWritable(long val) {
            this.val = val;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(val);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            val = in.readLong();
        }

        /*
         * 当key进行排序时会调用以下这个compreTo方法
         */
        @Override
        public int compareTo(MyLongWritable anotherVal) {
            return (int) (anotherVal.val - val);
        }
    }

    public static class MyMapper extends
            Mapper<Object, Text , MyLongWritable, Text> {
        protected void map(
        		Object key,
        		Text value,
                Mapper<Object, Text , MyLongWritable, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            String[] spilted = value.toString().split("\t");
            String word = spilted[0];
            long counter = Long.parseLong(spilted[1]);

            context.write(new MyLongWritable(counter), new Text(word));
        };
    }

    public static class MyReducer extends
            Reducer<MyLongWritable, Text, Text, LongWritable> {
        protected void reduce(
                MyLongWritable key,
                Iterable<Text> values,
                Reducer<MyLongWritable, Text, Text, LongWritable>.Context context)
                throws java.io.IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, new LongWritable(key.val));
            }
        };
    }
    
    
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
          System.err.println("Usage: wordcountsort <in> [<in>...] <out>");
          System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count sort");
        job.setJarByClass(WCSort.class);
        job.setMapperClass(MyMapper.class);
        // job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        
        job.setMapOutputKeyClass(MyLongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        for (int i = 0; i < otherArgs.length - 1; ++i) {
          FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
          new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }

}
