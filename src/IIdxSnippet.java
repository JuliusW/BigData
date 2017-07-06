
// get inverted index

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class IIdxSnippet {
	public static class IIdxMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();
		private FileSplit split;
		// static HashMap<String, Long> docsmark = new HashMap<String, Long>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			split = (FileSplit) context.getInputSplit();
			StringTokenizer itr = new StringTokenizer(value.toString());
			

			String filename = split.getPath().getName().toString();
			long linestartpos = 0;
			/*
			if(docsmark.containsKey(filename)) {
				linestartpos = docsmark.get(filename);
				docsmark.put(filename, linestartpos + split.getLength());
			} else {
				linestartpos = 0;
				docsmark.put(filename, linestartpos + split.getLength());
			}
			*/
			linestartpos = key.get();
			while (itr.hasMoreTokens()) {
				String tmp_str = itr.nextToken().replaceAll("[\\pP‘’“”]", "").toLowerCase();
				if (tmp_str.equals("")) {
					continue;
				}
				keyInfo.set(tmp_str);
				valueInfo.set(filename + ":" + String.valueOf(linestartpos));
				context.write(keyInfo, valueInfo);
			}
		}

	}

	public static class IIdxReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, ArrayList<Integer>> wordidx = new HashMap<String, ArrayList<Integer>>();
			String doclist = new String();
			int wordcount = 0;
			for (Text val : values) {
				String[] s = val.toString().split(":");
				String filename = s[0];
				if (wordidx.containsKey(filename)) {
					int cnt = wordidx.get(filename).get(0);
					wordidx.get(filename).set(0, cnt + 1);
					wordidx.get(filename).add(Integer.valueOf(s[1]));
				} else {
					ArrayList<Integer> tmp_one = new ArrayList<Integer>();
					tmp_one.add(1);
					tmp_one.add(Integer.valueOf(s[1]));
					wordidx.put(filename, tmp_one);
				}
			}
			Iterator<Entry<String, ArrayList<Integer>>> iter = wordidx.entrySet().iterator();
			while (iter.hasNext()) {
				HashMap.Entry<String, ArrayList<Integer>> entry = (HashMap.Entry<String, ArrayList<Integer>>) iter.next();
				doclist += entry.getKey()+":";
				for(int i = 0; i < entry.getValue().size(); i++) {
					doclist += (entry.getValue().get(i).toString() + ":");
					if(i == 0) {
						wordcount += (int) entry.getValue().get(i);
					}
				}
				doclist += ";";
			}
			 if (wordcount <= 500) {
				result.set(doclist);
				context.write(key, result);
			 }

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: IIdx <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "IIdx");
		job.setJarByClass(IIdxSnippet.class);

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
