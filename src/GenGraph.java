/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GenGraph {

  public static class MyMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text outKey = new Text();
    private Text outVal = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String s = value.toString();
        
        String title = getTitle(s);
        String id = getID(s);
        HashSet<String> outLinkSet = new HashSet<String>(getOutLinkSet(s));
        String tmp_outVal = "";
        for(Iterator<String> it=outLinkSet.iterator();it.hasNext();) {
        	tmp_outVal += it.next() + "\t";
	    }
        
        outKey.set(title+"\t"+id);
        outVal.set(tmp_outVal);
        context.write(outKey, outVal);

      }
    }
  
  
  public static class MyRudecer 
       extends Reducer<Text, Text, Text, Text> {
	  
	  private Text outVal = new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
    	String tmp_outVal = "";
    	// HashSet<String> tmp_outLinkSet = new HashSet<String>();
	    for (Text val : values) {
	    	tmp_outVal += val.toString();
	    }
        outVal.set(tmp_outVal);
        context.write(key, outVal);
    }
  }
  
  public static String getTitle(String str) {
	  String title = "";
	  java.util.regex.Pattern titlePtn = java.util.regex.Pattern.compile("(?<=<title>)(.*?)(?=</title>)");
      java.util.regex.Matcher titleMtr = titlePtn.matcher(str);
	  if(titleMtr.find()) {
		  title = titleMtr.group();
	  }
	  return title;
  }
  
  public static String getID(String str) {
	  String ID = "";
	  java.util.regex.Pattern IDPtn = java.util.regex.Pattern.compile("(?<=<id>)(.*?)(?=</id>)");
      java.util.regex.Matcher IDMtr = IDPtn.matcher(str);
	  if(IDMtr.find()) {
		  ID = IDMtr.group();
	  }
	  return ID;
  }
  
  public static HashSet<String> getOutLinkSet(String str) {
	  HashSet<String> outLinkList = new HashSet<String>();
	  String s = "";
	  java.util.regex.Pattern textPtn = java.util.regex.Pattern.compile("(?<=<text )(.*?)(?=</text>)");
      java.util.regex.Matcher textMtr = textPtn.matcher(str);
	  if(textMtr.find()) {
		  s = textMtr.group();
	  }
	  
	  int start = 0;  
      int startFlag = 0;  
      int endFlag = 0;  
      for (int i = 0; i < s.length(); i++) {  
          if (s.charAt(i) == '[' && s.charAt(i + 1) == '[') {
        	  i++;
              startFlag++;  
              if (startFlag == endFlag + 1) {  
                  start = i;  
              }  
          } else if (s.charAt(i) == ']' && s.charAt(i + 1) == ']') {
        	  i++;
              endFlag++;  
              if (endFlag == startFlag) {  
            	  outLinkList.add(s.substring(start + 1, i - 1));  
              }  
          }  
      }
      return outLinkList;
  }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: GenGraph <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "Gen Graph");
    job.setJarByClass(GenGraph.class);
    job.setMapperClass(MyMapper.class);
    // job.setCombinerClass(MyRudecer.class);
    job.setReducerClass(MyRudecer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
