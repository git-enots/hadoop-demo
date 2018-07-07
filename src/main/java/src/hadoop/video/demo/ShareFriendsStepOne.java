package src.hadoop.video.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * qq 好友精准推荐
 * @author Administrator
 * 个人:好友1,好友2,...
 * a:b,c,d,e,f
 * b:c,d,f,h
 * c:b,d,f,h
 * 第一阶段
 * 好友 个人1,个人2,...
 * b a,c,
 * c a,b,
 * d a,b
 * e a,
 * f a,b,c
 * 第二阶段
 * a-b c d f
 * 个人1-个人2 好友1 好友2 ...
 * 
 */




public class ShareFriendsStepOne {
     /**a:b,c,d,e,f
      * b:g,c,d,e,f
      * c:h,d,e,f
      * d:f,a
      * e:a,b
      * 
      * b a,e,
      * c a,b,
	 */
	 static class ShareFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text>{
		  protected void map(LongWritable key, Text value, 
                 Context context) throws IOException, InterruptedException {
				 String line=value.toString();
				 String[] lineArray=line.split(":");
				 for(String friend:lineArray[1].split(",")){
					 context.write(new Text(friend), new Text(lineArray[0]));
				 }
		  }
		 
	 }
	 static class ShareFriendsStepOneReducer extends Reducer<Text, Text, Text, Text>{
		 protected void reduce(Text key, Iterable<Text> values, Context context
                 ) throws IOException, InterruptedException {
			 StringBuilder stringBuilder=new StringBuilder();
			 for(Text persion:values){
				 stringBuilder.append(persion).append(",");
			 }
			 context.write(new Text(key), new Text(stringBuilder.toString()));
		 }
	 }
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		 Configuration conf=new Configuration();
		 Job job=Job.getInstance(conf);
		 job.setJarByClass(ShareFriendsStepOne.class);
		 job.setMapperClass(ShareFriendsStepOneMapper.class);
		 job.setReducerClass(ShareFriendsStepOneReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
	
		 FileInputFormat.setInputPaths(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 boolean res=job.waitForCompletion(true);
		 System.exit(res?0:1);
	 }
}
