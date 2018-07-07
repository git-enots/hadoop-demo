package src.hadoop.video.demo;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class ShareFriendsStepTwo {
     /**b a,e,j
      * c a,h,e
      * map========
      * a-e b
      * a-j b
      * e-j b
      * a-e c
      * a-h c
      * e-h c
      * 
      * reduce
      * a-e b c
	 */
	 static class ShareFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		  protected void map(LongWritable key, Text value, 
                 Context context) throws IOException, InterruptedException {
				 String line=value.toString();
				 String[] friend_persions=line.split("\t");
				 String friend = friend_persions[0];
				 String[] persions=friend_persions[1].split(",");
				
			     Arrays.sort(persions);
			     for(int i=0;i<persions.length-2;i++){
			    	 for(int j=i+1;j<persions.length-1;j++){
			    		 context.write(new Text(persions[i]+"-"+persions[j]), new Text(friend)); 
			    	 }
			     }
			
		  }
		 
	 }
	 static class ShareFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text>{
		 protected void reduce(Text key, Iterable<Text> values, Context context
                 ) throws IOException, InterruptedException {
			 StringBuilder stringBuilder=new StringBuilder();
			 for(Text friend:values){
				 stringBuilder.append(friend).append(" ");
			 }
			 context.write(new Text(key), new Text(stringBuilder.toString()));
		 }
	 }
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		 Configuration conf=new Configuration();
		 Job job=Job.getInstance(conf);
		 job.setJarByClass(ShareFriendsStepTwo.class);
		 job.setMapperClass(ShareFriendsStepTwoMapper.class);
		 job.setReducerClass(ShareFriendsStepTwoReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
	
		 FileInputFormat.setInputPaths(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 boolean res=job.waitForCompletion(true);
		 System.exit(res?0:1);
	 }
}
