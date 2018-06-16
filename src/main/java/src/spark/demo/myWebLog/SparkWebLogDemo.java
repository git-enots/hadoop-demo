package src.spark.demo.myWebLog;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWebLogDemo {
	/*
    本地执行 setMaster("local[2]")
	
	*/
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		  if (args.length < 1) {
	            System.err.println("Usage: WebLog <file>");
	            System.exit(1);
	      }
		  //String path="D:\\program\\study\\大数据精品VIP视频\\6.MapReduce分布式计算模型\\localhost_access_log.2017-07-30.txt";
		  SparkConf sparkConf = new SparkConf().setAppName("SparkWebLogDemo").setMaster("local[2]");
		  JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		  //192.168.88.1 - - [30/Jul/2017:12:53:43 +0800] "GET /MyDemoWeb/head.jsp HTTP/1.1" 200 713
		  JavaRDD<String> lines = ctx.textFile(args[0], 1).map(x->{
			  int index1=x.indexOf("\"");
			  int index2=x.lastIndexOf("\"");
			  //  "GET /MyDemoWeb/head.jsp HTTP/1.1"
			  x=x.substring(index1+1, index2);
			  index1=x.indexOf(" ");
			  index2=x.lastIndexOf(" ");
			  //  /MyDemoWeb/head.jsp
			  x=x.substring(index1+1, index2);
			  //  head.jsp
			  x=x.substring(x.lastIndexOf("/")+1);
			  return x;
		  }).filter(x->!"".equals(x));
		
		  JavaPairRDD<String, Integer> ones = lines.mapToPair(
	        		(String s)->new Tuple2<String, Integer>(s, 1) );
		  JavaPairRDD<String, Integer> counts = ones.reduceByKey(
	        		(Integer i1,Integer i2)->i1+i2
	        		);
		  
		  List<Tuple2<String, Integer>> output = counts.collect();
	        output.stream().forEach(tuple->{
	        
	        	 System.out.println(tuple._1() + "======" + tuple._2());
	        });
		  ctx.stop();
		  ctx.close();

	}

}
