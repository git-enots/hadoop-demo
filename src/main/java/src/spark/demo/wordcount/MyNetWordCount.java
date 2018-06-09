package src.spark.demo.wordcount;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
public class MyNetWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) throws InterruptedException{
		SparkConf sparkConf = new SparkConf().setAppName("MyNetWordCount").setMaster("local[2]");
		JavaStreamingContext jssc=new JavaStreamingContext(sparkConf,Durations.seconds(5));
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("192.168.0.108",7788,StorageLevel.MEMORY_ONLY());
		JavaDStream<String>  words = lines.flatMap((String s)->Arrays.asList(SPACE.split(s)).iterator());
		JavaPairDStream<String, Integer> ones= words.mapToPair(
        		(String s)->new Tuple2<String, Integer>(s, 1) );
		JavaPairDStream<String, Integer>  counts = ones.reduceByKey(
        		(Integer i1,Integer i2)->i1+i2
        		);
		counts.print();

		
		jssc.start();
		jssc.awaitTermination();
	}
	
}
