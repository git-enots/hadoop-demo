package src.spark.demo.maxtem;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
/*spark-submit  --class src.spark.demo.maxtem.SparkMaxTemDemo --executor-memory 500m --total-executor-cores 2 /home/clouder/jar/hadoop-demo-0.0.1-SNAPSHOT.jar hdfs://192.168.0.108:9000/user/xink/data/20180519_weather_data.txt*/
public class SparkMaxTemDemo {
	private static final int MISSING = 9999;
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: MaxTem <file>");
            System.exit(1);
        }

        /**
         * 对于所有的spark程序所言，要进行所有的操作，首先要创建一个spark上下文。
         * 在创建上下文的过程中，程序会向集群申请资源及构建相应的运行环境。
         * 设置spark应用程序名称
         * 创建的 sarpkContext 唯一需要的参数就是 sparkConf，它是一组 K-V 属性对。
         */
        SparkConf sparkConf = new SparkConf().setAppName("SparkMaxTemDemo");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        /**
         * 利用textFile接口从文件系统中读入指定的文件，返回一个RDD实例对象。
         * RDD的初始创建都是由SparkContext来负责的，将内存中的集合或者外部文件系统作为输入源。
         * RDD：弹性分布式数据集，即一个 RDD 代表一个被分区的只读数据集。一个 RDD 的生成只有两种途径，
         * 一是来自于内存集合和外部存储系统，另一种是通过转换操作来自于其他 RDD，比如 Map、Filter、Join，等等。
         * textFile()方法可将本地文件或HDFS文件转换成RDD，读取本地文件需要各节点上都存在，或者通过网络共享该文件
         *读取一行
         */
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        lines=lines.filter(row->{
        	int airTemperature;
        	if (row.charAt(87) == '+') { // parseInt doesn't like leading plus
        	                                                               // signs
        	        airTemperature = Integer.parseInt(row.substring(88, 92));
        	} else {
        	        airTemperature = Integer.parseInt(row.substring(87, 92));
        	}
        	String quality = row.substring(92, 93);
        	return airTemperature != MISSING && quality.matches("[01459]");
        });
      
        
        /**
         * map 键值对 ，类似于MR的map方法
         * pairFunction<T,K,V>: T:输入类型；K,V：输出键值对
         * 表示输入类型为T,生成的key-value对中的key类型为k,value类型为v,对本例,T=String, K=String, V=Integer(计数)
         * 需要重写call方法实现转换
         */
        JavaPairRDD<String, Integer> ones = lines.mapToPair(
        		(String row)->{
        			String year = row.substring(15, 21);
        			int airTemperature;
        			if (row.charAt(87) == '+') { // parseInt doesn't like leading plus
                        // signs
        				airTemperature = Integer.parseInt(row.substring(88, 92));
					} else {
						airTemperature = Integer.parseInt(row.substring(87, 92));
					}
        			return new Tuple2<String, Integer>(year, airTemperature);
        		} );
        /*new PairFunction<String, String, Integer>() {
            //scala.Tuple2<K,V> call(T t)
            //Tuple2为scala中的一个对象,call方法的输入参数为T,即输入一个单词s,新的Tuple2对象的key为这个单词,计数为1
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        }*/
        //A two-argument function that takes arguments
        // of type T1 and T2 and returns an R.
        /**
         * 调用reduceByKey方法,按key值进行reduce
         *  reduceByKey方法，类似于MR的reduce
         *  要求被操作的数据（即下面实例中的ones）是KV键值对形式，该方法会按照key相同的进行聚合，在两两运算
         *  若ones有<"one", 1>, <"one", 1>,会根据"one"将相同的pair单词个数进行统计,输入为Integer,输出也为Integer
         *输出<"one", 2>
         */
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
        		(Integer i1,Integer i2)-> {
	        			if(i1>i2) {
	        				return i1;
	        			}
	        			else{
	        				return i2;
	        			}
        			}
        		);
        /*new Function2<Integer, Integer, Integer>() {
            //reduce阶段，key相同的value怎么处理的问题
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        }*/
        //备注：spark也有reduce方法，输入数据是RDD类型就可以，不需要键值对，
        // reduce方法会对输入进来的所有数据进行两两运算

        /**
         * collect方法用于将spark的RDD类型转化为我们熟知的java常见类型
         */
        List<Tuple2<String, Integer>> output = counts.collect();
        output.stream().forEach(tuple->{
        	 System.out.println(tuple._1() + ": " + tuple._2());
        });
        /* for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }*/
        ctx.stop();
        ctx.close();
    }

}
