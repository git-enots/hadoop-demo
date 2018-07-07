package src.spark.demo.graph;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class GraphShortPaths {

	public static void main( String[] args ){
		SparkConf conf = new SparkConf().setAppName( "Graph short path" ).setMaster( "local" );
		JavaSparkContext ctx = new JavaSparkContext( conf );
	}
	/**
	 * build verts
	 * @param ctx
	 * @return
	 */
	private static JavaRDD<Tuple2<Object, String>> buildVerts(JavaSparkContext ctx){
		return  ctx.parallelize( Arrays.asList(  
				new Tuple2<Object, String>( 1L, "a" ),
				new Tuple2<Object, String>( 2L, "b" ),
				new Tuple2<Object, String>( 3L, "c" ),
				new Tuple2<Object, String>( 4L, "d" ),
				new Tuple2<Object, String>( 5L, "e" )) );
	} 
	/**
	 * build edges
	 * @param ctx
	 * @return
	 */
//	private static JavaRDD<Edge<Double>>  buildEdges (JavaSparkContext ctx){
//		return  ctx.parallelize( Arrays.asList( 
//				new Edge<Double>( 1L, 2L, 10.0 ),
//				new Edge<Double>( 2L, 3L, 20.0 ),
//				new Edge<Double>( 2L, 4L, 30.0 ),
//				new Edge<Double>( 4L, 5L, 80.0 ),
//				new Edge<Double>( 1L, 4L, 30.0 )
//				) );
//	}  
}
