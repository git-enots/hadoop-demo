package src.spark.tmp.opr;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
/*
 * spark-submit  --class src.spark.tmp.opr.SparkTmpApp --executor-memory 500m --total-executor-cores 2 /home/clouder/weatherdata/hadoop-demo-0.0.1-SNAPSHOT.jar 
 * union join
 * 
 * 
 * 
 * spark-submit  --class src.spark.tmp.opr.SparkTmpApp --master spark://Master:7077 --executor-memory 1024m --total-executor-cores 2 /home/clouder/weatherdata/hadoop-demo-0.0.1-SNAPSHOT.jar 
 * */
public class SparkTmpApp {

	public static void main(String[] args) {
			//String filePath="file:/home/clouder/weatherdata/weatherData_20180630.txt";
		    String filePath="hdfs:/sparkDemo/weatherdata/weatherData_20180630.txt";
			SparkConf sparkConf = new SparkConf().setAppName("SparkTmpApp")
					//.setMaster("local[2]")
					;
			
			JavaSparkContext ctx = new JavaSparkContext(sparkConf);
			ctx.setLogLevel("ERROR");
			
			JavaRDD<String> rddData=null;
			try{
				rddData=TmpRddDataUtils.filterData(filePath, ctx);
				rddData.cache();
				createDayStatationTempFile(rddData);
				createMonthStatationTempFile(rddData);
				createYearStatationTempFile(rddData);
				createProvinceStatationTempFile(rddData);
				createCityStatationTempFile(rddData);
			}
			catch (Exception e) {
				
			}
			finally {
				if(rddData!=null){
					rddData.unpersist();
				}
			}
	}
	/**
	 * yyyymmdd*xxxxx,tt
	 * @param filePath
	 * @param ctx
	 */
	private static void createDayStatationTempFile(JavaRDD<String> rddData){
		JavaPairRDD<String, Integer> dayStataionTemp =null;
		String fileMaxPath="file:/home/clouder/weatherdata/dayStationMax";
		String fileMinPath="file:/home/clouder/weatherdata/dayStationMin";
		try {
				dayStataionTemp=TmpRddDataUtils.pairDayStataionTempData(rddData);
				dayStataionTemp.cache(); 
				//存储文件为一个分区 data.coalesce(1,true).saveAsTextFile() data.repartition(1).saveAsTextFile( )
				TmpRddDataUtils.reduceMaxData(dayStataionTemp).saveAsTextFile(fileMaxPath);
				//存储文件为一个分区
				TmpRddDataUtils.reduceMinData(dayStataionTemp).saveAsTextFile(fileMinPath);
			} catch (Exception e) {
				// TODO: handle exception
			}
			finally{
				dayStataionTemp.unpersist();
			}
	}
	/**
	 * yyyymm*xxxxx,tt
	 * @param filePath
	 * @param ctx
	 */
	private static void createMonthStatationTempFile(JavaRDD<String> rddData){
		JavaPairRDD<String, Integer> dayStataionTemp =null;
		String fileMaxPath="file:/home/clouder/weatherdata/monthStationMax";
		String fileMinPath="file:/home/clouder/weatherdata/monthStationMin";
		try {
				dayStataionTemp=TmpRddDataUtils.pairMonthStataionTempData(rddData);
				dayStataionTemp.cache(); 
				//存储文件为一个分区 data.coalesce(1,true).saveAsTextFile() data.repartition(1).saveAsTextFile( )
				TmpRddDataUtils.reduceMaxData(dayStataionTemp).saveAsTextFile(fileMaxPath);
				//存储文件为一个分区
				TmpRddDataUtils.reduceMinData(dayStataionTemp).saveAsTextFile(fileMinPath);
			} catch (Exception e) {
				// TODO: handle exception
			}
			finally{
				dayStataionTemp.unpersist();
			}
	}
	/**
	 * yyyy*xxxxx,tt
	 * @param filePath
	 * @param ctx
	 */
	private static void createYearStatationTempFile(JavaRDD<String> rddData){
		JavaPairRDD<String, Integer> dayStataionTemp =null;
		String fileMaxPath="file:/home/clouder/weatherdata/yearStationMax";
		String fileMinPath="file:/home/clouder/weatherdata/yearStationMin";
		try {
				dayStataionTemp=TmpRddDataUtils.pairYearStataionTempData(rddData);
				dayStataionTemp.cache(); 
				//存储文件为一个分区 data.coalesce(1,true).saveAsTextFile() data.repartition(1).saveAsTextFile( )
				TmpRddDataUtils.reduceMaxData(dayStataionTemp).saveAsTextFile(fileMaxPath);
				//存储文件为一个分区
				TmpRddDataUtils.reduceMinData(dayStataionTemp).saveAsTextFile(fileMinPath);
			} catch (Exception e) {
				// TODO: handle exception
			}
			finally{
				dayStataionTemp.unpersist();
			}
	}
	
	private static void createProvinceStatationTempFile(JavaRDD<String> rddData){
		JavaPairRDD<String, Integer> dayStataionTemp =null;
		String fileAveragePath="file:/home/clouder/weatherdata/monthProvinceStationAverage";
		
		try {
			    JavaRDD<String> provinceData=TmpRddDataUtils.filterProvinceData(rddData, TmpRddDataUtils.GD_STATION_CODE);
				dayStataionTemp=TmpRddDataUtils.pairMonthStataionTempData(provinceData);
				
				//存储文件为一个分区 data.coalesce(1,true).saveAsTextFile() data.repartition(1).saveAsTextFile( )
				TmpRddDataUtils.reduceAvegData(dayStataionTemp).saveAsTextFile(fileAveragePath);

			} catch (Exception e) {
				// TODO: handle exception
			}
			
	}
	private static void createCityStatationTempFile(JavaRDD<String> rddData){
		JavaRDD<String> cityData =null;
		String fileDayCityStationMaxPath="file:/home/clouder/weatherdata/dayCityStationMax";
		String fileDayCityStationMinPath="file:/home/clouder/weatherdata/dayCityStationMin";
		String fileMonthCityStationMaxPath="file:/home/clouder/weatherdata/monthCityStationMax";
		String fileMonthCityStationMinPath="file:/home/clouder/weatherdata/monthCityStationMin";
		
		try {
			     cityData=TmpRddDataUtils.filterCityData(rddData, TmpRddDataUtils.GZ_STATION_CODE);
			     cityData.cache(); 
			     JavaPairRDD<String, Integer> dayStataionTemp=TmpRddDataUtils.pairDayStataionTempData(cityData);
				 TmpRddDataUtils.reduceMaxData(dayStataionTemp).saveAsTextFile(fileDayCityStationMaxPath);
				 
				 JavaPairRDD<String, Integer> dayStataionTemp1=TmpRddDataUtils.pairDayStataionTempData(cityData);
				 TmpRddDataUtils.reduceMinData(dayStataionTemp1).saveAsTextFile(fileDayCityStationMinPath);
				 
				 JavaPairRDD<String, Integer> dayStataionTemp2=TmpRddDataUtils.pairMonthStataionTempData(cityData);
				 TmpRddDataUtils.reduceMaxData(dayStataionTemp2).saveAsTextFile(fileMonthCityStationMaxPath);
				 
				 JavaPairRDD<String, Integer> dayStataionTemp3=TmpRddDataUtils.pairMonthStataionTempData(cityData);
				 TmpRddDataUtils.reduceMinData(dayStataionTemp3).saveAsTextFile(fileMonthCityStationMinPath);
				 

			} 
		catch (Exception e) {
				// TODO: handle exception
			}
		finally{
			if(cityData!=null)
				cityData.unpersist();
			}
		}
	
	

}
