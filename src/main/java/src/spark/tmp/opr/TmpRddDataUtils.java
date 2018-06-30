package src.spark.tmp.opr;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TmpRddDataUtils {
	private static final int MISSING = 9999;
	private static final String SLIPT_STATION_TMP = "*";
	public static final String GZ_STATION_CODE="592870";
	public static final String[] GD_STATION_CODE={"592710","592780","592870","592930","592940",
			"592980","593030","593160","593170","593240","594560","594620","594780","594930","595010",
			"596580","596630","596640","596730","597540"};
	private static final int BEG_STATION_INDEX=4;
	private static final int BEG_STATION_END=10;
	public static  JavaRDD<String> filterData(String filePath,JavaSparkContext ctx){
		JavaRDD<String> lines = ctx.textFile(filePath, 1);
        lines=lines.filter(row->{
        	int airTemperature;
        	if (row.charAt(87) == '+') { // parseInt doesn't like leading plus           
        	        airTemperature = Integer.parseInt(row.substring(88, 92));// signs
        	} else {
        	        airTemperature = Integer.parseInt(row.substring(87, 92));
        	}
        	String quality = row.substring(92, 93);
        	return airTemperature != MISSING && quality.matches("[01459]");
        });
		return lines;
	}
	public static  JavaRDD<String> filterCityData( JavaRDD<String> lines,String cityStationCode){
		if(cityStationCode!=null&&cityStationCode.length()>0){
			return lines.filter(row->{
				String station = row.substring(BEG_STATION_INDEX, BEG_STATION_END);
				return cityStationCode.equals(station);
			});
		}
		return lines;
	}
	public static  JavaRDD<String> filterProvinceData( JavaRDD<String> lines,String[] ProvinceStationCode){
		if(ProvinceStationCode!=null&&ProvinceStationCode.length>0){
			return lines.filter(row->{
				String station = row.substring(BEG_STATION_INDEX, BEG_STATION_END);
				return Arrays.asList(ProvinceStationCode).contains(station);
				
				 
			});
		}
		return lines;
	}
	
	/**
	 * 每天+气象站,温度
	 * @param lines
	 * @return
	 */
	public static JavaPairRDD<String, Integer> pairDayStataionTempData(JavaRDD<String> lines){
		 JavaPairRDD<String, Integer> ones = lines.mapToPair(
	        		(String row)->{
	        			String station = row.substring(BEG_STATION_INDEX, BEG_STATION_END);
	        			String day = row.substring(15, 23);
	        			return new Tuple2<String, Integer>(day+SLIPT_STATION_TMP+station, retValidTemperature(row));
	        		} );
		
		 return ones;
	}
	/**
	 * 每月+气象站,温度
	 * @param lines
	 * @return
	 */
	public static JavaPairRDD<String, Integer> pairMonthStataionTempData(JavaRDD<String> lines){
		 JavaPairRDD<String, Integer> ones = lines.mapToPair(
	        		(String row)->{
	        			String station = row.substring(BEG_STATION_INDEX, BEG_STATION_END);
	        			String month = row.substring(15, 21);
	        			return new Tuple2<String, Integer>(month+SLIPT_STATION_TMP+station, retValidTemperature(row));
	        		} );
		
		 return ones;
	}
	/**
	 * 每年+气象站,温度
	 * @param lines
	 * @return
	 */
	public static JavaPairRDD<String, Integer> pairYearStataionTempData(JavaRDD<String> lines){
		 JavaPairRDD<String, Integer> ones = lines.mapToPair(
	        		(String row)->{
	        			String station = row.substring(BEG_STATION_INDEX, BEG_STATION_END);
	        			String year = row.substring(15, 19);
	        			return new Tuple2<String, Integer>(year+SLIPT_STATION_TMP+station, retValidTemperature(row));
	        		} );
		
		 return ones;
	}
	
	public static JavaPairRDD<String, Integer> reduceMaxData(JavaPairRDD<String, Integer> ones){
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
		 return counts;
	}
	public static JavaPairRDD<String, Integer> reduceMinData(JavaPairRDD<String, Integer> ones){
		 JavaPairRDD<String, Integer> counts = ones.reduceByKey(
				 (Integer i1,Integer i2)-> {
	        			if(i1<i2) {
	        				return i1;
	        			}
	        			else{
	        				return i2;
	        			}
    			}
				 );
	
		 return counts;
	}
	public static JavaPairRDD<String, Integer> reduceAvegData(JavaPairRDD<String, Integer> ones){
		 JavaPairRDD<String, Integer> counts = ones.reduceByKey(
				 (Integer i1,Integer i2)-> {
	        			return (i1+i2)/2;
   			}
				 );
	
		 return counts;
	}
	private static int retValidTemperature(String row){
		int airTemperature;
		if (row.charAt(87) == '+') { // parseInt doesn't like leading plus
            // signs
			airTemperature = Integer.parseInt(row.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(row.substring(87, 92));
		}
		return airTemperature;
	}
}
