package src.mapreduce.demo.maxtem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * 1\ mvn package 
 * 2\ wget -D --accept-regex=REGEX -P data -r -c ftp://ftp.ncdc.noaa.gov/pub/data/noaa/2017/5*
 * 3\ zcat /home/chensj/weatherData/data/ftp.ncdc.noaa.gov/pub/data/noaa/5*.gz > data.txt
 * 4\ hdfs dfs -put 20180519_weather_data.txt  /user/xink/data
 * 5\ yarn jar hadoop.demo-1.0-SNAPSHOT.jar  src.mapreduce.demo.MaxTemperature 
 *  -Dinput=/user/xink/data/20180519_weather_data.txt  
 *  -Doutput=/temp/weather_20180519_result
 *  
 *  hdfs dfs -cat /temp/weather_20180519_result/*
 * */
public class MaxTemperature extends Configured implements Tool {    
    @Override
    public int run(String[] args) throws Exception{
        // 作业配置
        // 构建作业所处理的数据的输入输出路径
        Configuration conf=getConf();
        Path input=new Path(conf.get("input"));
        Path output=new Path(conf.get("output"));
        // 构建作业配置
        Job job=Job.getInstance(conf,
            this.getClass().getSimpleName()+":Kevin");
        // 设置该作业所要执行的类
        job.setJarByClass(this.getClass());

        // 设置自定义的Mapper类以及Map端数据输出时的数据类型
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置自定义的Reducer类以及数据输出数据类型
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置读取最原始数据的格式信息以及
        // 数据输出到HDFS集群中的格式信息
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置数据读入和输出的路径到相关的Format类中
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);
        // 控制Reduce的个数
        job.setNumReduceTasks(2);
        // 提交作业
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new MaxTemperature(),args));
    }
}
