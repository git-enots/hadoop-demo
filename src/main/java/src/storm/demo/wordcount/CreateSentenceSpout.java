package src.storm.demo.wordcount;

import backtype.storm.spout.SpoutOutputCollector;  
import backtype.storm.task.TopologyContext;  
import backtype.storm.topology.OutputFieldsDeclarer;  
import backtype.storm.topology.base.BaseRichSpout;  
import backtype.storm.tuple.Fields;  
import backtype.storm.tuple.Values;  
import backtype.storm.utils.Utils;  
import org.joda.time.DateTime;  
  
import java.util.Map;  
import java.util.Random;  
  
/** 
 * Created by QinDongLiang on 2016/8/31. 
 * 创建数据源 
 */  
public class CreateSentenceSpout extends BaseRichSpout {  
    //  
    SpoutOutputCollector collector;  
    Random random;  
    String [] sentences=null;  
  
    @Override  
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {  
        this.collector=spoutOutputCollector;//spout_collector  
        random=new Random();//  
        sentences=new String[]{"hadoop hadoop hadoop java java strom","strom is stream","strom aaa bbb aaa ccc"};  
  
    }  
  
    @Override  
    public void nextTuple() {  
        Utils.sleep(3000);  
        //获取数据  
        String sentence=sentences[random.nextInt(sentences.length)];  
        System.out.println("线程名："+Thread.currentThread().getName()+"  "+new DateTime().toString("yyyy-MM-dd HH:mm:ss  ")+"10s发射一次数据："+sentence);  
        //向下游发射数据  
        this.collector.emit(new Values(sentence));  
    }  
  
    @Override  
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {  
        outputFieldsDeclarer.declare(new Fields("sentence"));  
    }  
}  
