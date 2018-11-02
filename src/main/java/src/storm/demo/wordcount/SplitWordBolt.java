package src.storm.demo.wordcount;


import org.apache.storm.task.OutputCollector;  
import org.apache.storm.task.TopologyContext;  
import org.apache.storm.topology.OutputFieldsDeclarer;  
import org.apache.storm.topology.base.BaseRichBolt;  
import org.apache.storm.tuple.Fields;  
import org.apache.storm.tuple.Tuple;  
import org.apache.storm.tuple.Values;  
 
import java.util.HashMap;  
import java.util.Map;  
 
/** 
* 简单的按照空格进行切分后，发射到下一阶段bolt 
* Created by QinDongLiang on 2016/8/31. 
*/  
public class SplitWordBolt extends BaseRichBolt {  
 
   Map<String,Integer> counts=new HashMap<>();  
 
   private OutputCollector outputCollector;  
 
   @Override  
   public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {  
       this.outputCollector=outputCollector;  
   }  
 
   @Override  
   public void execute(Tuple tuple) {  
       String sentence=tuple.getString(0);  
//       System.out.println("线程"+Thread.currentThread().getName());  
//       简单的按照空格进行切分后，发射到下一阶段bolt  
      for(String word:sentence.split(" ") ){  
          outputCollector.emit(new Values(word));//发送split  
      }  
 
   }  
 
   @Override  
   public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {  
       //声明输出的filed  
       outputFieldsDeclarer.declare(new Fields("word"));  
   }  
}  
