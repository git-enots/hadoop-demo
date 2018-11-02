package src.storm.demo.wordcount;


import org.apache.storm.Config;  
import org.apache.storm.LocalCluster;  
import org.apache.storm.StormSubmitter;  
import org.apache.storm.topology.TopologyBuilder;  
import org.apache.storm.tuple.Fields;  

 
public class TopologyWordCount {  

  public static void main(String[] args) throws  Exception {  
      TopologyBuilder builder=new TopologyBuilder();  
      //设置数据源  
      builder.setSpout("spout",new CreateSentenceSpout(),1);  
      //读取spout数据源的数据，进行split业务逻辑  
      builder.setBolt("split",new SplitWordBolt(),1).shuffleGrouping("spout");  
      //读取split后的数据，进行count (tick周期10秒)  
      builder.setBolt("count",new SumWordBolt(),1).fieldsGrouping("split",new Fields("word"));  
      //读取count后的数据，进行缓冲打印 （tick周期3秒，仅仅为测试tick使用，所以多加了这个bolt）  
      //builder.setBolt("show",new ShowBolt(),1).shuffleGrouping("count");  
      //读取show后缓冲后的数据，进行最终的打印 （实际应用中，最后一个阶段应该为持久层）  
      builder.setBolt("final",new FinalBolt(),1).allGrouping("count");  

      Config config=new Config();  
      config.setDebug(false);  
      //集群模式  
      if(args!=null&&args.length>0){  
          config.setNumWorkers(2);  
          StormSubmitter.submitTopology(args[0],config,builder.createTopology());  
      //单机模式  
      }else{  
          config.setMaxTaskParallelism(1);;  
          LocalCluster cluster=new LocalCluster();  
          cluster.submitTopology("word-count",config,builder.createTopology());  
          Thread.sleep(100000);  
          cluster.shutdown();  
      }  
  }  

}  
