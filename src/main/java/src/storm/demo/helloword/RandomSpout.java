package src.storm.demo.helloword;


import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RandomSpout extends BaseRichSpout {
    
    private SpoutOutputCollector collector;
    private static String[] words = {"Hadoop","Storm","Apache","Linux","Nginx","Tomcat","Spark"};
    public void nextTuple() {
        String word = words[new Random().nextInt(words.length)];
        collector.emit(new Values(word));

    }

    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
        this.collector = arg2;
    }

    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("randomstring"));
    }

}