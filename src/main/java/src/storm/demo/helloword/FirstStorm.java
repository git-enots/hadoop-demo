package src.storm.demo.helloword;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

//import backtype.storm.Config;
//import backtype.storm.LocalCluster;
//import backtype.storm.StormSubmitter;
//import backtype.storm.generated.AlreadyAliveException;
//import backtype.storm.generated.InvalidTopologyException;
//import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.utils.Utils;

public class FirstStorm {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSpout());
        builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout");
        Config conf = new Config();
        conf.setDebug(false);
        //集群
        if(args != null && args.length > 0) {
            conf.setNumWorkers(3);
            try {
               
					StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }catch (AuthorizationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				
            } catch (AlreadyAliveException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } 
        //本地
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("firststorm", conf, builder.createTopology());
            Utils.sleep(30000);
            cluster.killTopology("firststorm");
            cluster.shutdown();
        }
    }

}
