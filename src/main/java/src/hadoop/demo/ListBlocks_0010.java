package src.hadoop.demo;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ListBlocks_0010
extends Configured
implements Tool{
@Override
public int run(String[] args) throws Exception{
    Configuration conf=getConf();
    String input=conf.get("input");
    FileSystem fs=
        FileSystem.get(
            URI.create(input),conf);
    HdfsDataInputStream hdis=
        (HdfsDataInputStream)
            fs.open(new Path(input));
    List<LocatedBlock> allBlocks=
        hdis.getAllBlocks();
    for(LocatedBlock block:allBlocks){
        ExtendedBlock eBlock=
            block.getBlock();
        System.out.println("------------------------");
        System.out.println(
            eBlock.getBlockId());
        System.out.println(
            eBlock.getBlockName());
        System.out.println(
            block.getBlockSize());
        System.out.println(
            block.getStartOffset());
        // 获取当前的数据块所在的DataNode的信息
        DatanodeInfo[] locations=
            block.getLocations();
        for(DatanodeInfo info:locations){
            System.out.println(
                info.getIpAddr());
            System.out.println(
                info.getHostName());
        }
    }
    return 0;
}

public static void main(String[] args) throws Exception{
	Configuration conf= ConfigurationFactory.getInstance();
	String[] argss= {"-Dinput=///user/xink/aa.txt"};
    System.exit(
        ToolRunner.run(conf,
            new ListBlocks_0010(),argss));
}
}
