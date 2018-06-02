package src.hadoop.demo;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class P00031_HdfsDemo_PutFile_0010 extends Configured implements Tool{
    //节点运行 不可本地运行
	FSDataOutputStream os=null;
    @Override
    public int run(String[] strings) throws Exception{
    	     Configuration conf=getConf();
            String input=conf.get("input");
            String output=conf.get("output");
            FileSystem inFs=
                FileSystem.get(
                    URI.create(input),conf);
            FSDataInputStream is=
                inFs.open(new Path(input));
            FileSystem outFs=
                FileSystem.getLocal(conf);
            FSDataOutputStream os=
                outFs.create(new Path(output));
            IOUtils.copyBytes(is,os,conf,true);
            return 0;
    }

    public static void main(String[] args) throws Exception{
    	String[] argss= {"-Dinput=///user/xink/aa.txt","-Doutput=55.txt"};
    	Configuration conf= ConfigurationFactory.getInstance();
    	System.exit(ToolRunner.run(conf,new P00031_HdfsDemo_PutFile_0010(),argss));
    }

}
