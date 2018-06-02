package src.hadoop.demo;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ListFileStatus_0010
extends Configured
implements Tool{
FileSystem fs;
@Override
public int run(String[] args) throws Exception{
    Configuration conf=getConf();
    String input=conf.get("input");

    fs=FileSystem.get(
            URI.create(input),conf);
    FileStatus[] fileStatuses=
        fs.listStatus(new Path(input));
    for(FileStatus status:fileStatuses){
        process(status);
    }
    return 0;
}

public void process(
    FileStatus fileStatus) throws IOException{
    if(fileStatus.isFile()){
        System.out.println("--------------");
        System.out.println(
            fileStatus.getAccessTime());  //上次访问的时间
        System.out.println(
            fileStatus.getOwner());  //文件的所有者
        System.out.println(
            fileStatus.getGroup());  //文件的所属者
        System.out.println(
            fileStatus.getPath());  //得到文件的路径
        System.out.println(
            fileStatus.getPermission());  //文件的权限
        System.out.println(
            fileStatus.getReplication());  //文件的备份数
    }else if(fileStatus.isDirectory()){

        // 和Java的File不一样的地方：
        // 当File对象所代表的是目录的时候，
        // 可以通过listFiles方法来获取该目录下的所有文件（有可能还包含目录）

        // 在HDFS中，当FileStatus对象代表一个目录的时候
        // 没有相应的方法来获取该目录下的所有文件
        // 要通过FileSystem类来获取该目录下的文件
        //      path=fileStatus.getPath();
        //      FileStatus[] fileStstuses=
        //          fs.listStatus(path);
        FileStatus[] fileStatuses=
            fs.listStatus(fileStatus.getPath());
        for(FileStatus status:fileStatuses){
            process(status);
        }
    }
}

public static void main(String[] args) throws Exception{
	Configuration conf= ConfigurationFactory.getInstance();
	String[] argss= {"-Dinput=///user/xink"};
    System.exit(ToolRunner.run(conf,new ListFileStatus_0010(),argss));
}
}
