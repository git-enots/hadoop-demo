package src.hadoop.demo;


import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CatDemo_0010 {

	public static void main(String[] args) throws IOException{
		String file="///user/xink/aa.txt";
		// 创建Configuration对象
        Configuration conf= ConfigurationFactory.getInstance();
        // 创建FileSystem对象
        FileSystem fs=
                FileSystem.get(URI.create(file),conf);
        // 需求：查看/user/kevin/passwd的内容
        // args[0] hdfs://192.168.0.108:9000/user/zyh/passwd
        // args[0] file:///etc/passwd
        FSDataInputStream is=
                fs.open(new Path(file));
        byte[] buff=new byte[1024];
        int length=0;
        while((length=is.read(buff))!=-1){
            System.out.println(
                    new String(buff,0,length));
        }
        System.out.println(
                fs.getClass().getName());

	}

}
