package src.hadoop.video.demo;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class Demo1 {

	@Test
	public void testMkDir() throws Exception{
		//这是一个客户端，链接到服务器上
		//服务器的地址 ： 核心的配置文件 core-site.xml
		/*
		 * <!--配置NameNode的地址-->
		<property>
		  <name>fs.defaultFS</name>
		  <value>hdfs://192.168.157.111:9000</value>
		</property>
		 */
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");
		
		//创建一个客户端
		FileSystem client = FileSystem.get(conf);
		
		//创建一个目录
		client.mkdirs(new Path("/bbb"));
		
		//关闭客户端
		client.close();
	}
	
	@Test
	public void testUpload() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");
	
		//创建一个客户端
		FileSystem client = FileSystem.get(conf);
		
		//得到一个输入流
		InputStream in = new FileInputStream("d:\\temp\\a.txt");
		
		//创建一个输出流 ---> 指向HDFS
		OutputStream out = client.create(new Path("/bbb/a.txt"));
		
		//上传数据: 使用HDFS的工具类
		IOUtils.copyBytes(in, out, 1024);		
		
		//关闭客户端
		client.close();
	}
}
