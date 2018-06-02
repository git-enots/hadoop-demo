package src.hadoop.demo;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationFactory {
    private ConfigurationFactory(){}
    public final static String KEY_FS_DEFAULTFS="fs.defaultFS";
    public final static String VAL_FS_DEFAULTFS="hdfs://192.168.0.108:9000";
    public static class ConfigurationStaticInner{
    	private final static Configuration configuration=new Configuration(); 
    	static{
    		configuration.set(KEY_FS_DEFAULTFS, VAL_FS_DEFAULTFS);
    	}
    }
    public static Configuration getInstance(){
    	return ConfigurationStaticInner.configuration;
    }
}
