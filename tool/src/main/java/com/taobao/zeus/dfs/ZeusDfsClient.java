package com.taobao.zeus.dfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;

/**
 * 云梯HDFS客户端
 */
public class ZeusDfsClient {
    private static String hadoopxml;
    
    private static Map<String, String> defaultConfigs=new HashMap<String, String>();
    private Map<String, String> customConfigs=new HashMap<String, String>();

    public ZeusDfsClient() {
    	this(null);
    }
    
    static{
    	defaultConfigs.put("hadoop.rpc.socket.factory.class.default", "com.taobao.cmp.proxy.HadoopProxy");
    	defaultConfigs.put("proxy.hosts", "172.23.226.152:1080,172.24.160.65:1080,cmpgateway.taobao.org:9999");
    	DefaultDiamondManager manager=new DefaultDiamondManager("zeus","com.taobao.zeus.hadoop.conf", new ManagerListener() {
			public void receiveConfigInfo(String configInfo) {
				hadoopxml=configInfo;
			}
			public Executor getExecutor() {
				return null;
			}
		});
    	hadoopxml=manager.getAvailableConfigureInfomation(3000);
    }
    
    public ZeusDfsClient(Map<String, String> configs){
    	if(configs!=null){
    		customConfigs=new HashMap<String, String>(configs);
    	}
    	
    }
    
    private Configuration getDefaultConfiguration(){
    	Configuration conf=new Configuration();
    	InputStream is=new ByteArrayInputStream(hadoopxml.getBytes());
    	conf.addResource(is);
    	for(String key:defaultConfigs.keySet()){
    		conf.set(key, defaultConfigs.get(key));
    	}
    	for(String key:customConfigs.keySet()){
    		conf.set(key, customConfigs.get(key));
    	}
		return conf;
    }
    
    public void copyToLocal(String hdfsPath,String localPath) throws IOException{
    	FileSystem fileSystem=FileSystem.get(getDefaultConfiguration());
    	fileSystem.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
    }

}
