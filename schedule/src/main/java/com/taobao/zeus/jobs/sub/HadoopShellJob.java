package com.taobao.zeus.jobs.sub;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.taobao.zeus.jobs.JobContext;
import com.taobao.zeus.jobs.sub.conf.ConfUtil;

public class HadoopShellJob extends ShellJob{

	public HadoopShellJob(JobContext jobContext) {
		super(jobContext);
	}
	
	@Override
	public Integer run() throws Exception {
		Configuration conf=ConfUtil.getDefaultConf();
		if(new File(jobContext.getWorkDir()+File.separator+"hadoop-site.xml").exists()){
			conf.addResource(new Path(jobContext.getWorkDir()+File.separator+"hadoop-site.xml"));
		}
		if(new File(jobContext.getWorkDir()+File.separator+"hive-site.xml").exists()){
			conf.addResource(new Path(jobContext.getWorkDir()+File.separator+"hive-site.xml"));
		}
		conf.size();//通过此方法来让conf加载资源文件
		Map<String, String> all=jobContext.getProperties().getAllProperties();
		for(String key:all.keySet()){
			if(key.startsWith("hadoop.")){
				conf.set(key.substring("hadoop.".length()), all.get(key));
			}
		}
		try {
			File f=new File(jobContext.getWorkDir()+File.separator+"hadoop-site.xml");
			if(f.exists()){
				f.delete();
			}
			FileOutputStream fos=new FileOutputStream(f);
			conf.writeXml(fos);
			fos.close();
		} catch (Exception e) {
			log(e);
		}
		
		try{
			File f=new File(jobContext.getWorkDir()+File.separator+"hive-site.xml");
			if(f.exists()){
				f.delete();
			}
			FileOutputStream fos=new FileOutputStream(f);
			conf.writeXml(fos);
			fos.close();
		}catch(Exception e){
			log(e);
		}
		
		Properties log4j=new Properties();
		ByteArrayInputStream log4jIO=new ByteArrayInputStream(ConfUtil.getHiveLog4j().getBytes());
		try {
			log4j.load(log4jIO);
		}finally{
			log4jIO.close();
		}
		File localLog4j=new File(jobContext.getWorkDir()+File.separator+"hive-log4j.properties");
		if(localLog4j.exists()){
			log4j.load(new FileInputStream(new File(jobContext.getWorkDir()+File.separator+"hive-log4j.properties")));
			localLog4j.delete();
		}
		log4j.store(new FileOutputStream(localLog4j), "zeus generate hive-log4j.properties");
		
		return super.run();
	}
}
