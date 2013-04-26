package com.taobao.zeus.jobs.sub.conf;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.taobao.zeus.jobs.JobContext;

public class ConfUtil {
	
	private static String hiveLog4j="";

	public static String getHiveLog4j() {
		return hiveLog4j;
	}

	private static String defaultHadoopXml="";
	public static String getDefaultHadoopXml() {
		return defaultHadoopXml;
	}

	public static String getDefaultHiveXml() {
		return defaultHiveXml;
	}

	private static String defaultHiveXml="";

	static {
		try {
			URL hadoop=ConfUtil.class.getClassLoader().getResource("templates/hadoop-site.xml");
			List<String> hadoopLines=Files.readLines(new File(hadoop.getFile()), Charset.forName("utf-8"));
			defaultHadoopXml="";
			for(String s:hadoopLines){
				defaultHadoopXml+=s;
			}
			
			URL hive=ConfUtil.class.getClassLoader().getResource("templates/hive-site.xml");
			List<String> hiveLines=Files.readLines(new File(hive.getFile()), Charset.forName("utf-8"));
			defaultHiveXml="";
			for(String s:hiveLines){
				defaultHiveXml+=s;
			}
			
		} catch (IOException e) {
			LoggerFactory.getLogger(ConfUtil.class).error("error get default hadoop-site.xml",e);
		}
		
		try {
			File f=new File(ConfUtil.class.getClassLoader().getResource("templates/hive-log4j.properties").getPath());
			List<String> lines=Files.readLines(f, Charset.forName("utf-8"));
			StringBuffer sb=new StringBuffer();
			for(String s:lines){
				sb.append(s+"\n");
			}
			hiveLog4j=sb.toString();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Configuration getWorkConf(JobContext jobContext) {
		String workDir = jobContext.getWorkDir();
		Configuration conf = new Configuration();
		File hadoopSite = new File(workDir + File.separator + "hadoop-site.xml");
		if (hadoopSite.exists()) {
			conf.addResource(new Path(workDir + File.separator
					+ "hadoop-site.xml"));
		}
		File hiveSite = new File(workDir + File.separator + "hive-site.xml");
		if (hiveSite.exists()) {
			conf.addResource(new Path(workDir + File.separator + "hive-site.xml"));
		}
		return conf;
	}

	public static Configuration getDefaultConf() {
		Configuration conf = new Configuration();
		conf.addResource(new ByteArrayInputStream(defaultHadoopXml.getBytes()));
		conf.addResource(new ByteArrayInputStream(defaultHiveXml.getBytes()));
		return conf;
	}



	public static Configuration getDefaultZeusHadoopConf() {
		Configuration conf = new Configuration();
		conf.addResource(new ByteArrayInputStream(defaultHadoopXml.getBytes()));
		return conf;
	}

	public static Configuration getDefaultZeusHiveConf() {
		Configuration conf = new Configuration();
		conf.addResource(new ByteArrayInputStream(defaultHiveXml.getBytes()));
		return conf;
	}
}
