package com.taobao.zeus.jobs.sub;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.taobao.zeus.jobs.JobContext;
import com.taobao.zeus.jobs.sub.conf.ConfUtil;
import com.taobao.zeus.jobs.sub.main.MapReduceMain;
import com.taobao.zeus.jobs.sub.tool.DownloadHdfsFileJob;
import com.taobao.zeus.store.HierarchyProperties;
import com.taobao.zeus.util.RunningJobKeys;

public class MapReduceJob extends JavaJob{

	
	
	public MapReduceJob(JobContext jobContext) {
		super(jobContext);
		String main=getJavaClass();
		String args=getMainArguments();
		String classpath=getClassPaths();
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_JAVA_MAIN_CLASS, "com.taobao.zeus.jobs.sub.main.MapReduceMain");
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_CLASSPATH, System.getenv("JOB_CLASSPATH")+File.pathSeparator+classpath+
				File.pathSeparator+getSourcePathFromClass(MapReduceMain.class));
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_JAVA_MAIN_ARGS, main+" "+args);
		
	}
	
	@Override
	public Integer run() throws Exception {
		jobContext.getProperties().setProperty("instance.workDir", jobContext.getWorkDir());
		Configuration conf=ConfUtil.getDefaultConf();
		if(new File(jobContext.getWorkDir()+File.separator+"hadoop-site.xml").exists()){
			conf.addResource(new Path(jobContext.getWorkDir()+File.separator+"hadoop-site.xml"));
		}
		conf.size();//通过此方法来让conf加载资源文件
		Map<String, String> prop=jobContext.getProperties().getAllProperties();
		for(String key:prop.keySet()){
			if(key.startsWith("hadoop.")){
				conf.set(key.substring("hadoop.".length()), prop.get(key));
			}
		}
		List<Map<String, String>> resources=jobContext.getResources();
		if(resources!=null && !resources.isEmpty()){
			StringBuffer sb=new StringBuffer();
			for(Map<String, String> map:jobContext.getResources()){
				if(map.get("uri")!=null){
					String uri=map.get("uri");
					if(uri.startsWith("hdfs://") && uri.endsWith(".jar")){
						sb.append(uri.substring("hdfs://".length())).append(",");
					}
				}
			}
			conf.set("tmpjars", sb.toString().substring(0, sb.toString().length()-1));
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
		return super.run();
	}

	public static void main(String[] args) {
		JobContext context=JobContext.getTempJobContext();
		Map<String, String> map=new HashMap<String, String>();
		map.put("hadoop.ugi.name", "uginame");
		HierarchyProperties properties=new HierarchyProperties(map);
		context.setProperties(properties);
		
		new MapReduceJob(context);
	}

}
