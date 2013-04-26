package com.taobao.zeus.jobs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.zeus.jobs.sub.tool.CancelHadoopJob;
import com.taobao.zeus.store.HierarchyProperties;
/**
 * 通过操作系统创建进程Process的Job任务
 * @author zhoufang
 *
 */
public abstract class ProcessJob extends AbstractJob implements Job {
	
	protected volatile Process process;
	
	protected Map<String, String> envMap=new HashMap<String, String>();
	
	public static final int CLEAN_UP_TIME_MS = 1000;
	
	public ProcessJob(JobContext jobContext){
		super(jobContext);
	}

	
	public abstract List<String> getCommandList();
	
	public Integer run() throws Exception{
		
		int exitCode=-999;
		
		//设置环境变量
		for(String key:jobContext.getProperties().getAllProperties().keySet()){
			if(jobContext.getProperties().getProperty(key)!=null && (key.startsWith("instance.") || key.startsWith("secret."))){
				envMap.put(key, jobContext.getProperties().getProperty(key));
			}
		}
		envMap.put("HADOOP_CONF_DIR", ".:"+System.getenv("HADOOP_CONF_DIR"));
		envMap.put("HIVE_CONF_DIR", ".:"+System.getenv("HIVE_CONF_DIR"));
		
		List<String> commands=getCommandList();
		for(String s:commands){
			log("DEBUG Command:"+s);
			
			ProcessBuilder builder = new ProcessBuilder(partitionCommandLine(s));
			builder.directory(new File(jobContext.getWorkDir()));
			builder.environment().putAll(envMap);
			process=builder.start();
			final InputStream inputStream = process.getInputStream();
            final InputStream errorStream = process.getErrorStream();
			
			String threadName=null;
			if(jobContext.getJobHistory()!=null && jobContext.getJobHistory().getJobId()!=null){
				threadName="jobId="+jobContext.getJobHistory().getJobId();
			}else if(jobContext.getDebugHistory()!=null && jobContext.getDebugHistory().getId()!=null){
				threadName="debugId="+jobContext.getDebugHistory().getId();
			}else{
				threadName="not-normal-job";
			}
			new Thread(new Runnable() {
				@Override
				public void run() {
					try{
						BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream));
						String line;
						while((line=reader.readLine())!=null){
							logConsole(line);
						}
					}catch(Exception e){
						log(e);
						log("接收日志出错，推出日志接收");
					}
				}
			},threadName).start();
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						BufferedReader reader=new BufferedReader(new InputStreamReader(errorStream));
						String line;
						while((line=reader.readLine())!=null){
								logConsole(line);
							
						}
					} catch (Exception e) {
							log(e);
							log("接收日志出错，推出日志接收");
						}
				}
			},threadName).start();
			
			exitCode = -999;
			try {
				exitCode = process.waitFor();
			} catch (InterruptedException e) {
				log(e);
			} finally{
				process=null;
			}
			if(exitCode!=0){
				return exitCode;
			}
		}
		return exitCode;
	}
	
	public void cancel(){
		try {
			new CancelHadoopJob(jobContext).run();
		} catch (Exception e1) {
			log(e1);
		}
		//强制kill 进程
		if (process != null) {
			log("WARN Attempting to kill the process ");
			try {
				process.destroy();
				int pid=getProcessId();
				Runtime.getRuntime().exec("kill "+pid);
			} catch (Exception e) {
				log(e);
			} finally{
				process=null;
			}
		}
	}
	private int getProcessId() {
		int processId = 0;

		try {
			Field f = process.getClass().getDeclaredField("pid");
			f.setAccessible(true);

			processId = f.getInt(process);
		} catch (Throwable e) {
		}

		return processId;
	}

	
	protected String getProperty(String key,String defaultValue){
		String value=jobContext.getProperties().getProperty(key);
		if(value==null){
			value=defaultValue;
		}
		return value;
	}
	/**
	 * Splits the command into a unix like command line structure. Quotes and
	 * single quotes are treated as nested strings.
	 * 
	 * @param command
	 * @return
	 */
	public static String[] partitionCommandLine(String command) {
		
		ArrayList<String> commands = new ArrayList<String>();
		
		String os=System.getProperties().getProperty("os.name");
		if(os!=null && (os.startsWith("win") || os.startsWith("Win"))){
			commands.add("CMD.EXE");
			commands.add("/C");
			commands.add(command);
		}else{
			int index = 0;

	        StringBuffer buffer = new StringBuffer(command.length());

	        boolean isApos = false;
	        boolean isQuote = false;
	        while(index < command.length()) {
	            char c = command.charAt(index);

	            switch(c) {
	                case ' ':
	                    if(!isQuote && !isApos) {
	                        String arg = buffer.toString();
	                        buffer = new StringBuffer(command.length() - index);
	                        if(arg.length() > 0) {
	                            commands.add(arg);
	                        }
	                    } else {
	                        buffer.append(c);
	                    }
	                    break;
	                case '\'':
	                    if(!isQuote) {
	                        isApos = !isApos;
	                    } else {
	                        buffer.append(c);
	                    }
	                    break;
	                case '"':
	                    if(!isApos) {
	                        isQuote = !isQuote;
	                    } else {
	                        buffer.append(c);
	                    }
	                    break;
	                default:
	                    buffer.append(c);
	            }

	            index++;
	        }

	        if(buffer.length() > 0) {
	            String arg = buffer.toString();
	            commands.add(arg);
	        }
		}
        return commands.toArray(new String[commands.size()]);
	}

	public HierarchyProperties getProperties(){
		return jobContext.getProperties();
	}
	public JobContext getJobContext() {
		return jobContext;
	}
	
}
