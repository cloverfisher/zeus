package com.taobao.zeus.jobs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.zeus.model.DebugHistory;
import com.taobao.zeus.model.JobHistory;
import com.taobao.zeus.store.HierarchyProperties;

/**
 * Job上下文
 * 当存在多个Job顺序处理时，通过上下文莱传递Job状态与信息
 * @author zhoufang
 *
 */
public class JobContext {
	
	public static JobContext getTempJobContext(){
		JobContext jobContext=new JobContext();
		JobHistory history=new JobHistory();
		jobContext.setJobHistory(history);
		jobContext.setWorkDir("/tmp");
		jobContext.setProperties(new HierarchyProperties(new HashMap<String, String>()));
		return jobContext;
	}
	
	public static final int SCHEDULE_RUN=1;
	public static final int MANUAL_RUN=2;
	public static final int DEBUG_RUN=3;
	
	private final int runType;

	private Map<String, Object> data=new HashMap<String, Object>();
	
	private Integer preExitCode;
	private Integer coreExitCode;
	
	private String workDir;
	
	private HierarchyProperties properties;
	private List<Map<String, String>> resources;
	
	private JobHistory jobHistory;
	
	private DebugHistory debugHistory;
	
	public JobContext(){
		this(MANUAL_RUN);
	}
	
	public JobContext(int runType){
		this.runType=runType;
	}
	
	public Object getData(String key){
		return data.get(key);
	}
	public void putData(String key,Object d){
		data.put(key, d);
	}
	public Integer getPreExitCode() {
		return preExitCode;
	}
	public void setPreExitCode(Integer preExitCode) {
		this.preExitCode = preExitCode;
	}
	public Integer getCoreExitCode() {
		return coreExitCode;
	}
	public void setCoreExitCode(Integer coreExitCode) {
		this.coreExitCode = coreExitCode;
	}
	public String getWorkDir() {
		return workDir;
	}
	public void setWorkDir(String workDir) {
		this.workDir = workDir;
	}
	public HierarchyProperties getProperties() {
		return properties;
	}
	public void setProperties(HierarchyProperties properties) {
		this.properties = properties;
	}
	public JobHistory getJobHistory() {
		return jobHistory;
	}
	public void setJobHistory(JobHistory jobHistory) {
		this.jobHistory = jobHistory;
	}
	public List<Map<String, String>> getResources() {
		return resources;
	}
	public void setResources(List<Map<String, String>> resources) {
		this.resources = resources;
	}
	public DebugHistory getDebugHistory() {
		return debugHistory;
	}
	public void setDebugHistory(DebugHistory debugHistory) {
		this.debugHistory = debugHistory;
	}

	public int getRunType() {
		return runType;
	}
	
}
