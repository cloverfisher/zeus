package com.taobao.zeus.store.mysql.persistence;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
@Entity(name="zeus_job")
public class JobPersistence implements Serializable {
	private static final long serialVersionUID = 1L;
	@Id
	@GeneratedValue 
	private Long id;
	/**
	 * 是否开启调度
	 * 1:true
	 * 0:false
	 */
	@Column
	private Integer auto=0;
	/**
	 * 1:独立Job
	 * 2：有依赖的Job
	 */
	@Column(name="schedule_type")
	private Integer scheduleType;
	/**
	 * 运行的类型，比如Shell， Hive Mapreduce
	 */
	@Column(name="run_type")
	private String runType;
	@Column
	private String configs;
	@Column(name="cron_expression")
	private String cronExpression;
	@Column
	private String dependencies;
	
	@Column(nullable=false)
	private String name;
	@Column
	private String descr;
	@Column(name="group_id",nullable=false)
	private Integer groupId;
	@Column(nullable=false)
	private String owner;
	@Column
	private String resources;
	@Column
	private String script;
	@Column(name="gmt_create",nullable=false)
	private Date gmtCreate=new Date();
	@Column(name="gmt_modified",nullable=false)
	private Date gmtModified=new Date();
	@Column(name="history_id")
	private Long historyId;
	@Column
	private String status;
	@Column(name="ready_dependency")
	private String readyDependency;
	@Column(name="pre_processers")
	private String preProcessers;
	@Column(name="post_processers")
	private String postProcessers;
	
	public String getConfigs() {
		return configs;
	}
	public void setConfigs(String configs) {
		this.configs = configs;
	}
	public String getCronExpression() {
		return cronExpression;
	}
	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}
	public String getDependencies() {
		return dependencies;
	}
	public void setDependencies(String dependencies) {
		this.dependencies = dependencies;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public String getDescr() {
		return descr;
	}
	public void setDescr(String descr) {
		this.descr = descr;
	}
	public Integer getScheduleType() {
		return scheduleType;
	}
	public void setScheduleType(Integer scheduleType) {
		this.scheduleType = scheduleType;
	}
	public Date getGmtCreate() {
		return gmtCreate;
	}
	public void setGmtCreate(Date gmtCreate) {
		this.gmtCreate = gmtCreate;
	}
	public Date getGmtModified() {
		return gmtModified;
	}
	public void setGmtModified(Date gmtModified) {
		this.gmtModified = gmtModified;
	}
	public String getRunType() {
		return runType;
	}
	public void setRunType(String runType) {
		this.runType = runType;
	}
	public String getResources() {
		return resources;
	}
	public void setResources(String resources) {
		this.resources = resources;
	}
	public Integer getAuto() {
		return auto;
	}
	public void setAuto(Integer auto) {
		this.auto = auto;
	}
	public Integer getGroupId() {
		return groupId;
	}
	public void setGroupId(Integer groupId) {
		this.groupId = groupId;
	}
	public String getScript() {
		return script;
	}
	public void setScript(String script) {
		this.script = script;
	}
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getReadyDependency() {
		return readyDependency;
	}
	public void setReadyDependency(String readyDependency) {
		this.readyDependency = readyDependency;
	}
	public String getPreProcessers() {
		return preProcessers;
	}
	public void setPreProcessers(String preProcessers) {
		this.preProcessers = preProcessers;
	}
	public String getPostProcessers() {
		return postProcessers;
	}
	public void setPostProcessers(String postProcessers) {
		this.postProcessers = postProcessers;
	}
	public Long getHistoryId() {
		return historyId;
	}
	public void setHistoryId(Long historyId) {
		this.historyId = historyId;
	}
}
