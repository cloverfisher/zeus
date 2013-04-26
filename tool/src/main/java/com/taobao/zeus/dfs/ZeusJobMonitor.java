package com.taobao.zeus.dfs;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.sf.json.JSONObject;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.zeus.client.crud.ZeusJobService;

public class ZeusJobMonitor {
	
	private final String topic="zeus-topic"; 
	private Map<String, List<JobListener>> jobListenerMap=new HashMap<String, List<JobListener>>();
	private Map<String, List<JobInstanceListener>> jobInstanceListenerMap=new HashMap<String, List<JobInstanceListener>>();
	private ExecutorService executors=Executors.newCachedThreadPool();
	private Date startupTime=new Date();
	
	/**
	 * 传入组名
	 * 请确保该组名的唯一性，保证你的组名不会与其他应用的组名重复,建议加应用名作为前缀
	 * @param group
	 * @throws Exception
	 */
	public ZeusJobMonitor(String group,ZeusJobService jobService) throws Exception{
        MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(new MetaClientConfig());
        MessageConsumer consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

            public void recieveMessages(Message message) {
            	try {
					JSONObject content=JSONObject.fromObject(new String(message.getData()));
					final String jobId=content.getString("id");
					final String historyId=content.getString("historyId");
					Date date=new Date(content.getLong("time"));
					Boolean success=content.getBoolean("status");
					if(date.after(startupTime) && jobListenerMap.get(jobId)!=null){
						if(success){
							for(final JobListener lis:jobListenerMap.get(jobId)){
								executors.submit(new Runnable() {
									public void run() {
										lis.onSuccess(jobId);
									}
								});
							}
						}else{
							for(final JobListener lis:jobListenerMap.get(jobId)){
								executors.submit(new Runnable() {
									public void run() {
										lis.onFailure(jobId);
									}
								});
							}
						}
					}
					
					final String instanceId=gen(jobId, historyId);
					if(date.after(startupTime) && jobInstanceListenerMap.get(instanceId)!=null){
						if(success){
							List<JobInstanceListener> list=jobInstanceListenerMap.get(instanceId);
							synchronized (this) {
								jobInstanceListenerMap.remove(instanceId);
							}
							for(final JobInstanceListener lis:list){
								executors.submit(new Runnable() {
									public void run() {
										lis.onSuccess(jobId,historyId);
									}
								});
							}
						}else{
							List<JobInstanceListener> list=jobInstanceListenerMap.get(instanceId);
							synchronized (this) {
								jobInstanceListenerMap.remove(instanceId);
							}
							for(final JobInstanceListener lis:list){
								executors.submit(new Runnable() {
									public void run() {
										lis.onFailure(jobId,historyId);
									}
								});
							}
						}
					}
					
				} catch (Exception e) {
					e.printStackTrace();
				}
            }
            public Executor getExecutor() {
                return executors;
            }
        });
        consumer.completeSubscribe();
	}
	/**
	 * 添加任务执行成功的监听
	 * @param jobId
	 * @param runnable
	 */
	public synchronized void addJobListener(String jobId,JobListener listener){
		List<JobListener> list=jobListenerMap.get(jobId);
		if(list==null){
			list=new ArrayList<JobListener>();
			jobListenerMap.put(jobId, list);
		}
		list.add(listener);
	}
	
	public synchronized void removeJobListener(String jobId,JobListener listener){
		List<JobListener> list=jobListenerMap.get(jobId);
		if(list!=null){
			list.remove(listener);
		}
	}
	
	public synchronized void addJobInstanceListener(String jobId,String historyId,JobInstanceListener listener){
		List<JobInstanceListener> list=jobInstanceListenerMap.get(gen(jobId, historyId));
		if(list==null){
			list=new ArrayList<JobInstanceListener>();
			jobInstanceListenerMap.put(gen(jobId, historyId), list);
		}
		list.add(listener);
	}
	private String gen(String jobId,String historyId){
		return jobId+"-"+historyId;
	}
	public interface JobListener{
		public void onSuccess(String jobId);
		public void onFailure(String jobId);
	}
	public interface JobInstanceListener{
		public void onSuccess(String jobId,String historyId);
		public void onFailure(String jobId,String historyId);
	}

}
