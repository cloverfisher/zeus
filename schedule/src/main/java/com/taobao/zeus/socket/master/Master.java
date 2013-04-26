package com.taobao.zeus.socket.master;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.zeus.broadcast.alarm.MailAlarm;
import com.taobao.zeus.broadcast.alarm.SMSAlarm;
import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.DebugHistory;
import com.taobao.zeus.model.FileDescriptor;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobHistory;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.model.JobStatus.TriggerType;
import com.taobao.zeus.model.Profile;
import com.taobao.zeus.schedule.mvc.AddJobListener;
import com.taobao.zeus.schedule.mvc.DebugInfoLog;
import com.taobao.zeus.schedule.mvc.DebugListener;
import com.taobao.zeus.schedule.mvc.JobController;
import com.taobao.zeus.schedule.mvc.JobFailListener;
import com.taobao.zeus.schedule.mvc.JobSuccessListener;
import com.taobao.zeus.schedule.mvc.ScheduleInfoLog;
import com.taobao.zeus.schedule.mvc.StopScheduleJobListener;
import com.taobao.zeus.schedule.mvc.ZeusJobException;
import com.taobao.zeus.schedule.mvc.event.DebugFailEvent;
import com.taobao.zeus.schedule.mvc.event.DebugSuccessEvent;
import com.taobao.zeus.schedule.mvc.event.Events;
import com.taobao.zeus.schedule.mvc.event.JobFailedEvent;
import com.taobao.zeus.schedule.mvc.event.JobSuccessEvent;
import com.taobao.zeus.socket.SocketLog;
import com.taobao.zeus.socket.master.MasterWorkerHolder.HeartBeatInfo;
import com.taobao.zeus.socket.master.reqresp.MasterExecuteJob;
import com.taobao.zeus.socket.protocol.Protocol.ExecuteKind;
import com.taobao.zeus.socket.protocol.Protocol.Response;
import com.taobao.zeus.socket.protocol.Protocol.Status;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.util.Environment;
import com.taobao.zeus.util.Tuple;

public class Master {

	private MasterContext context;
	private static Logger log = LoggerFactory.getLogger(Master.class);

	public Master(final MasterContext context) {
		this.context = context;
		GroupBean root = context.getGroupManager().getGlobeGroupBean();

		if (Environment.isPrePub()) {
			// 如果是预发环境，添加stop listener，阻止自动调度执行
			context.getDispatcher().addDispatcherListener(
					new StopScheduleJobListener());
		}
		context.getDispatcher().addDispatcherListener(
				new AddJobListener(context, this));
		context.getDispatcher().addDispatcherListener(
				new JobFailListener(context));
		context.getDispatcher().addDispatcherListener(
				new DebugListener(context));
		context.getDispatcher().addDispatcherListener(
				new JobSuccessListener(context));
		Map<String, JobBean> allJobBeans = root.getAllSubJobBeans();
		for (String id : allJobBeans.keySet()) {
			context.getDispatcher().addController(
					new JobController(context, this, id));
		}

		// 初始化
		context.getDispatcher().forwardEvent(Events.Initialize);
		context.setMaster(this);

		// 定时扫描等待队列
		context.getSchedulePool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					scan();
				} catch (Exception e) {
				}
			}
		}, 0, 3, TimeUnit.SECONDS);
		// 定时扫描worker channel，心跳超过1分钟没有连接就主动断掉
		context.getSchedulePool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				Date now = new Date();
				for (MasterWorkerHolder holder : new ArrayList<MasterWorkerHolder>(
						context.getWorkers().values())) {
					if (holder.getHeart().timestamp == null
							|| (now.getTime() - holder.getHeart().timestamp
									.getTime()) > 1000 * 60) {
						holder.getChannel().close();
					}
				}
			}
		}, 30, 30, TimeUnit.SECONDS);
	}

	private MasterWorkerHolder getRunableWorker() {
		MasterWorkerHolder selectWorker = null;
		Float selectMemRate = null;
		for (MasterWorkerHolder worker : context.getWorkers().values()) {
			HeartBeatInfo heart = worker.getHeart();
			if (heart != null && heart.memRate != null && heart.memRate < 0.8) {
				if (selectWorker == null) {
					selectWorker = worker;
					selectMemRate = heart.memRate;
				} else if (selectMemRate > heart.memRate) {
					selectWorker = worker;
					selectMemRate = heart.memRate;
				}
			}
		}
		return selectWorker;
	}

	private void scan() {
		if (!context.getQueue().isEmpty()
				|| !context.getManualQueue().isEmpty()
				|| !context.getDebugQueue().isEmpty()) {
			MasterWorkerHolder selectWorker = getRunableWorker();
			if (selectWorker != null) {
				if (!context.getQueue().isEmpty()) {// 调度任务
					runScheduleJob(selectWorker);
				} else if (!context.getManualQueue().isEmpty()) {
					runManualJob(selectWorker);
				} else if (!context.getDebugQueue().isEmpty()) {// 调试任务
					runDebugJob(selectWorker);
				}
			}
		}
		// 检测任务超时
		checkTimeOver();
	}

	private void runDebugJob(MasterWorkerHolder selectWorker) {
		final MasterWorkerHolder w = selectWorker;
		final String debugId = context.getDebugQueue().poll();
		SocketLog.info("master scan and poll debugId=" + debugId + " and run!");

		new Thread() {
			@Override
			public void run() {
				DebugHistory history = context.getDebugHistoryManager()
						.findDebugHistory(debugId);
				history.getLog().appendZeus(
						new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
								.format(new Date()) + " 开始运行");
				context.getDebugHistoryManager().updateDebugHistoryLog(debugId,
						history.getLog().getContent());
				Exception exception = null;
				Response resp = null;
				try {
					Future<Response> f = new MasterExecuteJob().executeJob(
							context, w, ExecuteKind.DebugKind, debugId);
					resp = f.get();
				} catch (Exception e) {
					exception = e;
					DebugInfoLog.error(
							String.format("debugId:%s run failed", debugId), e);
				}
				boolean success = resp.getStatus() == Status.OK ? true : false;

				if (!success) {
					// 运行失败，更新失败状态，发出失败消息
					if (exception != null) {
						exception = new ZeusException(String.format(
								"fileId:%s run failed ", history.getFileId()),
								exception);
					} else {
						exception = new ZeusException(String.format(
								"fileId:%s run failed ", history.getFileId()));
					}
					DebugInfoLog.info("debugId:" + debugId + " run fail ");
					history = context.getDebugHistoryManager()
							.findDebugHistory(debugId);
					DebugFailEvent jfe = new DebugFailEvent(
							history.getFileId(), history, exception);
					context.getDispatcher().forwardEvent(jfe);
				} else {
					// 运行成功，发出成功消息
					DebugInfoLog.info("debugId:" + debugId + " run success");
					DebugSuccessEvent dse = new DebugSuccessEvent(
							history.getFileId(), history);
					context.getDispatcher().forwardEvent(dse);
				}
			}
		}.start();
	}

	private void runManualJob(MasterWorkerHolder selectWorker) {
		final MasterWorkerHolder w = selectWorker;
		final String historyId = context.getManualQueue().poll();
		SocketLog.info("master scan and poll historyId=" + historyId
				+ " and run!");
		new Thread() {
			@Override
			public void run() {
				JobHistory history = context.getJobHistoryManager()
						.findJobHistory(historyId);
				history.getLog().appendZeus(
						new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
								.format(new Date()) + " 开始运行");
				context.getJobHistoryManager().updateJobHistoryLog(historyId,
						history.getLog().getContent());
				Exception exception = null;
				Response resp = null;
				try {
					Future<Response> f = new MasterExecuteJob().executeJob(
							context, w, ExecuteKind.ManualKind, historyId);
					resp = f.get();
				} catch (Exception e) {
					exception = e;
					ScheduleInfoLog.error("JobId:" + history.getJobId()
							+ " run failed", e);
				}
				boolean success = resp.getStatus() == Status.OK ? true : false;

				if (!success) {
					// 运行失败，更新失败状态，发出失败消息
					ZeusJobException jobException = null;
					if (exception != null) {
						jobException = new ZeusJobException(history.getJobId(),
								String.format("JobId:%s run failed ",
										history.getJobId()), exception);
					} else {
						jobException = new ZeusJobException(history.getJobId(),
								String.format("JobId:%s run failed ",
										history.getJobId()));
					}
					ScheduleInfoLog.info("jobId:" + history.getJobId()
							+ " run fail ");
					history = context.getJobHistoryManager().findJobHistory(
							historyId);
					JobFailedEvent jfe = new JobFailedEvent(history.getJobId(),
							history.getTriggerType(), history, jobException);
					context.getDispatcher().forwardEvent(jfe);
				} else {
					// 运行成功，发出成功消息
					ScheduleInfoLog.info("manual jobId::" + history.getJobId()
							+ " run success");
					JobSuccessEvent jse = new JobSuccessEvent(
							history.getJobId(), history.getTriggerType(),
							historyId);
					context.getDispatcher().forwardEvent(jse);
				}
			};
		}.start();
	}

	private void runScheduleJob(MasterWorkerHolder selectWorker) {
		final MasterWorkerHolder w = selectWorker;
		final String jobId = context.getQueue().poll();
		SocketLog.info("master scan and poll jobId=" + jobId + " and run!");
		new Thread() {
			@Override
			public void run() {
				JobHistory his = context.getJobHistoryManager().findJobHistory(
						context.getGroupManager().getJobStatus(jobId)
								.getHistoryId());
				TriggerType type = his.getTriggerType();
				ScheduleInfoLog.info("JobId:" + jobId + " run start");
				his.getLog().appendZeus(
						new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
								.format(new Date()) + " 开始运行");
				context.getJobHistoryManager().updateJobHistoryLog(his.getId(),
						his.getLog().getContent());
				Exception exception = null;
				Response resp = null;
				try {
					Future<Response> f = new MasterExecuteJob().executeJob(
							context, w, ExecuteKind.ScheduleKind, his.getId());
					resp = f.get();
				} catch (Exception e) {
					exception = e;
					ScheduleInfoLog.error(
							String.format("JobId:%s run failed", jobId), e);
				}
				boolean success = resp.getStatus() == Status.OK ? true : false;

				JobStatus jobstatus = context.getGroupManager().getJobStatus(
						jobId);
				jobstatus
						.setStatus(com.taobao.zeus.model.JobStatus.Status.WAIT);
				if (success
						&& (his.getTriggerType() == TriggerType.SCHEDULE || his
								.getTriggerType() == TriggerType.MANUAL_RECOVER)) {
					ScheduleInfoLog.info("JobId:" + jobId
							+ " clear ready dependency");
					jobstatus.setReadyDependency(new HashMap<String, String>());
				}
				context.getGroupManager().updateJobStatus(jobstatus);

				if (!success) {
					// 运行失败，更新失败状态，发出失败消息
					ZeusJobException jobException = null;
					if (exception != null) {
						jobException = new ZeusJobException(jobId,
								String.format("JobId:%s run failed ", jobId),
								exception);
					} else {
						jobException = new ZeusJobException(jobId,
								String.format("JobId:%s run failed ", jobId));
					}
					ScheduleInfoLog.info("JobId:" + jobId
							+ " run fail and dispatch the fail event");
					JobFailedEvent jfe = new JobFailedEvent(jobId, type,
							context.getJobHistoryManager().findJobHistory(
									his.getId()), jobException);
					context.getDispatcher().forwardEvent(jfe);
				} else {
					// 运行成功，发出成功消息
					ScheduleInfoLog.info("JobId:" + jobId
							+ " run success and dispatch the success event");
					JobSuccessEvent jse = new JobSuccessEvent(jobId,
							his.getTriggerType(), his.getId());
					context.getDispatcher().forwardEvent(jse);
				}
			}
		}.start();
	}

	/**
	 * 检查任务超时
	 */
	private void checkTimeOver() {
		for (MasterWorkerHolder w : context.getWorkers().values()) {
			checkScheduleTimeOver(w);
			checkManualTimeOver(w);
			checkDebugTimeOver(w);
		}
	}

	private void checkDebugTimeOver(MasterWorkerHolder w) {
		for (Map.Entry<String, Boolean> entry : w.getDebugRunnings().entrySet()) {
			if (entry.getValue() != null && entry.getValue()) {
				continue;
			}
			String historyId = entry.getKey();
			DebugHistory his = context.getDebugHistoryManager()
					.findDebugHistory(historyId);
			long maxTime;
			FileDescriptor fd;
			try {
				fd = context.getFileManager().getFile(his.getFileId());
				Profile pf = context.getProfileManager().findByUid(
						fd.getOwner());
				String maxTimeString = pf.getHadoopConf().get(
						"zeus.job.maxtime");
				if (maxTimeString == null || maxTimeString.trim().isEmpty()) {
					continue;
				}
				maxTime = Long.parseLong(maxTimeString);

				if (maxTime < 0) {
					continue;
				}
			} catch (Exception e) {
				continue;
			}
			long runTime = (System.currentTimeMillis() - his.getStartTime()
					.getTime()) / 1000 / 60;
			if (runTime > maxTime) {
				if (timeOverAlarm(null, fd, runTime, maxTime, 2)) {
					w.getDebugRunnings().replace(historyId, false, true);
				}
			}
		}
	}

	private void checkManualTimeOver(MasterWorkerHolder w) {
		for (Map.Entry<String, Boolean> entry : w.getManualRunnings()
				.entrySet()) {
			if (entry.getValue() != null && entry.getValue()) {
				continue;
			}
			String historyId = entry.getKey();
			JobHistory his = context.getJobHistoryManager().findJobHistory(
					historyId);
			long maxTime;
			try {
				JobDescriptor jd = context.getGroupManager()
						.getJobDescriptor(his.getJobId()).getX();
				String maxTimeString = jd.getProperties().get(
						"zeus.job.maxtime");
				if (maxTimeString == null || maxTimeString.trim().isEmpty()) {
					continue;
				}
				maxTime = Long.parseLong(maxTimeString);

				if (maxTime < 0) {
					continue;
				}
			} catch (Exception e) {
				continue;
			}
			long runTime = (System.currentTimeMillis() - his.getStartTime()
					.getTime()) / 1000 / 60;
			if (runTime > maxTime) {
				if (timeOverAlarm(his, null, runTime, maxTime, 1)) {
					w.getManualRunnings().replace(historyId, false, true);
				}
			}
		}
	}

	private void checkScheduleTimeOver(MasterWorkerHolder w) {
		for (Map.Entry<String, Boolean> entry : w.getRunnings().entrySet()) {
			if (entry.getValue() != null && entry.getValue()) {
				continue;
			}
			String jobId = entry.getKey();
			JobDescriptor jd = context.getGroupManager()
					.getJobDescriptor(jobId).getX();
			String maxTimeString = jd.getProperties().get("zeus.job.maxtime");
			long maxTime;
			try {
				if (maxTimeString == null || maxTimeString.trim().isEmpty()) {
					continue;
				}
				maxTime = Long.parseLong(maxTimeString);

				if (maxTime < 0) {
					continue;
				}
			} catch (Exception e) {
				continue;
			}

			JobHistory his = context.getJobHistoryManager().findJobHistory(
					context.getGroupManager().getJobStatus(jobId)
							.getHistoryId());
			long runTime = (System.currentTimeMillis() - his.getStartTime()
					.getTime()) / 1000 / 60;
			if (runTime > maxTime) {
				if (timeOverAlarm(his, null, runTime, maxTime, 0)) {
					w.getRunnings().replace(jobId, false, true);
				}
			}
		}
	}

	private boolean timeOverAlarm(final JobHistory his, FileDescriptor fd,
			long runTime, long maxTime, int type) {
		final MailAlarm mailAlarm = (MailAlarm) context.getApplicationContext()
				.getBean("mailAlarm");
		SMSAlarm smsAlarm = (SMSAlarm) context.getApplicationContext().getBean(
				"smsAlarm");

		final StringBuffer title = new StringBuffer("宙斯任务超时[");
		switch (type) {
		case 0:
			title.append("自动调度").append("] jobID=").append(his.getJobId());
			break;
		case 1:
			title.append("手动调度").append("] jobID=").append(his.getJobId());
			break;
		case 2:
			title.append("调试任务").append("] 脚本名称：").append(fd.getName());
		}
		final StringBuffer content = new StringBuffer(title);
		content.append("\n已经运行时间：").append(runTime).append("分钟")
				.append("\n设置最大运行时间：").append(maxTime).append("分钟")
				.append("\n详情请登录zeus系统查看：http://zeus.taobao.com:9999");
		try {
			if (type == 2) {
				//此处可以发送IM消息
			} else {
				//此处可以发送IM消息
				new Thread() {
					@Override
					public void run() {
						try {
							Thread.sleep(6000);
							mailAlarm.alarm(his.getId(), title.toString(),
									content.toString().replace("\n","<br/>").replace("http://zeus.taobao.com:9999", "<a href='http://zeus.taobao.com:9999'>http://zeus.taobao.com:9999</a>"));
						} catch (Exception e) {
							log.error("send run timeover mail alarm failed", e);
						}
					}
				}.start();
				if (type == 0) {
					Calendar now = Calendar.getInstance();
					int hour = now.get(Calendar.HOUR_OF_DAY);
					int day = now.get(Calendar.DAY_OF_WEEK);
					if (day == Calendar.SATURDAY || day == Calendar.SUNDAY
							|| hour < 9 || hour > 18) {
						smsAlarm.alarm(his.getId(), title.toString(),
								content.toString(), null);
					}
				}
			}
			return true;
		} catch (Exception e) {
			log.error("send run timeover alarm failed", e);
			return false;
		}
	}

	public void workerDisconnectProcess(Channel channel) {
		MasterWorkerHolder holder = context.getWorkers().get(channel);
		if (holder != null) {
			context.getWorkers().remove(channel);
			final List<JobHistory> hiss = new ArrayList<JobHistory>();
			Map<String, Tuple<JobDescriptor, JobStatus>> map = context
					.getGroupManager().getJobDescriptor(
							holder.getRunnings().keySet());
			for (String key : map.keySet()) {
				JobStatus js = map.get(key).getY();
				if (js.getHistoryId() != null) {
					hiss.add(context.getJobHistoryManager().findJobHistory(
							js.getHistoryId()));
				}
				js.setStatus(com.taobao.zeus.model.JobStatus.Status.FAILED);
				context.getGroupManager().updateJobStatus(js);
			}
			new Thread() {
				@Override
				public void run() {
					try {
						Thread.sleep(30000);
					} catch (InterruptedException e) {
					}
					for (JobHistory his : hiss) {
						String jobId = his.getJobId();
						JobHistory history = new JobHistory();
						history.setJobId(jobId);
						history.setTriggerType(his.getTriggerType());
						history.setIllustrate("worker断线，重新跑任务");
						history.setOperator(his.getOperator());
						context.getJobHistoryManager().addJobHistory(history);
						Master.this.run(history);
					}
				};
			}.start();

		}
	}

	public void debug(DebugHistory debug) {
		debug.setStatus(com.taobao.zeus.model.JobStatus.Status.RUNNING);
		debug.setStartTime(new Date());
		context.getDebugHistoryManager().updateDebugHistory(debug);
		debug.getLog().appendZeus(
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
						+ " 进入任务队列");
		context.getDebugHistoryManager().updateDebugHistoryLog(debug.getId(),
				debug.getLog().getContent());
		context.getDebugQueue().offer(debug.getId());
	}

	public JobHistory run(JobHistory history) {
		history.setStatus(com.taobao.zeus.model.JobStatus.Status.RUNNING);
		String jobId = history.getJobId();
		if (history.getTriggerType() == TriggerType.MANUAL_RECOVER) {
			for (String job : new ArrayList<String>(context.getQueue())) {
				if (job.equals(jobId)) {
					history.getLog().appendZeus("已经在队列中，无法再次运行");
					history.setStartTime(new Date());
					history.setEndTime(new Date());
					history.setStatus(com.taobao.zeus.model.JobStatus.Status.FAILED);
					break;
				}
			}
			for (Channel key : context.getWorkers().keySet()) {
				MasterWorkerHolder worker = context.getWorkers().get(key);
				if (worker.getRunnings().containsKey(jobId)) {
					history.getLog().appendZeus("已经在运行中，无法再次运行");
					history.setStartTime(new Date());
					history.setEndTime(new Date());
					history.setStatus(com.taobao.zeus.model.JobStatus.Status.FAILED);
					break;
				}
			}
		}

		if (history.getStatus() == com.taobao.zeus.model.JobStatus.Status.RUNNING) {
			history.getLog().appendZeus(
					new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
							.format(new Date()) + " 进入任务队列");
			context.getJobHistoryManager().updateJobHistoryLog(history.getId(),
					history.getLog().getContent());
			if (history.getTriggerType() == TriggerType.MANUAL) {
				context.getManualQueue().offer(history.getId());
			} else {
				JobStatus js = context.getGroupManager().getJobStatus(
						history.getJobId());
				js.setStatus(com.taobao.zeus.model.JobStatus.Status.RUNNING);
				js.setHistoryId(history.getId());
				context.getGroupManager().updateJobStatus(js);
				context.getQueue().offer(jobId);
			}
		}
		context.getJobHistoryManager().updateJobHistory(history);
		context.getJobHistoryManager().updateJobHistoryLog(history.getId(),
				history.getLog().getContent());
		return history;
	}
}
