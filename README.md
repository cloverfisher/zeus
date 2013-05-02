维护者：<a href="http://github.com/xuhengfei" target="_blank">xuhengfei</a>  
###宙斯(zeus)是什么
宙斯是一个完整的Hadoop的作业平台  
从Hadoop任务的调试运行到生产任务的周期调度
宙斯支持任务的整个生命周期

从功能上来说，支持：  
Hadoop MapReduce任务的调试运行  
Hive任务的调试运行  
Shell任务的运行    
Hive元数据的可视化查询与数据预览  
Hadoop任务的自动调度  
完整的文档管理  


 
####宙斯开源，不仅仅是开源技术，更是开源产品  
开发中心，一个文档管理，开发调试的环境，在任务上线前的主要工作区域  
<a href="http://xuhengfei.github.io/assets/images/articles/zeus/snapshot-dev.png" target="_blank"><img src="http://xuhengfei.github.io/assets/images/articles/zeus/snapshot-dev.png" /></a>
调度中心，生产任务的调度环境，当任务调试通过后，在此处配置调度信息进行生产调度  
<a href="http://xuhengfei.github.io/assets/images/articles/zeus/snapshot-schedule.png" target="_blank"><img src="http://xuhengfei.github.io/assets/images/articles/zeus/snapshot-schedule.png" /></a>  

###宙斯运行原理
<a href="http://xuhengfei.github.io/assets/images/articles/zeus/snapshot-dev.png" target="_blank"><img src="http://xuhengfei.github.io/assets/images/articles/zeus/graph-network.png" /></a>  

<a href="http://xuhengfei.github.io/assets/images/articles/zeus/snapshot-dev.png" target="_blank"><img src="http://xuhengfei.github.io/assets/images/articles/zeus/graph-struct.png" /></a>  

<a href="http://xuhengfei.github.io/assets/images/articles/zeus/snapshot-dev.png" target="_blank"><img src="http://xuhengfei.github.io/assets/images/articles/zeus/graph-workflow.png" /></a>  

###使用指南    
快速启动(Quick Start)：  
1.设置配置项  
在/web/src/main/filter/antx.properties 中对配置项进行设置  
设置完成后，复制到${user.home}/antx.properties处  
2.pom.xml本地jar地址修改  
在/web/pom.xml中修改properties中的local.highcharts  
因为此jar不在maven仓库中，此jar已经在/web/libs/highcharts-1.4.0.jar  
将systemPath路径设置为绝对路径  
3.数据库配置  
zeus数据库:/web/src/main/resources/persistence.xml中对数据库进行配置  
hive元数据库:/web/src/main/resources/templates/hive-site.xml中对Hive metastore数据库进行配置  
4.打包  
mvn package   
打包在/web/target/exploded/zeus-web.war下  
使用tomcat之类容器运行即可  


以上步骤可以保证这个web项目正常启动，如果需要正式上线此项目，还需要配置以下内容：  
1.动态模板配置  
宙斯系统中有很多模板是可以动态修改的，包括以下一些，建议在正式运行之前都配置好  
首页展示内容 启动后参见页面指南  
首页通知内容 启动后参见页面指南  
hive 默认udf函数 com.taobao.zeus.jobs.sub.HiveJob实现TODO内容  

2.登陆系统  
宙斯不包含单独的注册系统  
建议使用单点登陆来实现登陆  
大致原理：   
(1) web.xml添加一个filter，用来跳转到单点登陆系统  
(2) Spring容器中添加一个Bean，实现com.taobao.zeus.web.Login.Filter.SSOLogin接口  

3.配置hadoop相关环境
默认的hadoop-site.xml和hive-site.xml在 /web/src/main/resources/templates下  
修改相应的配置以对应相应的hadoop集群    

4.超级管理员配置
在com.taobao.zeus.store.Super中进行配置