package com.taobao.zeus.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class YuntiJobDetailServlet extends HttpServlet{

	
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		String job=req.getParameter("jobId");
		String str="http://10.249.54.103:50030/jobdetails.jsp?jobid="+job+"&refresh=30";
		URL url=new URL(str);
		HttpURLConnection httpUrlCon=(HttpURLConnection)url.openConnection();
		InputStreamReader inRead=new InputStreamReader(httpUrlCon.getInputStream(),"gbk");
		BufferedReader bufRead=new BufferedReader(inRead);
		StringBuffer strBuf=new StringBuffer();
		String line="";
		while((line=bufRead.readLine())!=null){
			strBuf.append(line);
		}
		String html=strBuf.toString();
		if(html.length()>0){
			String need=html.substring(html.indexOf("<table "),html.indexOf("<p/>"));
			need=need.replaceAll("href=\"", "target='_blank' href=\"http://10.249.54.103:50030/");
			need=need.replace("Kind", "<a target='_blank' href='"+str+"'>Job</a>");
			resp.getWriter().print("<html><head><title>Yunti Job Detail</title>" +
					"<link rel='stylesheet' type='text/css' href='http://10.249.54.103:50030/static/hadoop.css'></head><body style='margin:0px;overflow-y:hidden'>" +
					""+need+"</body></html>");
		}
	}
}
