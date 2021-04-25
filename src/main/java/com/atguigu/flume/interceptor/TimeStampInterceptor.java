package com.atguigu.flume.interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.alibaba.fastjson.JSONObject;

public class TimeStampInterceptor implements Interceptor {

	private ArrayList<Event> events = new ArrayList<>();
	
	@Override
	public void initialize() {
		// TODO Auto-generated method stub

	}

	@Override
	public Event intercept(Event event) {
		// 解析日志数据，取出时间
		//1.获取log
		byte[] body = event.getBody();
		String log = new String(body, StandardCharsets.UTF_8);
		//2.将log数据转换为json格式
		JSONObject jsonObject = JSONObject.parseObject(log);
		//3.从json中取出key为ts的value
		String ts = jsonObject.getString("ts");

		// 将取出的时间放入header
		Map<String, String> headers = event.getHeaders();
		headers.put("timestamp", ts); 

		return event;
	}

	@Override
	public List<Event> intercept(List<Event> list) {
		events.clear();
		for (Event event : list) {
			events.add(intercept(event));
		}
		return events;
	}
	
	public static class Builder implements Interceptor.Builder{

		@Override
		public void configure(Context arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Interceptor build() {
			return new TimeStampInterceptor();
		}
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
