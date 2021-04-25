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
		// ������־���ݣ�ȡ��ʱ��
		//1.��ȡlog
		byte[] body = event.getBody();
		String log = new String(body, StandardCharsets.UTF_8);
		//2.��log����ת��Ϊjson��ʽ
		JSONObject jsonObject = JSONObject.parseObject(log);
		//3.��json��ȡ��keyΪts��value
		String ts = jsonObject.getString("ts");

		// ��ȡ����ʱ�����header
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
