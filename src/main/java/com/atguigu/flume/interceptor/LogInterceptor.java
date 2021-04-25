package com.atguigu.flume.interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class LogInterceptor implements Interceptor {

	public void initialize() {
		// TODO Auto-generated method stub

	}

	public Event intercept(Event event) {
		byte[] body = event.getBody();
		String log = new String(body, StandardCharsets.UTF_8);
		if (JSONUtils.isJSONValidate(log)) {
			return event;
		}

		return null;
	}

	public List<Event> intercept(List<Event> list) {
		Iterator<Event> iterator = list.iterator();
		while (iterator.hasNext()) {
			Event next = iterator.next();
			if (intercept(next) == null) {
				iterator.remove();
			}
		}

		return list;
	}

	public static class Builder implements Interceptor.Builder {

		public void configure(Context arg0) {
			// TODO Auto-generated method stub

		}

		public Interceptor build() {
			LogInterceptor interceptor = new LogInterceptor();
			return interceptor;
		}

	}

	public void close() {
		// TODO Auto-generated method stub

	}

}
