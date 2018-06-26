/*
 * Copyright 2016-2018 flexibledata.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package io.flexibledata.pipeline.input.canal;

import java.util.List;

import org.slf4j.MDC;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;

import io.flexibledata.pipeline.event.Event;
import io.flexibledata.pipeline.input.canal.handler.CanalEventHandler;
import io.flexibledata.pipeline.mq.MessageQueue;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Canal数据监听线程
 * 
 * @author tan.jie
 *
 */
@Slf4j
@AllArgsConstructor
public class CanalSubscribeThread extends Thread {

	private CanalClient canalClient;
	private MessageQueue<Event> queue;
	private int SLEEP_TIME = 500;
	private int QUEUE_MAX_SIZE = 10000;

	@Override
	public void run() {
		process();
	}

	public void process() {
		MDC.put("destination", canalClient.getCanalConfig().getDestination());
		MDC.put("tableFilterRegex", canalClient.getCanalConfig().getTableFilterRegex());
		CanalConnector connector = canalClient.getConnector();
		CanalEventHandler eventHandler = new CanalEventHandler();
		while (true) {
			// 获取指定数量的数据
			Message message = connector.getWithoutAck(QUEUE_MAX_SIZE);
			long batchId = message.getId();
			int size = message.getEntries().size();
			if (batchId == -1 && size == 0) {
				try {
					Thread.sleep(SLEEP_TIME);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				List<Event> events = eventHandler.onEvent(message);
				for (Event each : events) {
					queue.push(each);
					log.debug(each.toString());
				}
			}
			connector.ack(batchId);
		}
	}

}
