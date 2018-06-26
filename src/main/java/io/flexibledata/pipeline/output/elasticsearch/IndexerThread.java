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
package io.flexibledata.pipeline.output.elasticsearch;

import io.flexibledata.pipeline.event.Event;
import io.flexibledata.pipeline.event.EventType;
import io.flexibledata.pipeline.mq.MessageQueue;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 索引构建线程
 * 
 * @author tan.jie
 *
 */
@Slf4j
@AllArgsConstructor
public class IndexerThread extends Thread {
	private MessageQueue<Event> queue;
	private ESTemplate esTemplate;

	@Override
	public void run() {
		while (true) {
			Event event = queue.pop();
			EventType eventType = event.getEventType();
			switch (eventType.getName().toLowerCase()) {
			case "insert":
				handleInsertEvent(event);
				break;
			case "update":
				handleUpdateEvent(event);
				break;
			case "delete":
				handleDeleteEvent(event);
				break;
			default:
				log.warn("数据库事件变更类型有误！");
			}
		}
	}

	private void handleInsertEvent(Event event) {
		String indexName = event.getSchemaName();
		String typeName = event.getTableName();
		String docId = event.getSource().get("id").toString();
		log.debug("Index a document. index is {}, type is {}, docId is , event is {} ", indexName, typeName, docId, event);
		esTemplate.indexDoc(indexName, typeName, docId, event.getSource());
	}

	private void handleUpdateEvent(Event event) {
		handleUpdateEvent(event);
	}

	private void handleDeleteEvent(Event event) {
		// 暂时不做处理
	}
}
