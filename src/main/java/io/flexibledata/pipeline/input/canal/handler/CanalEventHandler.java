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
package io.flexibledata.pipeline.input.canal.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;

import io.flexibledata.pipeline.event.Event;

/**
 * 事件处理器
 * 
 * @author tan.jie
 *
 */
public class CanalEventHandler {

	public List<Event> onEvent(Message message) {
		List<Event> result = new ArrayList<>();
		for (Entry entry : message.getEntries()) {
			if (entry.getEntryType() == EntryType.ROWDATA) {
				return handlerRowData(entry);
			}
		}
		return result;
	}

	/**
	 * 处理Canal中的Entry数据
	 * 
	 * @param entry
	 * @return
	 */
	private List<Event> handlerRowData(Entry entry) {
		RowChange rowChage = null;
		try {
			rowChage = RowChange.parseFrom(entry.getStoreValue());
		} catch (Exception e) {
			throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
		}

		EventType eventType = rowChage.getEventType();
		if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
			return null;
		}

		String schemaName = entry.getHeader().getSchemaName();
		String tableName = entry.getHeader().getTableName();

		List<Event> events = new ArrayList<>(rowChage.getRowDatasList().size());
		for (RowData rowData : rowChage.getRowDatasList()) {
			Event event = null;
			if (eventType == EventType.DELETE) {
				event = handleData(rowData, schemaName, tableName, io.flexibledata.pipeline.event.EventType.DELETE);
			} else if (eventType == EventType.INSERT) {
				event = handleData(rowData, schemaName, tableName, io.flexibledata.pipeline.event.EventType.INSERT);
			} else if (eventType == EventType.UPDATE) {
				event = handleData(rowData, schemaName, tableName, io.flexibledata.pipeline.event.EventType.UPDATE);
			}
			if (null != event) {
				events.add(event);
			}
		}
		return events;
	}

	/**
	 * 处理数据
	 * 
	 * @param rowData
	 * @param schemaName
	 * @param tableName
	 * @param eventType
	 * @return
	 */
	private Event handleData(RowData rowData, String schemaName, String tableName, io.flexibledata.pipeline.event.EventType eventType) {
		Event result = new Event();
		result.setSchemaName(schemaName);
		result.setTableName(tableName);
		result.setEventType(eventType);
		Map<String, Object> source = extractSource(rowData);
		result.setSource(source);
		result.setId(String.valueOf(source.get("id")));
		return result;
	}

	/**
	 * 提取数据
	 * 
	 * @param rowData
	 * @return
	 */
	private Map<String, Object> extractSource(RowData rowData) {
		Map<String, Object> result = new HashMap<String, Object>();
		for (CanalEntry.Column each : rowData.getAfterColumnsList()) {
			String name = each.getName();
			String value = each.getValue();
			result.put(name, value);
		}
		return result;
	}

}
