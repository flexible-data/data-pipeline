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
package io.flexibledata.pipeline.filter;

import java.util.ArrayList;
import java.util.List;

import io.flexibledata.pipeline.event.Event;
import io.flexibledata.pipeline.mq.MessageQueue;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 过滤器线程
 * 
 * @author tan.jie
 *
 */
@Slf4j
@AllArgsConstructor
public class FilterThread extends Thread {
	private Integer bulkTakeSize;
	private MessageQueue<Event> inputToFilterQueue;
	private MessageQueue<Event> filterToOutputQueue;
	private EnrichFilter enrichFilter;

	@Override
	public void run() {
		process();
	}

	public void process() {
		log.info("Stared FilterService.");
		while (true) {
			List<Event> fromQueue = takeFromQueue();
			for (Event each : fromQueue) {
				Event event = enrichFilter.filter(each);
				filterToOutputQueue.push(event);
				log.debug("Put event {} to filterToOutputQueue.", event);
			}
		}
	}

	public List<Event> takeFromQueue() {
		List<Event> result = new ArrayList<>();
		inputToFilterQueue.bulkTake(result, bulkTakeSize);
		return result;
	}
}
