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

import io.flexibledata.pipeline.event.Event;
import io.flexibledata.pipeline.mq.MessageQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * Canal事件监听服务
 * 
 * @author tan.jie
 *
 */
@Slf4j
public class CanalSubscribeService {
	public void subscribe(CanalClient canalClient, MessageQueue<Event> queue, Integer sleepTime, Integer fetchSize) {
		CanalSubscribeThread canalDataListenerThread = new CanalSubscribeThread(canalClient, queue, sleepTime, fetchSize);
		canalDataListenerThread.setName("CanalListenerThread");
		log.info("Started CanalListenerThread.");
		canalDataListenerThread.start();
	}
}
