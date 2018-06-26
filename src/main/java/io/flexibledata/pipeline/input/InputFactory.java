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
package io.flexibledata.pipeline.input;

import io.flexibledata.pipeline.config.RegistryVo;
import io.flexibledata.pipeline.event.Event;
import io.flexibledata.pipeline.input.canal.CanalClient;
import io.flexibledata.pipeline.input.canal.CanalConfig;
import io.flexibledata.pipeline.input.canal.CanalSubscribeService;
import io.flexibledata.pipeline.mq.MessageQueue;

/**
 * @author tan.jie
 *
 */
public class InputFactory {

	public void createInputService(final RegistryVo registryVo, final MessageQueue<Event> queue) {
		switch (registryVo.getType().toLowerCase()) {
		case "canal":
			CanalConfig canalConfig = new CanalConfig(registryVo.getCluster(), registryVo.getDestination(), registryVo.getConnectUrl(), registryVo.getTableFilterRegex());
			CanalClient canalClient = new CanalClient(canalConfig);
			new CanalSubscribeService().subscribe(canalClient, queue, registryVo.getSleepTime(), registryVo.getFetchSize());
			break;
		case "kafka":
			break;
		default:
			break;
		}
	}
}
