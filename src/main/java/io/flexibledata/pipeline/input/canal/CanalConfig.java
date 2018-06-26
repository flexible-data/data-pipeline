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

import java.net.InetSocketAddress;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Canal连接配置
 * 
 * @author tan.jie
 *
 */
@Getter
@AllArgsConstructor
public class CanalConfig {
	private Boolean cluster;
	private String destination;
	private String connectUrl;
	private String tableFilterRegex;

	public CanalConnector getConnector() {
		if (!cluster) {
			String[] hostAndPort = connectUrl.split(":");
			return CanalConnectors.newSingleConnector(new InetSocketAddress(hostAndPort[0], Integer.valueOf(hostAndPort[1])), destination, "", "");
		} else {
			return CanalConnectors.newClusterConnector(connectUrl, destination, "", "");
		}
	}

}
