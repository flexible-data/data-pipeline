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

import java.net.UnknownHostException;

/**
 * 封装ES的TransportClient类
 *
 * @author tan.jie
 */
public class ESClient {
	private String cluster;
	private String appKey;
	private String appSecret;
	private String host;
	private Integer port;
	private Boolean sniff;

	public ESClient(String cluster, String appKey, String appSecret, String host, Integer port, Boolean sniff) {
		this.cluster = cluster;
		this.appKey = appKey;
		this.appSecret = appSecret;
		this.host = host;
		this.port = port;
		this.sniff = sniff;
	}

	public TransportClient getClient() throws UnknownHostException {
		Settings.Builder builder = Settings.builder().put("cluster.name", cluster);
		builder.put("client.transport.sniff", sniff);
		builder.put("xpack.security.user", appKey + ":" + appSecret);
		Settings settings = builder.build();
		TransportClient client = new PreBuiltXPackTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
		return client;
	}
}
