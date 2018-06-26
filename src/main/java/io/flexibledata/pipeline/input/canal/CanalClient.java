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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Predicates;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Canal客户端
 * 
 * @author tan.jie
 *
 */
@Slf4j
@Getter
@AllArgsConstructor
public class CanalClient {
	private CanalConfig canalConfig;

	/**
	 * 获取Canal连接，如果超时，则每秒重试一次
	 * 
	 * @return
	 */
	public CanalConnector getConnector() {
		final CanalConnector connector = canalConfig.getConnector();

		Callable<Boolean> yourTask = new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				try {
					connector.connect();
				} catch (CanalClientException e) {
					log.warn("Canal Server connetion exception. Try again a second later.");
					throw new CanalClientException(e);
				}
				return true;
			}
		};

		Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder().retryIfResult(Predicates.<Boolean>isNull()).retryIfExceptionOfType(CanalClientException.class)
				.withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS)).build();
		try {
			retryer.call(yourTask);
		} catch (RetryException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		if (StringUtils.isNotBlank(canalConfig.getTableFilterRegex())) {
			connector.subscribe(canalConfig.getTableFilterRegex());
		} else {
			connector.subscribe();
		}

		return connector;
	}

}