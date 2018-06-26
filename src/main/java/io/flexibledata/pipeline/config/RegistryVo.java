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
package io.flexibledata.pipeline.config;

import lombok.Data;

/**
 * @author tan.jie
 *
 */
@Data
public class RegistryVo {
	private String type;

	// canal type
	private Boolean cluster;
	private String destination;
	private String connectUrl;
	private String tableFilterRegex;
	private Integer sleepTime;
	private Integer fetchSize;

	// mq type
	private Integer inputToFilterQueueSize;
	private Integer filterToOutputQueueSize;
	private Integer inputToFilterQueueTakeSize;
	private Integer filterToOutputQueueTakeSize;
}
