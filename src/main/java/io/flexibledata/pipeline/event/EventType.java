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
package io.flexibledata.pipeline.event;

import java.util.Arrays;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

/**
 * 事件类型
 * 
 * @author tan.jie
 *
 */
public enum EventType {
	INSERT("insert"), UPDATE("update"), DELETE("delete");

	private final String name;

	private EventType(final String name) {
		this.name = name;
	}

	public String getName() {
		return this.name();
	}

	/**
	 * 通过事件类型名称获取事件类型枚举
	 * 
	 * @param eventTypeName
	 * @return
	 */
	public static EventType valueFrom(final String eventTypeName) {
		Optional<EventType> eventTypeOptional = Iterators.tryFind(Arrays.asList(EventType.values()).iterator(), new Predicate<EventType>() {

			@Override
			public boolean apply(final EventType input) {
				return input.name.equals(eventTypeName);
			}
		});
		if (eventTypeOptional.isPresent()) {
			return eventTypeOptional.get();
		}
		throw new UnsupportedOperationException(String.format("Can not support database type [%s].", eventTypeName));
	}
}
