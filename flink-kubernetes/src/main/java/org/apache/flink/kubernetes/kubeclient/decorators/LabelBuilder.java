/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.kubeclient.decorators;

import java.util.HashMap;
import java.util.Map;

public class LabelBuilder {

	private Map<String, String> labels;

	private LabelBuilder(){
		this.labels = new HashMap<>();
	}

	private LabelBuilder withLabel(String key, String value){
		this.labels.put(key, value);
		return this;
	}

	public static LabelBuilder withCommon(){
		return new LabelBuilder()
			.withLabel("app", "flink-native-k8s");
	}

	public static LabelBuilder withApplicationId(String id){
		return new LabelBuilder()
			.withLabel("appId", id);
	}

	public static LabelBuilder withJobManagerRole(){
		return new LabelBuilder()
			.withLabel("role", "jobmanager");
	}

	public static LabelBuilder withTaskManagerRole(){
		return new LabelBuilder()
			.withLabel("role", "taskmanager");
	}

	public Map<String, String> toLabels(){
		return labels;
	}
}

