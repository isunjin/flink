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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesClusterConfiguration;
import org.apache.flink.kubernetes.resourcemanager.KubernetesResourceManagerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointException;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfiguration;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.SessionClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.SessionDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import javax.annotation.Nonnull;

/**
 * Entrypoint for a Kubernetes session cluster.
 */
public class KubernetesSessionClusterEntrypoint extends SessionClusterEntrypoint {

	@Nonnull
	private final KubernetesClusterConfiguration kuberConfig;

	public KubernetesSessionClusterEntrypoint(Configuration configuration, KubernetesClusterConfiguration kuberConfig) {
		super(configuration);
		this.kuberConfig = kuberConfig;
	}

	@Override
	protected DispatcherResourceManagerComponentFactory<?> createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return new SessionDispatcherResourceManagerComponentFactory(new KubernetesResourceManagerFactory(this.kuberConfig.getImageName(), this.kuberConfig.getClusterId(), this.kuberConfig.getServiceUid()));
	}
}
