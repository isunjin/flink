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
import org.apache.flink.core.fs.Path;
import org.apache.flink.kubernetes.ClassPathJobGraphRetriever;
import org.apache.flink.kubernetes.resourcemanager.KubernetesResourceManagerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointException;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfiguration;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.JobDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Entrypoint for a Kubernetes job cluster.
 */

public class KubernetesJobClusterEntrypoint extends JobClusterEntrypoint {

	public static final String KUBERNETES_IMAGE_NAME = "KUBERNETES_IMAGE_NAME";

	public static final String KUBERNETES_CLUSTER_ID = "KUBERNETES_CLUSTER_ID";

	private  String[] programArguments;

	private  String jobClassName;

	private  SavepointRestoreSettings savepointRestoreSettings;

	@Nullable
	private final Path userCodeJarPath;

	@Nullable
	private final String entrypointClassName;

	@Nonnull
	private final String clusterId;

	@Nonnull
	private final String imageName;

	private final String[] args;

	private final int parallelism;

	public KubernetesJobClusterEntrypoint(Configuration configuration, @Nullable Path userCodeJarPath, @Nullable String entrypointClassName, @Nonnull String clusterId, @Nonnull String imageName) {
		super(configuration);
		this.userCodeJarPath = userCodeJarPath;
		this.entrypointClassName = entrypointClassName;
		this.clusterId = clusterId;
		this.imageName = imageName;
		args = new String[0];
		this.parallelism = 1;
	}

	@Override
	protected DispatcherResourceManagerComponentFactory<?> createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return new JobDispatcherResourceManagerComponentFactory(
			new KubernetesResourceManagerFactory(imageName, clusterId),
			new ClassPathJobGraphRetriever(jobClassName, savepointRestoreSettings, programArguments));
	}

	public static void main(String[] args) {
		final CommandLineParser<EntrypointClusterConfiguration> clusterConfigurationParser = new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());
		try {
			// startup checks and logging
			EnvironmentInformation.logEnvironmentInfo(LOG, KubernetesSessionClusterEntrypoint.class.getSimpleName(), args);
			SignalHandler.register(LOG);
			JvmShutdownSafeguard.installAsShutdownHook(LOG);

			EntrypointClusterConfiguration clusterConfiguration = clusterConfigurationParser.parse(args);

			Configuration configuration = loadConfiguration(clusterConfiguration);

			final String clusterId = System.getenv(KUBERNETES_CLUSTER_ID);
			final String imageName = System.getenv(KUBERNETES_IMAGE_NAME);

			KubernetesJobClusterEntrypoint entrypoint = new KubernetesJobClusterEntrypoint(configuration, null, null, clusterId, imageName);

			entrypoint.startCluster();
		} catch (FlinkParseException e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			clusterConfigurationParser.printHelp(KubernetesSessionClusterEntrypoint.class.getSimpleName());
			System.exit(1);
		}
		catch (ClusterEntrypointException e1){
			LOG.error("Could not start cluster {}", e1);
			System.exit(1);
		}
	}
}
