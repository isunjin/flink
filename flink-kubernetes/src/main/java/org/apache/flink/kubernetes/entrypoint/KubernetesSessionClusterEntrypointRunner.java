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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.ClusterEntrypointRunner;
import org.apache.flink.kubernetes.configuration.KubernetesClusterConfiguration;

import javax.annotation.Nonnull;
import java.util.Properties;

import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.CONFIG_DIR_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.HOST_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.REST_PORT_OPTION;

public class KubernetesSessionClusterEntrypointRunner extends ClusterEntrypointRunner<KubernetesClusterConfiguration, KubernetesSessionClusterEntrypoint> {

	public static final Option IMAGE_OPTION = Option.builder("i")
		.longOpt("image")
		.required(true)
		.hasArg(true)
		.argName("imagename")
		.desc("the docker image name.")
		.build();

	public static final Option CLUSTERID_OPTION = Option.builder("cid")
		.longOpt("clusterid")
		.required(true)
		.hasArg(true)
		.argName("clusterid")
		.desc("the cluster id that will be used in namespace")
		.build();

	@Override
	protected KubernetesSessionClusterEntrypoint createClusterEntrypoint(Configuration configuration, KubernetesClusterConfiguration entrypointClusterConfiguration) {
		return new KubernetesSessionClusterEntrypoint(configuration, entrypointClusterConfiguration);
	}

	@Override
	public Options getOptions() {
		final Options options = new Options();
		options.addOption(CONFIG_DIR_OPTION);
		options.addOption(REST_PORT_OPTION);
		options.addOption(HOST_OPTION);
		options.addOption(DYNAMIC_PROPERTY_OPTION);
		options.addOption(IMAGE_OPTION);
		options.addOption(CLUSTERID_OPTION);
		return options;
	}

	@Override
	public KubernetesClusterConfiguration createResult(@Nonnull CommandLine commandLine) {
		final String configDir = commandLine.getOptionValue(CONFIG_DIR_OPTION.getOpt());
		final Properties dynamicProperties = commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());
		final String restPortString = commandLine.getOptionValue(REST_PORT_OPTION.getOpt(), "-1");
		final int restPort = Integer.parseInt(restPortString);
		final String hostname = commandLine.getOptionValue(HOST_OPTION.getOpt());
		final String imageName = commandLine.getOptionValue(IMAGE_OPTION.getOpt());
		final String clusterId = commandLine.getOptionValue(CLUSTERID_OPTION.getOpt());

		return new KubernetesClusterConfiguration(
			configDir,
			dynamicProperties,
			hostname,
			restPort,
			imageName,
			clusterId);
	}

	public static void main(String[] args){
		new KubernetesSessionClusterEntrypointRunner().run(args);
	}
}
