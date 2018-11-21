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
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

import java.util.Properties;

import static org.apache.flink.kubernetes.FlinkKubernetesOptions.CLUSTERID_OPTION;
import static org.apache.flink.kubernetes.FlinkKubernetesOptions.IMAGE_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.HOST_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.REST_PORT_OPTION;

/**
 * the entrance of Kubernetes session cluster.
 * */
public class KubernetesSessionClusterEntrypointRunner extends ClusterEntrypointRunner {

	@Override
	protected ClusterEntrypoint createClusterEntrypoint(FlinkKubernetesOptions options) {
		return new KubernetesSessionClusterEntrypoint(options);
	}

	@Override
	public Options getOptions() {
		final Options options = new Options();
		options.addOption(REST_PORT_OPTION);
		options.addOption(HOST_OPTION);
		options.addOption(DYNAMIC_PROPERTY_OPTION);
		options.addOption(IMAGE_OPTION);
		options.addOption(CLUSTERID_OPTION);
		return options;
	}

	@Override
	public FlinkKubernetesOptions createResult(@Nonnull CommandLine commandLine) {
		final Properties dynamicProperties = commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());
		final String restPortString = commandLine.getOptionValue(REST_PORT_OPTION.getOpt(), "-1");
		int restPort = Integer.parseInt(restPortString);
		String hostname = commandLine.getOptionValue(HOST_OPTION.getOpt());
		final String imageName = commandLine.getOptionValue(IMAGE_OPTION.getOpt());
		final String clusterId = commandLine.getOptionValue(CLUSTERID_OPTION.getOpt());

		hostname = hostname == null ? clusterId : hostname;
		Configuration configuration = GlobalConfiguration
			.loadConfigurationWithDynamicProperties(ConfigurationUtils.createConfiguration(dynamicProperties));

		configuration.setString(RestOptions.ADDRESS, hostname);

		if (restPort == -1) {
			restPort = RestOptions.PORT.defaultValue();
		}

		configuration.setInteger(RestOptions.PORT, restPort);

		FlinkKubernetesOptions options = new FlinkKubernetesOptions(configuration, clusterId);
		options.setImageName(imageName);

		return options;
	}

	public static void main(String[] args) {
		new KubernetesSessionClusterEntrypointRunner().run(args);
	}
}
