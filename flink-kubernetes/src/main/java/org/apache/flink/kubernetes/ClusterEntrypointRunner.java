package org.apache.flink.kubernetes;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfiguration;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;

public abstract class ClusterEntrypointRunner<T extends EntrypointClusterConfiguration, R extends ClusterEntrypoint>
	implements ParserResultFactory<T> {

	protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypoint.class);

	protected abstract R createClusterEntrypoint(Configuration configuration, T entrypointClusterConfiguration);

	protected Configuration loadConfiguration(T entrypointClusterConfiguration) {
		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(entrypointClusterConfiguration.getDynamicProperties());
		final Configuration configuration = GlobalConfiguration.loadConfiguration(entrypointClusterConfiguration.getConfigDir(), dynamicProperties);

		final int restPort = entrypointClusterConfiguration.getRestPort();

		if (restPort >= 0) {
			configuration.setInteger(RestOptions.PORT, restPort);
		}

		final String hostname = entrypointClusterConfiguration.getHostname();

		if (hostname != null) {
			configuration.setString(JobManagerOptions.ADDRESS, hostname);
		}

		return configuration;
	}

	public void run(String[] args){

		Class clazz = (Class)((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];

		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, clazz.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		T entrypointClusterConfiguration = null;

		final CommandLineParser<T> commandLineParser = new CommandLineParser<>(this);

		try {
			entrypointClusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(clazz.getSimpleName());
			System.exit(1);
		}

		Configuration configuration = loadConfiguration(entrypointClusterConfiguration);

		R entrypoint = this.createClusterEntrypoint(configuration, entrypointClusterConfiguration);

		ClusterEntrypoint.runClusterEntrypoint(entrypoint);
	}
}
