package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;

/**
 * Base class for the run {@link ClusterEntrypoint}.
 * */
public abstract class ClusterEntrypointRunner implements ParserResultFactory<FlinkKubernetesOptions> {
	protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypointRunner.class);

	protected abstract ClusterEntrypoint createClusterEntrypoint(FlinkKubernetesOptions options);

	public void run(String[] args) {

		Class clazz = (Class) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];

		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, clazz.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		FlinkKubernetesOptions options;

		final CommandLineParser<FlinkKubernetesOptions> commandLineParser = new CommandLineParser<>(this);

		try {
			options = commandLineParser.parse(args);
			ClusterEntrypoint entrypoint = this.createClusterEntrypoint(options);
			ClusterEntrypoint.runClusterEntrypoint(entrypoint);

		} catch (FlinkParseException e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(clazz.getSimpleName());
			System.exit(1);
		}
	}
}
