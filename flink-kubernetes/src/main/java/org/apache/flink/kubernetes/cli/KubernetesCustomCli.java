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

package org.apache.flink.kubernetes.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.client.cli.AbstractCustomCommandLine;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.cluster.KubernetesClusterDescriptor;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.client.cli.CliFrontendParser.CLASS_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;

/**
 * Kubernetes {@link CustomCommandLine} implementation.
 */
public class KubernetesCustomCli extends AbstractCustomCommandLine<String> {

	private static final String CLUSTER_ID = "Kubernetes-cluster";

	private static final Option IMAGE_OPTION = new Option("i", "image", true, "Image name");

	private static final Option USER_CODE_JAR_OPTION = new Option("u", "userCodeJar", true, "User code jar");

	protected static final Option K8s_CONFIG_File = new Option("kc", "K8sConfigFile", true,
		"The config file to for K8s API client");

	protected static final Option K8s_MODE = new Option("k8s", "K8sMode", false,
		"Use K8s Mode");

	protected static final Option K8s_APPLICATION_ID = new Option("kid", "k8sApplicationId", true,
		"Kubernetes Application Id");

	private static final Option HELP = new Option("h", "help", false, "Help for the Yarn session CLI.");

	public class K8sOptions extends CommandLineOptions {

		protected final String configFile;

		protected final boolean helpOption;

		protected final Properties dynamicProperties;

		public K8sOptions(CommandLine line) {
			super(line);
			this.configFile = line.getOptionValue(K8s_CONFIG_File.getOpt(), "");
			this.helpOption = line.hasOption(HELP.getOpt());
			this.dynamicProperties = line.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());
		}

		public String getConfigFile() {
			return configFile;
		}

		public boolean hasHelpOption() {
			return helpOption;
		}

		public Properties getDynamicProperties() {
			return dynamicProperties;
		}
	}

	public class ListOptions extends K8sOptions {
		public ListOptions(CommandLine line) {
			super(line);
		}
	}

	public class StartOptions extends K8sOptions {
		public StartOptions(CommandLine line) {
			super(line);
		}
	}

	public class StopOptions extends K8sOptions {

		protected final String applicationId;

		public StopOptions(CommandLine line) {
			super(line);
			this.applicationId = line.getOptionValue(K8s_APPLICATION_ID.getOpt(), "");
		}

		public String getApplicationId() {
			return applicationId;
		}
	}

	public class RunOptions extends K8sOptions {

		protected final boolean k8sMode;

		protected final String address;

		protected final String imageName;

		protected final String entryClassName;

		protected final String userCodeJar;

		protected final String applicationId;

		public RunOptions(CommandLine line) {
			super(line);
			this.k8sMode = line.hasOption(K8s_MODE.getOpt());
			this.address = line.getOptionValue(addressOption.getOpt(), "");
			this.imageName = line.getOptionValue(IMAGE_OPTION.getOpt(), "flink-dummy");
			this.entryClassName = line.getOptionValue(CLASS_OPTION.getOpt());
			this.userCodeJar = line.getOptionValue(USER_CODE_JAR_OPTION.getOpt());
			this.applicationId = line.getOptionValue(K8s_APPLICATION_ID.getOpt(), "");
		}

		public String getAddress() {
			return address;
		}

		public boolean isInK8sMode() {
			return this.k8sMode
				|| !this.configFile.isEmpty()
				|| ADDRESS_PATTERN.matcher(this.address).find();
		}

		public String getImageName() {
			return imageName;
		}

		public String getEntryClassName() {
			return entryClassName;
		}

		public String getUserCodeJar() {
			return userCodeJar;
		}

		public String getApplicationId() {
			return applicationId;
		}
	}

	// actions
	private static final String ACTION_START = "start";
	private static final String ACTION_LIST = "list";
	private static final String ACTION_STOP = "stop";

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesCustomCli.class);

	private static final Pattern ADDRESS_PATTERN = Pattern.compile("k8s://https?://([^:]*):(\\d+)/?");

	private static final String K8S_PROPERTIES_FILE = ".k8s-properties-";

	private PropertyUtil propertyUtil;

	public KubernetesCustomCli(Configuration configuration) {
		super(configuration);
		this.propertyUtil = new PropertyUtil(this.getK8sPropertiesLocation().getAbsolutePath());
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		return new RunOptions(commandLine).isInK8sMode();
	}

	@Override
	public String getId() {
		return CLUSTER_ID;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		baseOptions.addOption(K8s_CONFIG_File)
			.addOption(addressOption)
			.addOption(K8s_MODE)
			.addOption(IMAGE_OPTION)
			.addOption(USER_CODE_JAR_OPTION)
			.addOption(CLASS_OPTION)
			.addOption(DYNAMIC_PROPERTY_OPTION)
			.addOption(K8s_APPLICATION_ID);
	}

	@Override
	public ClusterDescriptor<String> createClusterDescriptor(CommandLine commandLine) throws FlinkException {

		Configuration configuration = new Configuration(this.getConfiguration());

		RunOptions runOptions = new RunOptions(commandLine);

		for (String propertyName : runOptions.getDynamicProperties().stringPropertyNames()) {
			configuration.setString(propertyName, runOptions.getDynamicProperties().getProperty(propertyName));
		}

		Matcher matcher = ADDRESS_PATTERN.matcher(runOptions.getAddress());

		if (matcher.find()) {
			Preconditions.checkState(matcher.matches());

			String address = matcher.group(1);
			String port = matcher.group(2);

			configuration.setString(JobManagerOptions.ADDRESS, address);
			configuration.setInteger(JobManagerOptions.PORT, Integer.parseInt(port));
		}

		//MUST HAVE

		try {
			KubernetesClusterDescriptor descriptor = new KubernetesClusterDescriptor(configuration,
				runOptions.getConfigFile(),
				runOptions.getImageName(),
				runOptions.getEntryClassName(),
				runOptions.getUserCodeJar(),
				this.propertyUtil);

			return descriptor;
		} catch (Exception e) {
			throw new FlinkException("Could not create the KubernetesClusterDescriptor.", e);
		}
	}

	@Nullable
	@Override
	public String getClusterId(CommandLine commandLine) {
		try {
			RunOptions runOptions = new RunOptions(commandLine);

			String k8sApplicationId = runOptions.getApplicationId();

			if (!k8sApplicationId.isEmpty()) {
				return k8sApplicationId;
			}
			else{
				Properties properties = this.propertyUtil.read();
				return properties.getProperty("default");
			}
		}catch (Exception e){

		}

		return null;
	}

	@Override
	public ClusterSpecification getClusterSpecification(CommandLine commandLine) {
		return new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();
	}

	private File getK8sPropertiesLocation() {

		final String propertiesFileLocation = System.getProperty("java.io.tmpdir");
		String currentUser = System.getProperty("user.name");

		return new File(propertiesFileLocation, K8S_PROPERTIES_FILE + currentUser);
	}

	//// command line /////////////////////////////////////////////////////////////////////////////////////////////////

	public int start(String[] args) throws CliArgsException, FlinkException {
		//
		//	Command Line Options
		//
		final CommandLine cmd = parseCommandLineOptions(args, true);
		StartOptions startOptions = new StartOptions(cmd);

		if (startOptions.hasHelpOption()) {
			printUsage();
			return 0;
		}
		System.out.println("Starting K8s session.");

		ClusterDescriptor<String> cluster = this.createClusterDescriptor(cmd);
		final ClusterSpecification clusterSpecification = getClusterSpecification(cmd);
		cluster.deploySessionCluster(clusterSpecification);

		return 0;
	}

	public int stop(String[] args) throws CliArgsException, FlinkException {
		//
		//	Command Line Options
		//
		final CommandLine cmd = parseCommandLineOptions(args, true);
		StopOptions stopOptions = new StopOptions(cmd);

		if (stopOptions.hasHelpOption()) {
			printUsage();
			return 0;
		}

		KubernetesClusterDescriptor cluster = (KubernetesClusterDescriptor)this.createClusterDescriptor(cmd);
		cluster.killDefaultSessionCluster();

		return 0;
	}

	public int list(String[] args) throws CliArgsException, FlinkException {
		//
		//	Command Line Options
		//
		final CommandLine cmd = parseCommandLineOptions(args, true);
		ListOptions listOptions = new ListOptions(cmd);

		if (listOptions.hasHelpOption()) {
			printUsage();
			return 0;
		}

		return 0;
	}

	private void printUsage() {
		System.out.println("Usage:");
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.setLeftPadding(5);
		formatter.setSyntaxPrefix("   Required");
		Options req = new Options();
		formatter.printHelp(" ", req);

		formatter.setSyntaxPrefix("   Optional");
		Options options = new Options();
		addGeneralOptions(options);
		addRunOptions(options);
		formatter.printHelp(" ", options);
	}

	/**
	 * Parses the command line arguments and starts the requested action.
	 *
	 * @param args command line arguments of the client.
	 * @return The return code of the program
	 */
	public int parseParameters(String[] args) {

		// check for action
		if (args.length < 1) {
			System.out.println("Please specify an action.");
			return 1;
		}

		// get action
		String action = args[0];

		// remove action from parameters
		final String[] params = Arrays.copyOfRange(args, 1, args.length);

		try {
			// do action
			switch (action) {
				case ACTION_START:
					start(params);
					return 0;
				case ACTION_LIST:
					list(params);
					return 0;
				case ACTION_STOP:
					stop(params);
					return 0;
				case "--version":
					String version = EnvironmentInformation.getVersion();
					String commitID = EnvironmentInformation.getRevisionInformation().commitId;
					System.out.print("Version: " + version);
					System.out.println(commitID.equals(EnvironmentInformation.UNKNOWN) ? "" : ", Commit ID: " + commitID);
					return 0;
				default:
					System.out.printf("\"%s\" is not a valid action.\n", action);
					System.out.println();
					System.out.println("Valid actions are \"start\", \"list\", or \"stop\".");
					System.out.println();
					System.out.println("Specify the version option (-v or --version) to print Flink version.");
					System.out.println();
					System.out.println("Specify the help option (-h or --help) to get help on the command.");
					return 1;
			}
		} catch (CliArgsException ce) {
			return handleCliArgsException(ce);
		} catch (Exception e) {
			return handleError(e);
		}
	}

	public static void main(String[] args) {

		final Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration();

		int retCode;

		try {
			final KubernetesCustomCli cli = new KubernetesCustomCli(flinkConfiguration);

			SecurityUtils.install(new SecurityConfiguration(flinkConfiguration));

			retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.parseParameters(args));
		} catch (CliArgsException e) {
			retCode = handleCliArgsException(e);
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			retCode = handleError(strippedThrowable);
		}

		System.exit(retCode);
	}

	private static int handleCliArgsException(CliArgsException e) {
		LOG.error("Could not parse the command line arguments.", e);

		System.out.println(e.getMessage());
		System.out.println();
		System.out.println("Use the help option (-h or --help) to get help on the command.");
		return 1;
	}

	private static int handleError(Throwable t) {
		LOG.error("Error while running the Flink Yarn session.", t);

		System.err.println();
		System.err.println("------------------------------------------------------------");
		System.err.println(" The program finished with the following exception:");
		System.err.println();

		t.printStackTrace();
		return 1;
	}
}
