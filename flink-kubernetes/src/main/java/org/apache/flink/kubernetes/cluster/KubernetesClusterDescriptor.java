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

package org.apache.flink.kubernetes.cluster;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1HostPathVolumeSource;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1OwnerReference;
import io.kubernetes.client.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1ServiceList;
import io.kubernetes.client.models.V1ServicePort;
import io.kubernetes.client.models.V1ServiceSpec;
import io.kubernetes.client.models.V1ServiceStatus;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import io.kubernetes.client.util.Config;
import org.apache.commons.lang3.text.translate.UnicodeUnpairedSurrogateRemover;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.cli.KubernetesCustomCli;
import org.apache.flink.kubernetes.cli.PropertyUtil;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Kubernetes specific {@link ClusterDescriptor} implementation.
 */
public class KubernetesClusterDescriptor implements ClusterDescriptor<String> {

	@Nonnull
	private final Configuration configuration;

	@Nonnull
	private final String imageName;

	@Nullable
	private final String className;

	@Nullable
	private final String userCodeJar;

	private CoreV1Api kubernetesClient;

	private PropertyUtil propertyUtil;

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesClusterDescriptor.class);

	public KubernetesClusterDescriptor(@Nonnull Configuration configuration,
									   String kubeConfigFile,
									   @Nonnull String imageName,
									   @Nullable String className,
									   @Nullable String userCodeJar,
									   PropertyUtil propertyUtil) throws IOException {
		this.configuration = configuration;
		this.imageName = imageName;
		this.className = className;
		this.userCodeJar = userCodeJar;
		if (kubeConfigFile != null &&  (new File(kubeConfigFile).exists())) {
			kubernetesClient = new CoreV1Api(Config.fromConfig(kubeConfigFile));
		} else {
			kubernetesClient = new CoreV1Api(Config.defaultClient());
		}

		this.propertyUtil = propertyUtil;
	}

	private Map<String, String> getCommonLabels(){
		final Map<String, String> labels = new HashMap<>(2);
		labels.put("app", "flink");
		labels.put("role", "jobmanager");
		return labels;
	}

	private Map<String, String> getLabelWithClusterId(String clusterId){
		final Map<String, String> labels = this.getCommonLabels();
		labels.put("cluster", clusterId);
		return labels;
	}

	public String getExternalIP(String clusterId) throws  Exception {

		boolean debugMode = this.configuration.getBoolean("debug.enable", false);

		if(debugMode){
			return "192.168.99.100";
		}

		String labelSelector = this.getLabelWithClusterId(clusterId)
			.entrySet()
			.stream()
			.map(e -> e.getKey() + "=" + e.getValue())
			.collect(Collectors.joining(","));

		String loadBalanceId = null;
		while(true) {

			V1ServiceList list = kubernetesClient
				.listNamespacedService("default", "true", null, null, true, labelSelector, 1000, null, null, false);

			V1Service service = list.getItems().get(0);
			if(service.getSpec().getExternalIPs()!=null &&service.getSpec().getExternalIPs().size() > 0){
				return service.getSpec().getExternalIPs().get(0);
			}
			if (service.getStatus().getLoadBalancer() == null
				|| service.getStatus().getLoadBalancer().getIngress() == null) {
				Thread.sleep(1000);
				continue;
			}
			loadBalanceId = service.getStatus().getLoadBalancer().getIngress().get(0).getIp();
			break;
		}
		return loadBalanceId;
	}

	@Override
	public String getClusterDescription() {
		return "Kubernetes cluster";
	}

	@Override
	public ClusterClient<String> retrieve(String clusterId) throws ClusterRetrieveException {
		try {
			String jobManagerIP = this.getExternalIP(clusterId);
			configuration.setString(JobManagerOptions.ADDRESS, jobManagerIP);
			configuration.setInteger(JobManagerOptions.PORT, 8081);
			return new RestClusterClient<>(this.configuration, clusterId);
		} catch (Exception e) {
			throw new ClusterRetrieveException("Could not create the RestClusterClient.", e);
		}
	}

	@Override
	public ClusterClient<String> deploySessionCluster(ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
		String clusterId = "flink-session-cluster-" + UUID.randomUUID();

		final List<String> args = new ArrayList<>(1);

		args.add("cluster");
		args.add("-i");
		args.add(this.imageName);
		args.add("-cid");
		args.add(clusterId);
		boolean debugMode = this.configuration.getBoolean("debug.enable", false);

		if(debugMode){
			args.add("-D");
			args.add("debug.enable=true");
		}

		return deployClusterInternal(clusterId, args);
	}

	@Override
	public ClusterClient<String> deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) throws ClusterDeploymentException {
		final String clusterName = "flink-job-cluster-" + UUID.randomUUID();

		final List<String> args = new ArrayList<>(6);
		args.add("job");

		if (detached) {
			args.add("--detached");
		}

		if (className != null) {
			args.add("--class");
			args.add(className);
		}

		if (userCodeJar != null) {
			args.add("--userCodeJar");
			args.add(userCodeJar);
		}

		return deployClusterInternal(clusterName, args);
	}

	@Nonnull
	private ClusterClient<String> deployClusterInternal(String clusterId, List<String> args) throws ClusterDeploymentException {
		try {

			boolean debugMode = this.configuration.getBoolean("debug.enable", false);

			final Map<String, String> labels = this.getLabelWithClusterId(clusterId);

			final V1ServicePort rpcPort = new V1ServicePort()
				.name("rpc")
				.port(6123);

			final V1ServicePort blobPort = new V1ServicePort()
				.name("blob")
				.port(6124);

			final V1ServicePort queryPort = new V1ServicePort()
				.name("query")
				.port(6125);

			final String uiPortName = "ui";
			final V1ServicePort uiPort = new V1ServicePort()
				.name(uiPortName)
				.port(8081);

			V1ServiceSpec serviceSpec = new V1ServiceSpec()
				//.type("NodePort")
				.ports(
					Arrays.asList(rpcPort, blobPort, queryPort, uiPort))
				.selector(labels)
				.type("LoadBalancer")
				;//.addExternalIPsItem(this.jobManagerIpOrDns);

			if(debugMode){
				serviceSpec = serviceSpec.loadBalancerIP("192.168.99.100")
				.addExternalIPsItem("192.168.99.100");
			}

			final V1Service service = new V1Service()
				.apiVersion("v1")
				.kind("Service")
				.metadata(new V1ObjectMeta()
					.name(clusterId)
					.labels(labels))
				.spec(serviceSpec);

			kubernetesClient.createNamespacedService("default", service, "true");

			Thread.sleep(1000);

			String jobManagerIP = this.getExternalIP(clusterId);
			configuration.setString(JobManagerOptions.ADDRESS, jobManagerIP);
			configuration.setInteger(JobManagerOptions.PORT, 8081);


			String labelSelector = this.getLabelWithClusterId(clusterId)
				.entrySet()
				.stream()
				.map(e -> e.getKey() + "=" + e.getValue())
				.collect(Collectors.joining(","));

			V1ServiceList list = kubernetesClient
				.listNamespacedService("default", "true", null, null, true, labelSelector, 1000, null, null, false);

			V1Service service1 = list.getItems().get(0);

			//for deletion
			args.add("-uid");
			args.add(service1.getMetadata().getUid());

			V1Container clusterContainer = new V1Container()
				.name(clusterId)
				.image(imageName)
				.imagePullPolicy("IfNotPresent")
				.addPortsItem(new V1ContainerPort().containerPort(8081))
				.addPortsItem(new V1ContainerPort().containerPort(6123))
				.addPortsItem(new V1ContainerPort().containerPort(6124))
				.addPortsItem(new V1ContainerPort().containerPort(6125))
				.args(args);
			//.env(
			//Arrays.asList(
			//	new V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value(clusterName)));
			//new V1EnvVar().name(KUBERNETES_IMAGE_NAME).value(imageName),
			//new V1EnvVar().name(KUBERNETES_CLUSTER_ID).value(clusterName)));

			V1PodSpec spec = new V1PodSpec();

			LOG.info("NOT In debug mode");
			if (debugMode) {
				LOG.info("In debug mode");
				clusterContainer = clusterContainer
					.addVolumeMountsItem(new V1VolumeMount()
						.mountPath("/opt")
						.name("flink-dist-volume")
					)
					.addVolumeMountsItem(new V1VolumeMount()
						.mountPath("/opt1")
						.name("flink-root-volume")
					);
				spec = spec.addVolumesItem(new V1Volume().name("flink-dist-volume")
					.hostPath(new V1HostPathVolumeSource().path("/flink-root/flink-dist/target/flink-1.8-SNAPSHOT-bin")))
					.addVolumesItem(new V1Volume().name("flink-root-volume")
						.hostPath(new V1HostPathVolumeSource().path("/flink-root/")));
			}

			boolean attachOss = true;

			if(attachOss){
				clusterContainer = clusterContainer
					.addVolumeMountsItem(new V1VolumeMount().name("pvc-oss").mountPath("/data"));

				spec = spec.addVolumesItem(new V1Volume().name("pvc-oss")
					.persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
						.claimName("pvc-oss")));
			}

			final Map<String, String> podLabels = new HashMap<>(labels);
			podLabels.put("role","jobmanager");

			final V1Pod pod = new V1Pod()
				.apiVersion("v1")
				.metadata(new V1ObjectMeta().
					name(clusterId)
					.labels(labels)
					.addOwnerReferencesItem(new V1OwnerReference()
						.name(service.getMetadata().getName())
						.controller(true)
						.blockOwnerDeletion(true)
						.kind(service.getKind())
						.apiVersion(service.getApiVersion())
						.uid(service1.getMetadata().getUid())
					)
				)
				.spec(spec.containers(Collections.singletonList(clusterContainer)));

			kubernetesClient.createNamespacedPod("default", pod, "true");

			final Configuration modifiedConfiguration = new Configuration(configuration);

			/*Properties properties = this.propertyUtil.read();
			String endPoint = String.format("k8s://%s:%d", this.jobManagerIpOrDns, this.jobManagerUIPort);
			properties.setProperty("default", endPoint);
			properties.setProperty("default.clusterId", this.clusterId);
			properties.setProperty(this.clusterId,  endPoint);
			this.propertyUtil.write(properties);*/

			return new RestClusterClient<>(modifiedConfiguration, clusterId);
		} catch (ApiException e){
			this.tryKill(clusterId);
			throw new ClusterDeploymentException("Could not create the Kubernetes cluster client" + e.getResponseBody(), e);
		}
		catch (Exception e) {
			this.tryKill(clusterId);
			throw new ClusterDeploymentException("Could not create the Kubernetes cluster client", e);
		}
	}

	public void killDefaultSessionCluster() throws FlinkException {
		Properties properties = this.propertyUtil.read();
		String clusterId = properties.getProperty("default.clusterId");
		if(!clusterId.isEmpty()){
			this.killCluster(clusterId);
		}
	}

	private void tryKill(String clusterId){
		try {
			this.killCluster(clusterId);
		}catch (Exception e){
			LOG.info("Fail to kill cluster {}: {}", clusterId, e.toString());
		}
	}

	@Override
	public void killCluster(String clusterId) throws FlinkException {
		try {
			kubernetesClient.deleteNamespacedService(clusterId, "default", new V1DeleteOptions(), null, 1, null, "Foreground");
			//kubernetesClient.deleteCollectionNamespacedPod("default", "true", null, null, true, "app=flink,cluster=" + clusterId, 1000, null, null, false);
		} catch (ApiException e) {
			throw new FlinkException(String.format("Could not kill the cluster %s: %s.", clusterId, e.getResponseBody()), e);
		}
	}

	@Override
	public void close() throws Exception {
		// noop
	}
}
