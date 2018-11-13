package org.apache.flink.kubernetes.cluster;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1ServiceList;
import io.kubernetes.client.util.Config;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KubeClient implements IKubeClient {

	private static final Logger LOG = LoggerFactory.getLogger(KubeClient.class);

	private CoreV1Api kubernetesClient;

	public KubeClient(String kubeConfigFile) throws IOException {
		this(kubeConfigFile, true);
	}

	public KubeClient(String kubeConfigFile, boolean localClient) throws IOException {

		if (localClient) {
			if (kubeConfigFile != null && (new File(kubeConfigFile).exists())) {
				this.kubernetesClient = new CoreV1Api(Config.fromConfig(kubeConfigFile));
			} else {
				this.kubernetesClient = new CoreV1Api(Config.defaultClient());
			}
		} else {
			this.kubernetesClient = new CoreV1Api(Config.fromCluster());
		}
	}

	private Map<String, String> getCommonLabels() {
		final Map<String, String> labels = new HashMap<>(2);
		labels.put("app", "flink");
		labels.put("role", "jobmanager");
		return labels;
	}

	private Map<String, String> getLabelWithClusterId(String clusterId) {
		final Map<String, String> labels = this.getCommonLabels();
		labels.put("cluster", clusterId);
		return labels;
	}

	@Override
	public Map.Entry<String, Integer> getClusterRestEndpoint(String clusterId) throws ClusterDeploymentException {

		boolean debugMode = true;

		if (debugMode) {
			return new AbstractMap.SimpleEntry<>("192.168.99.100", 8081);
		}

		Map<String, String> lables = this.getLabelWithClusterId(clusterId);
		lables.put("role", "jobmanager");

		String labelSelector = lables
			.entrySet()
			.stream()
			.map(e -> e.getKey() + "=" + e.getValue())
			.collect(Collectors.joining(","));

		try {
			String loadBalanceId = null;
			while (true) {

				V1ServiceList list = kubernetesClient
					.listNamespacedService("default", "true", null, null, true, labelSelector, 1000, null, null, false);

				V1Service service = list.getItems().get(0);
				if (service.getStatus().getLoadBalancer() == null
					|| service.getStatus().getLoadBalancer().getIngress() == null) {
					Thread.sleep(1000);
					continue;
				}
				loadBalanceId = service.getStatus().getLoadBalancer().getIngress().get(0).getIp();
				return new AbstractMap.SimpleEntry<>(loadBalanceId, 8081);
			}
		} catch (ApiException e) {
			LOG.error("Cannot get cluster reset endpoint: {}", e.getResponseBody());
		} catch (Exception e) {
			throw new ClusterDeploymentException("Could not create the Kubernetes cluster client", e);
		}

		return null;
	}

	@Override
	public List<String> listClusters() throws ClusterDeploymentException {

		Map<String, String> labels = this.getCommonLabels();

		String labelSelector = labels
			.entrySet()
			.stream()
			.map(e -> e.getKey() + "=" + e.getValue())
			.collect(Collectors.joining(","));

		try {
			return kubernetesClient.listNamespacedService(
				"default",
				"true", null,
				null,
				true,
				labelSelector, 1000,
				null,
				null,
				false)
				.getItems()
				.stream()
				.map(s->s.getMetadata().getName())
				.collect(Collectors.toList());

		} catch (ApiException e) {
			LOG.error("Cannot get cluster reset endpoint: {}", e.getResponseBody());
		} catch (Exception e) {
			throw new ClusterDeploymentException("Could not create the Kubernetes cluster client", e);
		}

		return null;
	}
}
