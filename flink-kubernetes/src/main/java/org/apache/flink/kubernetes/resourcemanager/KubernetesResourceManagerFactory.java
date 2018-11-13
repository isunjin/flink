package org.apache.flink.kubernetes.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class KubernetesResourceManagerFactory implements ResourceManagerFactory<KubernetesResourceManager.KubernetesWorkerNode> {

	@Nonnull
	private final String imageName;

	@Nonnull
	private final String clusterId;

	private final String serviceUid;

	public KubernetesResourceManagerFactory(@Nonnull String imageName, @Nonnull String clusterId, String serviceUid) {
		this.imageName = imageName;
		this.clusterId = clusterId;
		this.serviceUid = serviceUid;
	}

	@Override
	public ResourceManager<KubernetesResourceManager.KubernetesWorkerNode> createResourceManager(
		Configuration configuration,
		ResourceID resourceId,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		MetricRegistry metricRegistry,
		FatalErrorHandler fatalErrorHandler,
		ClusterInformation clusterInformation,
		@Nullable String webInterfaceUrl,
		JobManagerMetricGroup jobManagerMetricGroup) throws Exception {
		final ResourceManagerRuntimeServicesConfiguration rmServicesConfiguration = ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration);
		final ResourceManagerRuntimeServices rmRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
			rmServicesConfiguration,
			highAvailabilityServices,
			rpcService.getScheduledExecutor());

		return new KubernetesResourceManager(
			configuration,
			rpcService,
			ResourceManager.RESOURCE_MANAGER_NAME,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			rmRuntimeServices.getSlotManager(),
			metricRegistry,
			rmRuntimeServices.getJobLeaderIdService(),
			clusterInformation,
			fatalErrorHandler,
			clusterId,
			imageName,
			this.serviceUid,
			jobManagerMetricGroup);
	}
}
