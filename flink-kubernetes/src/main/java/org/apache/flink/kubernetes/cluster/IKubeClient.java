package org.apache.flink.kubernetes.cluster;

import java.util.List;
import java.util.Map;

public interface IKubeClient {

	Map.Entry<String, Integer> getClusterRestEndpoint(String clusterId) throws Exception;

	List<String> listClusters() throws Exception;
}
