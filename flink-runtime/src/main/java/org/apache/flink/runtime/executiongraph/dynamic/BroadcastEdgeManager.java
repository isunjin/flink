package org.apache.flink.runtime.executiongraph.dynamic;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.JobEdge;

public class BroadcastEdgeManager extends EdgeManager {

	BroadcastEdgeManager(ExecutionGraph graph, JobEdge edge){
		super(graph, edge);

	}

	@Override
	public void onPartitionProduced(IntermediateResultPartition partition) {

	}

	@Override
	public void onPartitionAdded(IntermediateResultPartition partition) {

	}

	@Override
	public void onPartitionRemoved(IntermediateResultPartition partition) {

	}

	@Override
	public void onExecutionVertexRemoved(ExecutionVertex vertex) {

	}

	@Override
	public void onExecutionVertexAdded(ExecutionVertex vertex) {

	}
}
