package org.apache.flink.runtime.executiongraph.dynamic;

import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;

public class AlltoAllConnectionStragety extends ConnectionStrategy {

	public AlltoAllConnectionStragety(IntermediateResult source){
		super(source);
	}

	@Override
	public ExecutionEdge[] connectSource(ExecutionVertex target, int inputNumber) {
		final IntermediateResultPartition[] sourcePartitions = source.getPartitions();
		ExecutionEdge[] edges = new ExecutionEdge[sourcePartitions.length];

		for (int i = 0; i < sourcePartitions.length; i++) {
			IntermediateResultPartition irp = sourcePartitions[i];
			edges[i] = new ExecutionEdge(irp, target, inputNumber);
		}

		return edges;
	}
}
