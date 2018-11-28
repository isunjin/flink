package org.apache.flink.runtime.executiongraph.dynamic;

import org.apache.flink.runtime.executiongraph.*;

import java.util.ArrayList;

public class AlltoAllConnectionStragety extends ConnectionStrategy {

	public AlltoAllConnectionStragety(IntermediateResult source, ExecutionJobVertex target,
									  int inputNumber, int consumerNumber){
		super(source, target, inputNumber, consumerNumber);
	}

	protected void addEdge(ArrayList<ExecutionEdge> edges, IntermediateResultPartition partition){

	}

	@Override
	public void connectToSource(ExecutionVertex task) {
		final ArrayList<IntermediateResultPartition> sourcePartitions = this.source.getPartitions();
		ArrayList<ExecutionEdge> edges = task.getEdges(this.inputNumber);
		edges.ensureCapacity(sourcePartitions.size());

		for (int i = 0; i < sourcePartitions.size(); i++) {
			IntermediateResultPartition irp = sourcePartitions.get(i);
			edges.set(i, new ExecutionEdge(irp, task, this.inputNumber));
			irp.addConsumer(edges.get(i), this.consumerNumber);
		}
	}

	@Override
	public void connectToTarget(IntermediateResultPartition data) {
		ArrayList<ExecutionVertex> vertices = this.target.getTaskVertices();
		for (int i = 0; i < vertices.size(); i++) {
			ExecutionVertex vertex = vertices.get(i);
			ArrayList<ExecutionEdge> edges = vertex.getEdges(this.inputNumber);
			edges.ensureCapacity(data.getPartitionNumber() + 1);
			edges.set(data.getPartitionNumber(), new ExecutionEdge(data, vertex, this.inputNumber));
			data.addConsumer(edges.get(i), this.consumerNumber);
		}
	}

	@Override
	public void disconnectFromSource(ExecutionVertex task) {
		final ArrayList<IntermediateResultPartition> sourcePartitions = this.source.getPartitions();
		ArrayList<ExecutionEdge> edges = task.getEdges(this.inputNumber);

		for (int i = 0; i < sourcePartitions.size(); i++) {
			IntermediateResultPartition irp = sourcePartitions.get(i);
			irp.removeConsumer(edges.get(i), this.consumerNumber);
			edges.remove(i);
		}
	}

	@Override
	public void disconnectFromTarget(IntermediateResultPartition data) {
		ArrayList<ExecutionVertex> vertices = this.target.getTaskVertices();
		for (int i = 0; i < vertices.size(); i++) {
			ExecutionVertex vertex = vertices.get(i);
			ArrayList<ExecutionEdge> edges = vertex.getEdges(this.inputNumber);
			ExecutionEdge edge = edges.get(data.getPartitionNumber());
			edges.remove(data.getPartitionNumber());
			data.removeConsumer(edge, this.consumerNumber);
		}
	}
}
