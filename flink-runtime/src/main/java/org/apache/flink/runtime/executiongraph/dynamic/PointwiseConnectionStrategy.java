package org.apache.flink.runtime.executiongraph.dynamic;

import org.apache.flink.runtime.executiongraph.*;

import java.util.ArrayList;

public class PointwiseConnectionStrategy extends ConnectionStrategy {

	public PointwiseConnectionStrategy(IntermediateResult source, ExecutionJobVertex target,
									   int inputNumber, int consumerNumber){
		super(source, target, inputNumber, consumerNumber);
	}

	@Override
	public void connectToSource(ExecutionVertex task) {
		/*final ArrayList<IntermediateResultPartition> sourcePartitions = source.getPartitions();

		final int numSources = sourcePartitions.size();
		final int parallelism = task.getTotalNumberOfParallelSubtasks();
		int subTaskIndex = task.getParallelSubtaskIndex();

		// simple case same number of sources as targets
		if (numSources == parallelism) {
			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions.get(subTaskIndex), task, this.inputNumber) };
		}
		else if (numSources < parallelism) {

			int sourcePartition;

			// check if the pattern is regular or irregular
			// we use int arithmetics for regular, and floating point with rounding for irregular
			if (parallelism % numSources == 0) {
				// same number of targets per source
				int factor = parallelism / numSources;
				sourcePartition = subTaskIndex / factor;
			}
			else {
				// different number of targets per source
				float factor = ((float) parallelism) / numSources;
				sourcePartition = (int) (subTaskIndex / factor);
			}

			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[sourcePartition], task, this.inputNumber) };
		}
		else {
			if (numSources % parallelism == 0) {
				// same number of targets per source
				int factor = numSources / parallelism;
				int startIndex = subTaskIndex * factor;

				ExecutionEdge[] edges = new ExecutionEdge[factor];
				for (int i = 0; i < factor; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[startIndex + i], task, this.inputNumber);
				}
				return edges;
			}
			else {
				float factor = ((float) numSources) / parallelism;

				int start = (int) (subTaskIndex * factor);
				int end = (subTaskIndex == task.getTotalNumberOfParallelSubtasks() - 1) ?
					sourcePartitions.size() :
					(int) ((subTaskIndex + 1) * factor);

				ExecutionEdge[] edges = new ExecutionEdge[end - start];
				for (int i = 0; i < edges.length; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[start + i], task, this.inputNumber);
				}

				return edges;
			}
		}*/
	}

	@Override
	public void connectToTarget(IntermediateResultPartition data) {

	}

	@Override
	public void disconnectFromSource(ExecutionVertex task) {

	}

	@Override
	public void disconnectFromTarget(IntermediateResultPartition data) {

	}
}
