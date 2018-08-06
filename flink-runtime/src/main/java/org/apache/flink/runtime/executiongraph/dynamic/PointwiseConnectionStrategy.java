package org.apache.flink.runtime.executiongraph.dynamic;

import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;

public class PointwiseConnectionStrategy extends ConnectionStrategy {

	public PointwiseConnectionStrategy(IntermediateResult source){
		super(source);
	}

	@Override
	public ExecutionEdge[] connectSource(ExecutionVertex target, int inputNumber) {
		final IntermediateResultPartition[] sourcePartitions = source.getPartitions();

		final int numSources = sourcePartitions.length;
		final int parallelism = target.getTotalNumberOfParallelSubtasks();
		int subTaskIndex = target.getParallelSubtaskIndex();

		// simple case same number of sources as targets
		if (numSources == parallelism) {
			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[subTaskIndex], target, inputNumber) };
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

			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[sourcePartition], target, inputNumber) };
		}
		else {
			if (numSources % parallelism == 0) {
				// same number of targets per source
				int factor = numSources / parallelism;
				int startIndex = subTaskIndex * factor;

				ExecutionEdge[] edges = new ExecutionEdge[factor];
				for (int i = 0; i < factor; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[startIndex + i], target, inputNumber);
				}
				return edges;
			}
			else {
				float factor = ((float) numSources) / parallelism;

				int start = (int) (subTaskIndex * factor);
				int end = (subTaskIndex == target.getTotalNumberOfParallelSubtasks() - 1) ?
					sourcePartitions.length :
					(int) ((subTaskIndex + 1) * factor);

				ExecutionEdge[] edges = new ExecutionEdge[end - start];
				for (int i = 0; i < edges.length; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[start + i], target, inputNumber);
				}

				return edges;
			}
		}
	}
}
