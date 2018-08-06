package org.apache.flink.runtime.executiongraph.dynamic;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;


/** replace current edge to two edges:
 * 	1) edge to vertex
 * 	2) edge from the intermediate data
 *
 * 							IR
 * 							 \		-> edgeFromIntermedicateResult
 * 							  \
 * 							  ______
 * 	IR						 | EJV  |
 *	|	->  Current Edge =>  | |	|  -> sub graph
 * EJV						 | IR	|
 * 							 --------
 * 							  /
 * 							 / 		-> edgeToVertex
 * 							 EJV
 *
 * */
public class SubExecutionGraph {

	public ExecutionJobVertex inputEJV;

	public IntermediateResult outputIR;

	private ExecutionGraph fullGraph;

	protected JobEdge connect(ExecutionJobVertex executionJobVertex,
							  IntermediateResult intermediateResult, DistributionPattern pattern) throws JobException{
		JobVertex jobVertex = executionJobVertex.getJobVertex();
		JobEdge jobEdge = jobVertex.connectIdInput(intermediateResult.getId(), pattern);
		executionJobVertex.connectPredecessor(this.fullGraph.getAllIntermediateResults(), jobEdge);
		return jobEdge;
	}

	public JobEdge connectToVertex(ExecutionJobVertex executionJobVertex) throws JobException {
		return this.connect(executionJobVertex, this.outputIR, DistributionPattern.ALL_TO_ALL);
	}

	public JobEdge connectToIntermediateResult(IntermediateResult intermediateResult) throws JobException{
		return this.connect(this.inputEJV, intermediateResult, DistributionPattern.ALL_TO_ALL);
	}
}
