package org.apache.flink.runtime.executiongraph.dynamic;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;

public abstract class EdgeManager {

	protected JobEdge jobEdge;

	protected ExecutionGraph executionGraph;

	protected ConnectionStrategy strategy;

	public EdgeManager(ExecutionGraph graph, JobEdge edge) {
		this.jobEdge = edge;
		this.executionGraph = graph;
	}

	/**
	 * connect vertex to source by the given strategy
	 * */
	public ExecutionEdge[] connectSource(ExecutionVertex target, int inputNumber, int comsumeNumber) {

		ExecutionEdge[] edges = this.strategy.connectSource(target, inputNumber);

		// add the consumers to the source
		// for now (until the receiver initiated handshake is in place), we need to register the
		// edges as the execution graph
		for (ExecutionEdge ee : edges) {
			ee.getSource().addConsumer(ee, comsumeNumber);
		}

		target.setExecutionEdges(inputNumber, edges);

		return edges;
	}

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
	 * 							 / 	-> edgeToVertex
	 * 							 EJV
	 *
	 * */
	protected void replaceEdgeWithSubgraph() throws JobException {

		SubExecutionGraph graph = this.createReplacementSubGraph();

		//lock
		JobVertex currentVertex = this.jobEdge.getTarget();
		IntermediateDataSet currentDataset = this.jobEdge.getSource();
		ExecutionJobVertex currentEJV = this.executionGraph.getJobVertex(this.jobEdge.getTarget().getID());
		IntermediateResult currentIR = this.executionGraph.getAllIntermediateResults().get(this.jobEdge.getSourceId());

		//Disconnect
		currentVertex.disconnectDataSet(this.jobEdge);
		currentDataset.removeComsumer(this.jobEdge);
		currentEJV.disconnectPredecessor(this.jobEdge);
		currentIR.deregisterConsumer(this.jobEdge);

		//connect
		graph.connectToVertex(currentEJV);
		graph.connectToIntermediateResult(currentIR);

		//unlock
	}

	//---------------------------------------------------------------------------------------------
	//  Handle dynamic changes
	//---------------------------------------------------------------------------------------------

	protected SubExecutionGraph createReplacementSubGraph(){
		return null;
	}

	public void onPartitionProduced(IntermediateResultPartition partition){

	}

	public void onPartitionAdded(IntermediateResultPartition partition){

	}

	public void onPartitionRemoved(IntermediateResultPartition partition){

	}

	public void onExecutionVertexRemoved(ExecutionVertex vertex){

	}

	public void onExecutionVertexAdded(ExecutionVertex vertex){

	}
}
