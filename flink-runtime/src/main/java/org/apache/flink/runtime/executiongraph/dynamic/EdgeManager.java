package org.apache.flink.runtime.executiongraph.dynamic;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;

/***/
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
	 * this method most likely called while a new graph was building
	 * */
	public void connectSource(ExecutionVertex target) {
		this.strategy.connectToSource(target);
	}

	/** replace current edge to two edges:
	 * 	1) edge to vertex
	 * 	2) edge from the intermediate data
	 *
	 * 	 * 							IR
	 * 	 * 							 \		-> edgeFromIntermedicateResult
	 * 	 * 							  \
	 * 	 * 							  ______
	 * 	 * 	IR						 | EJV  |
	 * 	 *	|	->  Current Edge =>  | |	|  -> sub graph
	 * 	 * EJV						 | IR	|
	 * 	 * 							 --------
	 * 	 * 							  /
	 * 	 * 							 / 	-> edgeToVertex
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

	public void onPartitionReady(IntermediateResultPartition partition){
		//TODO: this might trigger graph structure change
		//TODO: this might also trigger Consumer parallelism change

	}

	public void onPartitionAdded(IntermediateResultPartition partition){
		this.strategy.connectToTarget(partition);
		//TODO: will target vertex change parallelism?
	}

	public void onPartitionRemoved(IntermediateResultPartition partition){
		this.strategy.disconnectFromTarget(partition);
	}

	/**
	 * Increase/Reduce ExecutionJobVertex parallelism will be triggered by external event
	 * */
	public void onExecutionVertexRemoved(ExecutionVertex target){
		this.strategy.disconnectFromSource(target);
		//TODO: this will always trigger partition remove
	}

	/**
	 * Increase/Reduce ExecutionJobVertex parallelism will be triggered by external event
	 * */
	public void onExecutionVertexAdded(ExecutionVertex vertex){
		this.strategy.connectToSource(vertex);
		//TODO: this will always trigger partition Add
	}
}
