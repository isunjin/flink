package org.apache.flink.runtime.executiongraph.dynamic;

import org.apache.flink.runtime.executiongraph.*;

/**
 * the connection strategy, to manage connections between intermediate result and job vertex
 */
public abstract class ConnectionStrategy {

	protected final IntermediateResult source;

	protected final ExecutionJobVertex target;

	/**the input number in execution job vertex*/
	protected final int inputNumber;

	 /**consumer number in IntermediateResultPartition*/
	protected final int consumerNumber;

	/**constructor*/
	public ConnectionStrategy(IntermediateResult source, ExecutionJobVertex target, int inputNumber, int consumerNumber) {
		this.source = source;
		this.target = target;
		this.inputNumber = inputNumber;
		this.consumerNumber = consumerNumber;
	}

	// --------------------------------------------------------------------------------------------
	// interface to manager connections

	public abstract void connectToSource(ExecutionVertex task);

	public abstract void connectToTarget(IntermediateResultPartition data);

	public abstract void disconnectFromSource(ExecutionVertex task);

	public abstract void disconnectFromTarget(IntermediateResultPartition data);

}
