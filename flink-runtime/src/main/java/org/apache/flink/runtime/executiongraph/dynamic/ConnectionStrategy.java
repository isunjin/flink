package org.apache.flink.runtime.executiongraph.dynamic;

import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;

public abstract class ConnectionStrategy {

	protected IntermediateResult source;

	public ConnectionStrategy(IntermediateResult source){
		this.source = source;
	}

	public abstract ExecutionEdge[] connectSource(ExecutionVertex target, int inputNumber);
}
