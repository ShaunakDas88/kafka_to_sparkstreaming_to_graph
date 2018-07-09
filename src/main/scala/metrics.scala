package com.datastax.kafka_to_sparkstreaming_to_graph

import scala.collection.mutable.ListBuffer

class Metrics
{
	//  these two lists will coincide with each other
	var phases: ListBuffer[String] = new ListBuffer[String]()
	var loading_times: ListBuffer[Double] = new ListBuffer[Double]()

	// these two lists will coincide with each other
	var vertex_loading_phases: ListBuffer[String] = new ListBuffer[String]()
	var vertex_loading_times: ListBuffer[Double] = new ListBuffer[Double]()

	// these two lists will coincide with each other
	var edge_loading_phases: ListBuffer[String] = new ListBuffer[String]()
	var edge_loading_times: ListBuffer[Double] = new ListBuffer[Double]()
	
	var cumulative_vertex_loading_time: Double = 0
	var cumulative_edge_loading_time: Double = 0

	def update(phase: String, time: Double, vertex: Boolean)
	{
		this.phases += phase
		this.loading_times += time
		if (vertex)
		{
			this.vertex_loading_phases += phase
			this.vertex_loading_times += time
			this.cumulative_vertex_loading_time += time
		}
		else
		{
			this.edge_loading_phases += phase
			this.edge_loading_times += time
			this.cumulative_edge_loading_time += time
		}
	}
}
