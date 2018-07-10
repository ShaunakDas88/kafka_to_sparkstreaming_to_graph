package com.datastax.kafka_to_sparkstreaming_to_graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, LongType, IntegerType}
import org.apache.spark.sql.Row

import com.datastax.bdp.graph.spark.graphframe._

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

	var prev_vertices: Long = 0
	var prev_edges: Long = 0

	var spark: SparkSession = null
        var graph: DseGraphFrame = null

	var max_rate_per_partition: Int = 10000 

	var metrics_df: DataFrame = null


	def this(graph_name: String, spark: SparkSession, max_rate_per_partition: Int)
        {
                this()
                this.spark = spark
                this.graph = spark.dseGraph(graph_name)
		this.max_rate_per_partition = max_rate_per_partition

		// will store metrics around loading in its own DataFrame
		val schema = StructType(
				StructField("Phase", StringType, true) ::
				StructField("Kafka ingestion rate", IntegerType, true) :: 
				StructField("vertices loaded", LongType, true) :: 
				StructField("vertex loading time (sec)", DoubleType, true) :: 
				StructField("edges loaded", LongType, true) ::
				StructField("edge loading time (sec)", DoubleType, true) ::
				StructField("vertex + edge loading time (sec)", DoubleType, true) :: Nil)
		this.metrics_df = this.spark.createDataFrame(this.spark.sparkContext.emptyRDD[Row], schema)
	}

	def update(phase: String, time: Double, vertex: Boolean)
	{
		this.phases += phase
		this.loading_times += time
		val loaded_vertices: Long  = this.graph.V.count().next()
                val loaded_edges: Long = this.graph.E.count().next()

		var vertex_loading_time: Double = 0
		var edge_loading_time: Double = 0

		if (vertex)
		{
			this.vertex_loading_phases += phase
			this.vertex_loading_times += time
			this.cumulative_vertex_loading_time += time
			vertex_loading_time = time
        	}
		else
		{
			this.edge_loading_phases += phase
			this.edge_loading_times += time
			this.cumulative_edge_loading_time += time
			edge_loading_time = time
		}

		val spark2 = this.spark
        	import spark2.implicits._

		val result_df = Seq(DataRow(phase, this.max_rate_per_partition, loaded_vertices - this.prev_vertices, vertex_loading_time, loaded_edges - this.prev_edges, edge_loading_time, vertex_loading_time + edge_loading_time)).toDF("Phase", "Kafka ingestion rate", "vertices loaded", "vertex loading time (sec)", "edges loaded", "edge loading time (sec)", "vertex + edge loading time (sec)")
		this.metrics_df = this.metrics_df.union(result_df)

		// update these counts
		this.prev_vertices = loaded_vertices
		this.prev_edges = loaded_edges
	}
}
