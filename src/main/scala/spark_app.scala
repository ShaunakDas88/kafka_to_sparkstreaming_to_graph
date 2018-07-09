package com.datastax.kafka_to_sparkstreaming_to_graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, LongType, IntegerType}
import org.apache.spark.sql.Row

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.dstream.DStream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable.ArrayBuffer


case class DataRow(field1: String, field2: Int, field3: Long, field4: Double, field5: Long, field6: Double, field7: Double)

object AmazonStreamedFromKafka
{
	var spark: SparkSession = null
	var ssc: StreamingContext = null
	var graphframes_utility: AmazonGraphFramesUtility = null
	var max_rate_per_partition: Int = 10000
	var max_empty_windows: Int = 1
	var empty_metadata_windows = 0
	var empty_review_windows = 0
	var metrics_df: DataFrame = null

	
	def get_DStream(topics: Array[String], kafka_params: Map[String,Object]) : DStream[ConsumerRecord[String,String]] =
	{
		val dstream = KafkaUtils.createDirectStream[String, String](
					ssc,
					PreferConsistent,
					Subscribe[String, String](topics, kafka_params))
		return dstream
	}

	def load_from_DStream(dstream: DStream[ConsumerRecord[String,String]], data_source: String)
	{
		dstream.map(record => record.value)
			.foreachRDD( rdd => {
				if(!rdd.isEmpty)
				{
					//rdd.collect().foreach(println)
					val true_df = this.spark.read.json(rdd)
					//true_df.printSchema

					// this is data-set specific
					if(data_source == "metadata")
					{
						// reset our counter to 0
						this.empty_metadata_windows = 0
					
						// load relevant vertices, record metrics
						this.graphframes_utility.load_item_vertices(true_df)
						this.graphframes_utility.load_category_vertices(true_df)

						// load relevant edges, record metrics
                                                this.graphframes_utility.load_belongs_in_category_edges(true_df)
                                                this.graphframes_utility.load_viewed_with_edges(true_df)
                                                this.graphframes_utility.load_also_bought_edges(true_df)
                                                //this.graphframes_utility.load_bought_after_viewing_edges(true_df)
                                                this.graphframes_utility.load_purchased_with_edges(true_df)
					}
					else if(data_source == "reviews")
					{
						// reset our counter to 0
						this.empty_review_windows = 0

						// load relevant vertices, record metrics
						this.graphframes_utility.load_customer_vertices(true_df)
						
						// load relevant edges, record metrics
						this.graphframes_utility.load_reviewed_edges(true_df)
					}
				}
				else
				{
					if (data_source == "metadata")
					{
						this.empty_metadata_windows += 1
					}
					if (data_source == "reviews")
					{
						this.empty_review_windows += 1
					}
					if (scala.math.min(this.empty_metadata_windows, this.empty_review_windows) == this.max_empty_windows)
					{
						// want to display metrics
						this.get_metrics("Final")
						this.metrics_df.show
						// graceful shutdown of Spark streaming context
                                                System.err.println("Shutting down Spark streaming context!")
						this.ssc.stop(true)
						this.spark.stop()
					}
				}
			})
	}

	def get_metrics(phase: String)
	{
		val spark2 = this.spark
		import spark2.implicits._

		val vertex_loading_time = this.graphframes_utility.metrics.cumulative_vertex_loading_time
                val edge_loading_time = this.graphframes_utility.metrics.cumulative_edge_loading_time
                val loaded_vertices = graphframes_utility.graph.V.count().next()
                val loaded_edges = graphframes_utility.graph.E.count().next()

                val result_df = Seq(DataRow(phase, this.max_rate_per_partition, loaded_vertices, vertex_loading_time, loaded_edges, edge_loading_time, vertex_loading_time + edge_loading_time)).toDF("Phase", "Kafka ingestion rate", "cumulative vertices loaded", "cumulative vertex loading time (sec)", "cumulative edges loaded", "cumulative edge loading time (sec)", "cumulative vertex + edge loading time (sec)")
		this.metrics_df = this.metrics_df.union(result_df)
	}

	def main(args: Array[String])
	{
		// command-line arguments
	        val graph_name = args(0)
                val item_topic = Array(args(1))
                val review_topic = Array(args(2))
		this.max_rate_per_partition = args(3).toInt
		this.max_empty_windows = args(4).toInt
		
		this.spark = SparkSession.builder.appName("Amazon: Kafka -> SparkStreaming -> DseGraph").config("spark.streaming.kafka.maxRatePerPartition", this.max_rate_per_partition).getOrCreate()
		this.ssc = new StreamingContext(this.spark.sparkContext, Seconds(1))
		this.graphframes_utility = new AmazonGraphFramesUtility(graph_name, this.spark)

		// will store metrics around loading in its own DataFrame
		val schema = StructType(
				StructField("Phase", StringType, true) ::
				StructField("Kafka ingestion rate", IntegerType, true) :: 
				StructField("cumulative vertices loaded", LongType, true) :: 
				StructField("cumulative vertex loading time (sec)", DoubleType, true) :: 
				StructField("cumulative edges loaded", LongType, true) ::
				StructField("cumulative edge loading time (sec)", DoubleType, true) ::
				StructField("cumulative vertex + edge loading time (sec)", DoubleType, true) :: Nil)
		this.metrics_df = this.spark.createDataFrame(this.spark.sparkContext.emptyRDD[Row], schema)
		this.get_metrics("Start")

		// these will be Kafka-specific
		val kafka_params = Map[String, Object](
			"bootstrap.servers" -> "localhost:9092,anotherhost:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "use_a_separate_group_id_for_each_stream",
			"auto.offset.reset" -> "earliest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		// get DStreams, one for each Kafka topic
		val item_dstream = this.get_DStream(item_topic, kafka_params)
		val reviews_dstream = this.get_DStream(review_topic, kafka_params)

		this.load_from_DStream(item_dstream, "metadata")
		this.load_from_DStream(reviews_dstream, "reviews")

		this.ssc.start()
		// wait for graceful showdown of Spark streaming context before proceeding forward
		this.ssc.awaitTermination()
	}
}
