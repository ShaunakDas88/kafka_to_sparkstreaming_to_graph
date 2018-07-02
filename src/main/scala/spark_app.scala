package com.datastax.kafka_to_sparkstreaming_to_graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.dstream.DStream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

object AmazonStreamedFromKafka
{
	var spark: SparkSession = null
	var ssc: StreamingContext = null
	var graphframes_utility: AmazonGraphFramesUtility = null

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
				val true_df = this.spark.read.json(rdd)
				true_df.printSchema()

				// this is data-set specific
				if(data_source == "metadata")
				{
					// load relevant vertices
					this.graphframes_utility.load_item_vertices(true_df)
					this.graphframes_utility.load_category_vertices(true_df)

					// load relevant edges
					this.graphframes_utility.load_belongs_in_category_edges(true_df)
					this.graphframes_utility.load_viewed_with_edges(true_df)
					//this.graphframes_utility.load_has_salesRank_edges(true_df)
					this.graphframes_utility.load_also_bought_edges(true_df)
					this.graphframes_utility.load_bought_after_viewing_edges(true_df)
					this.graphframes_utility.load_purchased_with_edges(true_df)
				}
				else if(data_source == "reviews")
				{
					// load relevant vertices
					this.graphframes_utility.load_customer_vertices(true_df)
					
					// load relevant edges
					this.graphframes_utility.load_reviewed_edges(true_df)
				}
			})
	}

	def main(args: Array[String])
	{
		this.spark = SparkSession.builder.appName("Amazon: Kafka -> SparkStreaming -> DseGraph").getOrCreate() 
		this.ssc = new StreamingContext(this.spark.sparkContext, Seconds(1))
		val graph_name = args(0)
		this.graphframes_utility = new AmazonGraphFramesUtility(graph_name, this.spark)

		// these will be Kafka-specific
		val item_topic = Array(args(1))
		val review_topic = Array(args(2))
		val kafka_params = Map[String, Object](
			"bootstrap.servers" -> "localhost:9092,anotherhost:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "use_a_separate_group_id_for_each_stream",
			"auto.offset.reset" -> "earliest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		val item_dstream = this.get_DStream(item_topic, kafka_params)
		val reviews_dstream = this.get_DStream(review_topic, kafka_params)

		this.load_from_DStream(item_dstream, "metadata")
		this.load_from_DStream(reviews_dstream, "reviews")

		this.ssc.start()
		this.ssc.awaitTermination()
		this.spark.stop()
	}
}
