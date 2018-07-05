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

import scala.collection.mutable.ArrayBuffer


object AmazonStreamedFromKafka
{
	var spark: SparkSession = null
	var ssc: StreamingContext = null
	var graphframes_utility: AmazonGraphFramesUtility = null
	var max_empty_windows: Integer = 1
	var empty_metadata_windows = 0
	var empty_review_windows = 0

	
	def get_DStream(topics: Array[String], kafka_params: Map[String,Object]) : DStream[ConsumerRecord[String,String]] =
	{
		val dstream = KafkaUtils.createDirectStream[String, String](
					ssc,
					PreferConsistent,
					Subscribe[String, String](topics, kafka_params))
		return dstream
	}

	def load_vertices_from_DStream(dstream: DStream[ConsumerRecord[String,String]], data_source: String)
	{
		dstream.map(record => record.value)
			.foreachRDD( rdd => {
				if(!rdd.isEmpty)
				{
					val true_df = this.spark.read.json(rdd)
					true_df.printSchema

					// this is data-set specific
					if(data_source == "metadata")
					{
						this.empty_metadata_windows = 0
						// load relevant vertices
						this.graphframes_utility.load_item_vertices(true_df)
						this.graphframes_utility.load_category_vertices(true_df)
					}
					else if(data_source == "reviews")
					{
						this.empty_review_windows = 0
						// load relevant vertices
						this.graphframes_utility.load_customer_vertices(true_df)
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
						this.ssc.stop()
                                                this.spark.stop()
					}
				}
			})
	}

	def load_edges_from_DStream(dstream: DStream[ConsumerRecord[String,String]], data_source: String)
        {
                dstream.map(record => record.value)
                        .foreachRDD( rdd => {
                                if(!rdd.isEmpty)
                                {
                                        val true_df = this.spark.read.json(rdd)
                                        true_df.printSchema

                                        // this is data-set specific
                                        if(data_source == "metadata")
                                        {
                                                // load relevant edges
                                                this.graphframes_utility.load_belongs_in_category_edges(true_df)
                                                this.graphframes_utility.load_viewed_with_edges(true_df)
                                                this.graphframes_utility.load_also_bought_edges(true_df)
                                                this.graphframes_utility.load_bought_after_viewing_edges(true_df)
                                                this.graphframes_utility.load_purchased_with_edges(true_df)
                                        }
                                        else if(data_source == "reviews")
                                        {                                         
                                                // load relevant edges
                                                this.graphframes_utility.load_reviewed_edges(true_df)
                                        }
                                }
                        })
	}

	def main(args: Array[String])
	{
		// command-line arguments
	        val graph_name = args(0)
                val item_topic = Array(args(1))
                val review_topic = Array(args(2))
		val max_rate_per_partition = args(3).toInt
		this.max_empty_windows = args(4).toInt
		
		this.spark = SparkSession.builder.appName("Amazon: Kafka -> SparkStreaming -> DseGraph").config("spark.streaming.kafka.maxRatePerPartition", max_rate_per_partition).getOrCreate()
		this.ssc = new StreamingContext(this.spark.sparkContext, Seconds(1))
		this.graphframes_utility = new AmazonGraphFramesUtility(graph_name, this.spark)

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

		this.load_vertices_from_DStream(item_dstream, "metadata")
		this.load_edges_from_DStream(item_dstream, "metadata")

		this.load_vertices_from_DStream(reviews_dstream, "reviews")
		this.load_edges_from_DStream(reviews_dstream, "reviews")

		//val true_df = this.spark.read.json("file:///home/automaton/kafka_to_sparkstreaming_to_graph/amazon_data/meta_Grocery_and_Gourmet_Food.json").filter("asin is not null")
		
                //this.graphframes_utility.load_item_vertices(true_df)
                //this.graphframes_utility.load_category_vertices(true_df)
                // load relevant edges
                //this.graphframes_utility.load_belongs_in_category_edges(true_df)
                //this.graphframes_utility.load_viewed_with_edges(true_df)
                //this.graphframes_utility.load_also_bought_edges(true_df)
                //this.graphframes_utility.load_bought_after_viewing_edges(true_df)
                //this.graphframes_utility.load_purchased_with_edges(true_df)

		//true_df = this.spark.read.json("file:///home/automaton/kafka_to_sparkstreaming_to_graph/amazon_data/reviews_Grocery_and_Gourmet_Food.json").filter("reviewerID is not null")
		// load relevant vertices
		//this.graphframes_utility.load_customer_vertices(true_df)
		// load relevant edges
		//this.graphframes_utility.load_reviewed_edges(true_df)

		this.ssc.start()
	}
}
