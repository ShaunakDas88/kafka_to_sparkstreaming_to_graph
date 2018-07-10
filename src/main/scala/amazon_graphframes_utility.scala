package com.datastax.kafka_to_sparkstreaming_to_graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.apache.tinkerpop.gremlin.driver.Cluster

import com.datastax.bdp.graph.spark.graphframe._

class AmazonGraphFramesUtility 
{
	var spark: SparkSession = null
	var graph: DseGraphFrame = null
	var graph_name: String = null

	var metrics: Metrics = null

	def this(graph_name: String, spark: SparkSession, metrics: Metrics)
	{
		this()
		this.graph_name = graph_name
		this.spark = spark
		this.graph = spark.dseGraph(this.graph_name)
		this.metrics = metrics
	}

	def load_item_vertices(metadata_df: DataFrame) 
	{
		var start = System.nanoTime()
		metadata_df.rdd.count
		var duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("Item vertex loading: count", duration, true)

		start = System.nanoTime()
		metadata_df.withColumn("~label", lit("Item")).withColumnRenamed("asin", "_id").rdd.count
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("Item vertex loading: transform", duration, true)

		start = System.nanoTime()
		this.graph.updateVertices(metadata_df.withColumn("~label", lit("Item")).withColumnRenamed("asin", "_id"))
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("Item vertex loading", duration, true)
	}

	def load_customer_vertices(reviews_df: DataFrame) 
	{
		var start = System.nanoTime()
		reviews_df.rdd.count
		var duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("Customer vertex loading: count", duration, true)		

		start = System.nanoTime()
		reviews_df.withColumn("~label", lit("Customer")).withColumnRenamed("reviewerID", "_id").withColumnRenamed("reviewerName", "name").rdd.count
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("Customer vertex loading: transformg", duration, true)

		start = System.nanoTime()
		this.graph.updateVertices(reviews_df.withColumn("~label", lit("Customer")).withColumnRenamed("reviewerID", "_id").withColumnRenamed("reviewerName", "name"))
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("Customer vertex loading", duration, true)
	}

	def load_category_vertices(metadata_df: DataFrame) 
	{
		var start = System.nanoTime()
		metadata_df.rdd.count
		var duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("Category vertex loading: count", duration, true)

		start = System.nanoTime()
		val categories_df = metadata_df.select(col("asin"), explode(col("categories"))).select(col("asin"), explode(col("col")))
		categories_df.rdd.count
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("Category vertex loading: transform", duration, true)

		start = System.nanoTime()
		this.graph.updateVertices(categories_df.withColumn("~label", lit("Category")).withColumnRenamed("col", "_id"))
                duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("Category vertex loading", duration, true)
	}

	def load_reviewed_edges(reviews_df: DataFrame) 
	{
		var start = System.nanoTime()
		reviews_df.rdd.count
		var duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("reviewed edge loading: count", duration, false)
		
		start = System.nanoTime()
		val e = reviews_df.select(this.graph.idColumn(lit("Customer"), col("reviewerID")) as "src", this.graph.idColumn(lit("Item"), col("asin")) as "dst",  lit("reviewed") as "~label")
		e.rdd.count
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("reviewed edge loading: transform", duration, false)

		start = System.nanoTime()
		this.graph.updateEdges(e)
                duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("reviewed edge loading", duration, false)
	}

	def load_belongs_in_category_edges(metadata_df: DataFrame) 
	{
		var start = System.nanoTime()
		metadata_df.rdd.count
		var duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("belongs_in_category edge loading: count", duration, false)

		start = System.nanoTime()
		val categories_df = metadata_df.select(col("asin"), explode(col("categories"))).select(col("asin"), explode(col("col")))
		val e = categories_df.select(this.graph.idColumn(lit("Item"), col("asin")) as "src", this.graph.idColumn(lit("Category"), col("col")) as "dst",  lit("belongs_in_category") as "~label")
		e.rdd.count
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("belongs_in_category edge loading: transform", duration, false)


		start = System.nanoTime()
		this.graph.updateEdges(e)
                duration = (System.nanoTime() - start)/scala.math.pow(10,9)
                this.metrics.update("belongs_in_category edge loading", duration, false)
	}

	def load_Item_to_Item_edges(df: DataFrame, label: String) 
	{
		// need to make sure the adjacent vertices are loaded
		var start = System.nanoTime()
		this.graph.updateVertices(df.withColumn("~label", lit("Item")).withColumnRenamed("col", "_id"))
		var duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update(label + " vertex loading", duration, true)

		start = System.nanoTime()
		// now we can load the edges
		val e = df.select(this.graph.idColumn(lit("Item"), col("asin")) as "src", this.graph.idColumn(lit("Item"), col("col")) as "dst",  lit(label) as "~label")
		this.graph.updateEdges(e)
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update(label + " edge loading", duration, false)
	}

	def load_viewed_with_edges(metadata_df: DataFrame) 
	{
		var start = System.nanoTime()
		metadata_df.rdd.count
		var duration = (System.nanoTime() - start)/scala.math.pow(10,9)
		this.metrics.update("viewed_with: count", duration, false)

		start = System.nanoTime()
		val viewed_with_df = metadata_df.select(col("asin"), explode(col("related.also_viewed")))
		viewed_with_df.rdd.count
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
                this.metrics.update("viewed_with: transform", duration, false)

		this.load_Item_to_Item_edges(viewed_with_df, "viewed_with")
	}

	def load_also_bought_edges(metadata_df: DataFrame) 
	{
		var start = System.nanoTime()
                metadata_df.rdd.count
                var duration = (System.nanoTime() - start)/scala.math.pow(10,9)
                this.metrics.update("also_bought: count", duration, false)

		start = System.nanoTime()
		val also_bought_df = metadata_df.select(col("asin"), explode(col("related.also_bought")))
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
                this.metrics.update("also_bought: transform", duration, false)

		this.load_Item_to_Item_edges(also_bought_df, "also_bought")
	}

	def load_bought_after_viewing_edges(metadata_df: DataFrame) 
	{
		var start = System.nanoTime()
                metadata_df.rdd.count
                var duration = (System.nanoTime() - start)/scala.math.pow(10,9)
                this.metrics.update("bought_after_viewing: count", duration, false)

		start = System.nanoTime()
		val bought_after_viewing_df = metadata_df.select(col("asin"), explode(col("related.buy_after_viewing")))
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
                this.metrics.update("bought_after_viewing: transform", duration, false)

		this.load_Item_to_Item_edges(bought_after_viewing_df, "bought_after_viewing")
	}
	
	def load_purchased_with_edges(metadata_df: DataFrame) 
	{
		var start = System.nanoTime()
                metadata_df.rdd.count
                var duration = (System.nanoTime() - start)/scala.math.pow(10,9)
                this.metrics.update("purchased_with: count", duration, false)
	
		start = System.nanoTime()
		val purchased_with_df = metadata_df.select(col("asin"), explode(col("related.bought_together")))
		duration = (System.nanoTime() - start)/scala.math.pow(10,9)
                this.metrics.update("purchased_with: transform", duration, false)

		this.load_Item_to_Item_edges(purchased_with_df, "purchased_with")
	}
}
