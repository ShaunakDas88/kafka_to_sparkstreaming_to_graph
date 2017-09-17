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

	def this(graph_name: String, spark: SparkSession)
	{
		this()
		this.graph_name = graph_name
		this.spark = spark
		this.create_schema()
		this.graph = spark.dseGraph(this.graph_name)
	}

	def load_item_vertices(metadata_df: DataFrame) {
		this.graph.updateVertices(metadata_df.withColumn("~label", lit("Item")).withColumnRenamed("asin", "_id"))
	}

	def load_customer_vertices(reviews_df: DataFrame) {
		this.graph.updateVertices(reviews_df.withColumn("~label", lit("Customer")).withColumnRenamed("reviewerID", "_id").withColumnRenamed("reviewerName", "name"))
	}

	def load_category_vertices(metadata_df: DataFrame) {
		val categories_df = metadata_df.select(col("asin"), explode(col("categories"))).select(col("asin"), explode(col("col")))
		this.graph.updateVertices(categories_df.withColumn("~label", lit("Category")).withColumnRenamed("col", "_id"))
	}

	def load_reviewed_edges(reviews_df: DataFrame) {
		val e = reviews_df.select(this.graph.idColumn(lit("Customer"), col("reviewerID")) as "src", this.graph.idColumn(lit("Item"), col("asin")) as "dst",  lit("reviewed") as "~label")
		this.graph.updateEdges(e)
	}

	def load_belongs_in_category_edges(metadata_df: DataFrame) {
		val categories_df = metadata_df.select(col("asin"), explode(col("categories"))).select(col("asin"), explode(col("col")))
		val e = categories_df.select(this.graph.idColumn(lit("Item"), col("asin")) as "src", this.graph.idColumn(lit("Category"), col("col")) as "dst",  lit("belongs_in_category") as "~label")
		this.graph.updateEdges(e)
	}

        /*def load_has_salesRank_edges(metadata_df: DataFrame) {
                val has_salesRank_df = metadata_df.select(col("asin"), explode(col("salesRankrelated.also_viewed")))
  
        }*/

	def load_Item_to_Item_edges(df: DataFrame, label: String) {
		// need to make sure the other vertices are loaded
		this.graph.updateVertices(df.withColumn("~label", lit("Item")).withColumnRenamed("col", "_id"))
		// now we can load the edges
		val e = df.select(this.graph.idColumn(lit("Item"), col("asin")) as "src", this.graph.idColumn(lit("Item"), col("col")) as "dst",  lit(label) as "~label")
		this.graph.updateEdges(e)
	}

	def load_viewed_with_edges(metadata_df: DataFrame) {
		val viewed_with_df = metadata_df.select(col("asin"), explode(col("related.also_viewed")))
		this.load_Item_to_Item_edges(viewed_with_df, "viewed_with")
	}

	def load_also_bought_edges(metadata_df: DataFrame) {
		val also_bought_df = metadata_df.select(col("asin"), explode(col("related.also_bought")))
		this.load_Item_to_Item_edges(also_bought_df, "also_bought")
	}

	def load_bought_after_viewing_edges(metadata_df: DataFrame) {
		val bought_after_viewing_df = metadata_df.select(col("asin"), explode(col("related.buy_after_viewing")))
		this.load_Item_to_Item_edges(bought_after_viewing_df, "bought_after_viewing")
	}
	
	def load_purchased_with_edges(metadata_df: DataFrame) {
		val purchased_with_df = metadata_df.select(col("asin"), explode(col("related.bought_together")))
		this.load_Item_to_Item_edges(purchased_with_df, "purchased_with")
	}

	def create_schema() {
		val cluster:org.apache.tinkerpop.gremlin.driver.Cluster = org.apache.tinkerpop.gremlin.driver.Cluster.build("localhost").create()
		val gremlin_client:org.apache.tinkerpop.gremlin.driver.Client = cluster.connect()
		gremlin_client.submit("if(system.graph('%s').exists()){ system.graph('%s').drop() }".format(this.graph_name, this.graph_name))
		gremlin_client.submit("system.graph('%s').create()".format(this.graph_name)).all.get
		val aliased_client = gremlin_client.alias("%s.g".format(this.graph_name))
		aliased_client.submit("schema.clear()")
		aliased_client.submit("""
				schema.config().option('graph.schema_mode').set(com.datastax.bdp.graph.api.model.Schema.Mode.Production);

				schema.propertyKey('summary').Text().single().create()
				schema.propertyKey('timestampAsText').Text().single().create()
				schema.propertyKey('answerType').Text().single().create()
				schema.propertyKey('rating').Double().single().create()
				schema.propertyKey('description').Text().single().create()
				schema.propertyKey('title').Text().single().create()
				schema.propertyKey('imUrl').Text().single().create()
				schema.propertyKey('name').Text().single().create()
				schema.propertyKey('answer').Text().single().create()
				schema.propertyKey('price').Double().single().create()
				schema.propertyKey('rank').Int().single().create()
				schema.propertyKey('id').Text().single().create()
				schema.propertyKey('helpful').Double().single().create()
				schema.propertyKey('brand').Text().single().create()
				schema.propertyKey('reviewText').Text().single().create()
				schema.propertyKey('timestamp').Timestamp().single().create()


				schema.vertexLabel('Item').partitionKey("id").properties('price', 'title', 'imUrl', 'description', 'brand').create()
				schema.vertexLabel('Category').partitionKey('id').create()
				schema.vertexLabel('Customer').partitionKey('id').properties('name').create()

				schema.edgeLabel('viewed_with').connection('Item', 'Item').create()
				schema.edgeLabel('also_bought').connection('Item', 'Item').create()
				schema.edgeLabel('reviewed').properties('summary', 'reviewText', 'timestampAsText', 'timestamp', 'helpful', 'rating').connection('Customer', 'Item').create()
				schema.edgeLabel('purchased_with').connection('Item', 'Item').create()
				schema.edgeLabel('belongs_in_category').connection('Item', 'Category').create()
				schema.edgeLabel('has_salesRank').properties('rank').connection('Item', 'Category').create()
				schema.edgeLabel('bought_after_viewing').connection('Item', 'Item').create()
			  """).all.get
		// close up our connection
		cluster.close()
	}
}
