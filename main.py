from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise2").getOrCreate()

products_df = spark.read.option("header", "true").csv("data/product.csv")
categories_df = spark.read.option("header", "true").csv("data/category.csv")
relations_df = spark.read.option("header", "true").csv("data/relation.csv")

joined_df = products_df.join(relations_df, on="product_id", how="left") \
                       .join(categories_df, on="category_id", how="left") \
                       .select("product", "category")
joined_df.show()