package playground
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, lit, max, struct, when,count}

object DeltaPlayground extends App {
    val spark = SparkSession.builder().
      .appName("Spark Essentials Playground App")
      .master("local[6]")
      .config("spark.sql.broadcastTimeout", "36000")
      .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider" )
      .config("fs.s3a.access.key", "ASIAWBZADJJHALGFMOS2")
      .config("fs.s3a.secret.key", "vIzCS/3vFn9boNFIspoHauzYVt751irehzDkgxIb")
      .config("fs.s3a.session.token","FwoGZXIvYXdzEAAaDJJxlRQrDNuhne5HXyLdATXvIuxq2juFWVaxGWjWH+34GjpRvamnQfCkH2c0hIScbBO5YNn1hI1Jq1kJCuhYey3qP8pZYjiJPsl2M5JWfFBo9xg8FcbwAs23lbzE1rQKr+5FJ8GsizMWWV6vD1r1sZdT9WIlgY45uXX8NbuPYL2nF0VFKQIHPeVUTJ9QV0V/8gjzWSoVUWjs7b7tcdYmeYDJ04V3Zj1wgowF0I8rs9vtrVuzNaGKyUYT9phogKFkIyZN671sg4w6fwBkeeiiNR5VbYRJDs6HrkDuENx9BlokoSHAkrwkgkhB9MSIKI7AhP8FMjInz2bWjLJhk2QJA0pPlqUCEGt0+vafj3+guD6beayPdFqU6bh4SIgydl+eqNzz53CQ0g==")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .config("spark.databricks.delta.schema.autoMerge.enabled","true")
      .getOrCreate()
    spark.conf.set("spark.sql.streaming.schemaInference", true)
    spark.sparkContext.setLogLevel("ERROR")

    val topic = "USERS"

    val data = spark.read.parquet(s"s3a://gm-analytics-storage/GLCCD/$topic/")
      .withColumn("deleted",lit(col("Op") === 'D'))

    if (data.columns.contains("Op")) {
        val writeDeltaTable =  data.filter(col("Op").isNull)
          .select(col("Op"),col("deleted"),
              struct(col("ID")).as("key"),
              struct(data.drop("Op").columns.head, data.drop("Op").columns.tail: _*).as("value"))


       // writeDeltaTable.write.format("delta").save(s"s3a://gm-analytics-storage/Delta_Tables/$topic/")

        val operationsDf = data.filter(col("Op").isNotNull)
          .select(col("Op"),
              struct(col("ID")).as("key"),
              struct(data.drop("Op").columns.head, data.drop("Op").columns.tail: _*).as("value"))

        val updatesdf = operationsDf.filter(lit(col("Op") === 'U'))
          .groupBy("key","Op")
          .agg(max("value").as("value"))
          .selectExpr("Op","key", "value")

        val unionDf = operationsDf.filter(lit(col("Op") === 'D').or(lit(col("Op") === 'I')))
          .union(updatesdf)
          .orderBy("value.cdcTime")
        // unionDf.createOrReplaceTempView("dataframe") --usar quando tiver usando o SQL puro

        println("UnionDF")
        unionDf             //Tem I, D e últimos U
          .selectExpr("Op","key", "value.*")
          .where(col("value.AKEY") === "testando" )
          .show()

        //-- FORMATO EM SQL PURO ESTÁ COMENTADO

        //    val deltaTable = spark.read.format("delta").load(s"s3a://gm-analytics-storage/Delta_Tables/$topic/")
        //  deltaTable.createOrReplaceTempView("delta") --usar quando tuver usando o SQL puro

        //    spark.sql("""MERGE INTO delta as delta
        //                |USING dataframe as dataframe
        //                |ON dataframe.key = delta.key
        //                |WHEN MATCHED AND dataframe.Op = 'D' THEN DELETE
        //                |WHEN MATCHED AND dataframe.Op = 'U' THEN UPDATE SET key = dataframe.key, value = dataframe.value
        //                |WHEN NOT MATCHED AND !dataframe.Op = 'U' AND !dataframe.Op= 'D' THEN INSERT (key, value,Op) VALUES (key, value,Op)
        //                |""".stripMargin)

        val deltaTable = DeltaTable.forPath(spark,s"s3a://gm-analytics-storage/Delta_Tables/$topic/")
        // Só os OP null

        // deltaTable.toDF.show()

        // Merjar os só NULL com I,D e últimos U
        // Aqui ele já muda lá no path
        deltaTable.as("delta")
          .merge(
              unionDf.as("dataframe"),
              "dataframe.key = delta.key")
          .whenMatched("dataframe.value.deleted = true")        // deleted= true
          .delete()
          //.whenMatched()
          .whenMatched("dataframe.Op = 'U'")
          .updateExpr(Map("key" -> "dataframe.key", "value" -> "dataframe.value"))
          // .updateAll()
          .whenNotMatched("!dataframe.Op = 'U' AND !dataframe.Op= 'D'")
          //.whenNotMatched("dataframe.Op = 'U' AND dataframe.Op = 'D'")
          .insertExpr(Map("key" -> "dataframe.key", "value" -> "dataframe.value","Op" -> "dataframe.Op"))
          //.insertAll()
          .execute()



        // --Print da delta table, junto com seu count que é para ver sua corretude
        println("deltatable final")
        val df = spark.read.format("delta").load(s"s3a://gm-analytics-storage/Delta_Tables/$topic/")
        df.orderBy(desc("value.cdcTime"))
          .selectExpr("Op","key", "value.*")
          .where(col("value.AKEY") === "testando" )
          .show()
        df.groupBy("Op").count().show()
        println(df.count())

    } else {
        data.write.format("delta").save(s"s3a://gm-analytics-storage/Delta_Tables/$topic/")
    }


}
