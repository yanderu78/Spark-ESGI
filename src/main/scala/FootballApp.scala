import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DateType};
import org.apache.spark.sql.functions.{udf, col, when, year}

object FootballApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    // Imports
    import spark.implicits._

    //UTILS
    // UDF that check if the a match is a CDM match
    val is_cdm = (value:String) => (value.split(" ")(0).trim() == "Coupe"))
    val is_cdm_udf = udf(is_cdm)
    // Check if Domicil
    val is_domicile = (value:String) => (value.split("-")(0).trim() == "France")
    val is_domicile_udf = udf(is_domicile)

    // MAIN CODE
    // Create dataframe and rename columns (step 1)
    val dfCsv = spark.read.option("header", "true").option("sep", ",").csv("C:\\Users\\Yanderu\\IdeaProjects\\Spark\\df_matches.csv")
                .withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")
                .select($"match", $"competition", $"adversaire", $"score_france".cast(IntegerType), $"score_adversaire".cast(IntegerType), $"penalty_france".cast(IntegerType),
                  $"penalty_adversaire".cast(IntegerType), $"date".cast(DateType))
                .withColumn("penalty_france", when($"penalty_france".isNull, 0))
                .withColumn("penalty_adversaire", when($"penalty_adversaire".isNull, 0))
                .filter(year($"date") > 1979)


    // Create stats dataframe (step 2)
    val dfStats = dfCsv.groupBy(dfCsv("adversaire")).agg(
        avg(dfCsv.col("score_france")).alias("FranceScoreMoyen"),
        avg(dfCsv.col("score_adversaire")).alias("AdversaireScoreMoyen"),
        count("*").alias("NbMatch"),
        (sum(is_domicile_udf(col("match")).cast(IntegerType)) / count("*") * 100).alias("PourcentageDomicile"),
        sum(is_cdm_udf(col("competition")).cast(IntegerType)).alias("NbCDM"),
        max(col("penalty_france")).alias("RecordNbPenalty"),
        (sum(col("penalty_france")) - sum(col("penalty_adversaire"))).alias("DifferencePenaltyFranceAdversaire")
      )

    // Create parquet file
    dfStats.write.parquet("stats.parquet")

    dfCsv.show
    dfCsv.printSchema()
    spark.stop()
  }
}