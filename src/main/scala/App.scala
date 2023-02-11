import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

object App {
  val log = Logger.getLogger(App.getClass().getName())

  def run(in: String, out: String) = {
    log.info("Starting App")
    val spark = createSession

    import spark.implicits._

    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .load(in)

    df.show()
    // TODO: transformations
    compute_CO2_emissions(df, out)
    CO2_emission_report1(df, out)
    CO2_emission_report2(df, out)
    createSession.close
    log.info("Stopping App") 
  }

  def compute_CO2_emissions(df: Dataset[Row], out: String) = {

    /** Compute the CO2 emissions for the coal / oil / gas using the provided data
      */

    val filteredDf = df
      .filter("Product in ('Nucléaire', 'Charbon', 'Gaz', 'Fioul')")
      .groupBy("Date", "Time")
      .sum("CO2_Totale")

    val coalDf = filteredDf
      .filter("Product = 'Charbon'")
      .select("Date", "Time", "sum(CO2_Totale) as Coal_CO2")

    val oilDf = filteredDf
      .filter("Product = 'Fioul'")
      .select("Date", "Time", "sum(CO2_Totale) as Oil_CO2")

    val finalDf = coalDf
      .join(oilDf, Seq("Date", "Time"), "outer")
      .sort("Date", "Time")

    finalDf.write
      .format("csv")
      .option("header", "true")
      .save(out)
  }

  def CO2_emission_report1(df: Dataset[Row], out: String) = {

    /** Report for each 15mn period if the CO2_emission_coal > CO2_emission_oil
      */

    // Select the relevant columns
    val co2Data = df.select(
      "Date",
      "Time",
      "CO2 Emissions (kg/MWh) Nucleaire",
      "CO2 Emissions (kg/MWh) Charbon",
      "CO2 Emissions (kg/MWh) Gaz",
      "CO2 Emissions (kg/MWh) Fioul"
    )

    // Rename the columns to a more descriptive name
    val renamedData = co2Data
      .withColumnRenamed("CO2 Emissions (kg/MWh) Nucleaire", "Nuclear_CO2")
      .withColumnRenamed("CO2 Emissions (kg/MWh) Charbon", "Coal_CO2")
      .withColumnRenamed("CO2 Emissions (kg/MWh) Gaz", "Gas_CO2")
      .withColumnRenamed("CO2 Emissions (kg/MWh) Fioul", "Oil_CO2")

    // Aggregate the data by 15-minute intervals
    val groupedData = renamedData
      .groupBy(
        floor(unix_timestamp(col("Date"), "dd/MM/yyyy").cast("timestamp")).cast("date"),
        floor(unix_timestamp(col("Time"), "HH:mm").cast("timestamp"))
          .cast("timestamp")
          .alias("Time")
      )
      .agg(sum("Coal_CO2").alias("Coal_CO2"), sum("Oil_CO2").alias("Oil_CO2"))

    // Report if the CO2 emissions from coal is greater than the CO2 emissions from oil for each 15-minute interval
    val co2Report = groupedData
      .withColumn(
        "Coal_CO2_greater_than_Oil_CO2",
        when(col("Coal_CO2") > col("Oil_CO2"), true).otherwise(false)
      )
      .select("Date", "Time", "Coal_CO2_greater_than_Oil_CO2")

    // Write the result to a CSV file
    co2Report.write.format("csv").option("header", "true").save(out)
  }

  def CO2_emission_report2(df: Dataset[Row], out: String) = {

    /** Report for each day if the CO2_emission_coal > CO2_emission_oi
      */

    val dfRenamed = df
      .withColumnRenamed("Date", "date")
      .withColumnRenamed("Heure", "time")
      .withColumnRenamed("Fioul", "oil_co2")
      .withColumnRenamed("Charbon", "coal_co2")

    val dfGrouped = dfRenamed
      .groupBy(
        floor(unix_timestamp(col("date"), "dd/MM/yyyy").cast("timestamp")).cast("date")
      )
      .agg(
        sum("oil_co2").alias("oil_co2"),
        sum("coal_co2").alias("coal_co2")
      )

    val dfReport = dfGrouped
      .withColumn(
        "coal_greater_than_oil",
        when(col("coal_co2") > col("oil_co2"), true).otherwise(false)
      )
      .select("date", "coal_greater_than_oil")

    dfReport.write
      .format("csv")
      .option("header", "true")
      .save(out)
  }

  def createSession = {
    val builder = SparkSession.builder.appName("Spark Batch")
    builder
      .config("es.index.auto.create", "true")
      .config("es.nodes.wan.only", "true")
      .config("es.batch.size.bytes", 1024 * 1024 * 4)
    // .config("spark.eventLog.enabled", true)
    // .config("spark.eventLog.dir", "./spark-logs")
    builder.getOrCreate()
  }

  def main(args: Array[String]) = args match {
    case Array(in, out) => run(in, out)
    case _              => println("usage: spark-submit app.jar")
  }
}
