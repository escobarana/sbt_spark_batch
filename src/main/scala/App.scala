import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
object App {
  val log = Logger.getLogger(App.getClass().getName())

  def run(in: String, out: String = null) = {
    log.info("Starting App")
    val spark = createSession

    val df = spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("encoding", "ISO-8859-1")
      .csv(in)

    compute_CO2_emissions(df, "data/CO2_emission.csv")
    CO2_emission_report1(df, "data/CO2_emission_report1.csv")
    CO2_emission_report2(df, "data/CO2_emission_report2.csv")

    createSession.close
    log.info("Stopping App") 
  }

  def compute_CO2_emissions(df: Dataset[Row], out: String) = {

    /** Compute the CO2 emissions for the coal / oil / gas using the provided data
      */

    // Filter the data to keep only the columns you're interested in
    val filteredData = df.select("Date", "Heures", "Fioul", "Charbon")
                         .withColumnRenamed("Heures", "Time")

    // Replace missing values with 0
    val cleanedData = filteredData.na.fill(0.0)

    // Compute the CO2 emissions for coal and oil
    val co2Emissions = cleanedData.withColumn("Date", date_format(to_date(col("Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))
                                  .withColumn("Coal_CO2", cleanedData("Charbon") * 0.986)
                                  .withColumn("Oil_CO2", cleanedData("Fioul") * 0.777)

    // Create a new DataFrame with date, time, and CO2 emissions for coal and oil
    val finalData = co2Emissions.select("Date", "Time", "Oil_CO2", "Coal_CO2")

    // Write the final DataFrame to a CSV file
    finalData.write
      .mode("Overwrite")
      .format("csv")
      .option("header", "true")
      .save(out)
  }

  def CO2_emission_report1(df: Dataset[Row], out: String) = {

    /** Report for each 15mn period if the CO2_emission_coal > CO2_emission_oil
      */

    // Filter the data to keep only the columns you're interested in
    val filteredData = df.select("Date", "Heures", "Fioul", "Charbon")
                         .withColumnRenamed("Heures", "Time")

    // Replace missing values with 0
    val cleanedData = filteredData.na.fill(0.0)

    // Compute the CO2 emissions for coal and oil
    val co2Emissions = cleanedData.withColumn("Date", date_format(to_date(col("Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))
      .withColumn("Coal_CO2", cleanedData("Charbon") * 0.986)
      .withColumn("Oil_CO2", cleanedData("Fioul") * 0.777)

    // Aggregate the data by 15-minute intervals
    val groupedData = co2Emissions
      .groupBy(
        col("Date"),
        col("Time")
      )
      .agg(sum("Coal_CO2").alias("CO2_emission_coal"), sum("Oil_CO2").alias("CO2_emission_oil"))

    // Report if the CO2 emissions from coal is greater than the CO2 emissions from oil for each 15-minute interval
    val co2Report = groupedData
      .withColumn(
        "Coal_CO2_greater_than_Oil_CO2",
        when(col("CO2_emission_coal") > col("CO2_emission_oil"), true).otherwise(false)
      )
      .select("Date", "Time", "Coal_CO2_greater_than_Oil_CO2")

    // Write the result to a CSV file
    co2Report.write
      .mode("Overwrite")
      .format("csv")
      .option("header", "true")
      .save(out)
  }

  def CO2_emission_report2(df: Dataset[Row], out: String) = {

    /** Report for each day if the CO2_emission_coal > CO2_emission_oil
      */

    // Filter the data to keep only the columns you're interested in
    val filteredData = df.select("Date", "Fioul", "Charbon")

    // Replace missing values with 0
    val cleanedData = filteredData.na.fill(0.0)

    // Compute the CO2 emissions for coal and oil
    val co2Emissions = cleanedData.withColumn("Date", date_format(to_date(col("Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))
      .withColumn("Coal_CO2", cleanedData("Charbon") * 0.986)
      .withColumn("Oil_CO2", cleanedData("Fioul") * 0.777)

    val dfGrouped = co2Emissions
      .groupBy(
        col("Date")
      )
      .agg(
        sum("Oil_CO2").alias("CO2_emission_oil"),
        sum("Coal_CO2").alias("CO2_emission_coal")
      )

    val dfReport = dfGrouped
      .withColumn(
        "Coal_CO2_greater_than_Oil_CO2",
        when(col("CO2_emission_coal") > col("CO2_emission_oil"), true).otherwise(false)
      )
      .select("Date", "Coal_CO2_greater_than_Oil_CO2")

    dfReport.write
      .mode("Overwrite")
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
    case Array(in) => run(in)
    case _              => println("usage: spark-submit --class App target/scala-2.12/spark-job-assembly-1.0.jar data/eCO2mix_RTE_Annuel-Definitif_2020.csv")
  }
}
