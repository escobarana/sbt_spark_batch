import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.elasticsearch.spark.sql._

object App {
    val log = Logger.getLogger(App.getClass().getName())

    def run(f: SparkSession => Unit) = {
        val spark = createSession
        f(spark)
        spark.close
    }

    def run(in: String, out: String) = {
        log.info("Starting App")
        val spark = createSession
        val df = spark.read.option("header", true).xml(in)

        // TODO: transformations
        createSession.close
        log.info("Stopping App")
    }

    def createSession = {
        val builder = SparkSession.builder.appName("Spark Batch")
        builder
        .config("es.index.auto.create", "true")
        .config("es.nodes.wan.only", "true")
        .config("es.net.http.auth.user", "elastic")
        .config("es.net.http.auth.pass", "somethingsecret")
        .config("es.batch.size.bytes", 1024*1024*4)
        //.config("spark.eventLog.enabled", true)
        //.config("spark.eventLog.dir", "./spark-logs")
        builder.getOrCreate()
    }

    def index(spark: SparkSession, in: String) = {
        val df = spark.read.option("header", true).xml(in).repartition(4)
        df.saveToEs("web/files")  // indexing the data, cache on disk - less expensive than AWS ElastiCache
    }



    def main(args: Array[String]) = args match {
        case Array(in, out) => run(in, out)
        case _ => println("usage: spark-submit app.jar")
    }
}