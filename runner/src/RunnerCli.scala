import zio._
import zio.ZEnvironment
object RunnerCli extends ZIOAppDefault {

  def run = job.provide(Process.live)

  def job = for {
    bir <- Process.runBatch("https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Annuel-Definitif_2020.zip")
    (batchSuccess, indexSuccess, reportSuccess) = bir
    _ <- Console.printLine(s"batchSuccess: $batchSuccess, indexSuccess: $indexSuccess, reportSuccess: $reportSuccess")
  } yield ()

}