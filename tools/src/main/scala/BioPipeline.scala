import org.bdgenomics.adam.cli.CommandGroup
import org.bdgenomics.utils.cli._
import grizzled.slf4j.Logging
import org.bdgenomics.adam.util.ParquetLogger
import org.apache.spark.SparkContext

import java.util.logging.Level._
import javax.inject.Inject
import com.google.inject.AbstractModule

class BioPipeline(commandGroups: List[CommandGroup]) {

  def apply(args: Array[String]) {

    val commands =
      for {
        grp <- commandGroups
        cmd <- grp.commands
      } yield cmd

    commands.find(_.commandName == args(0)) match {
      case None => println("No Command found")
      case Some(cmd) =>
        init(Args4j[InitArgs](args drop 1, ignoreCmdLineExceptions = true))
        cmd.apply(args drop 1).run()
    }
  }

  private def init(args: InitArgs) {
    // Set parquet logging (default: severe)
    ParquetLogger.hadoopLoggerLevel(parse(args.logLevel))
  }
}

private class InitArgs extends Args4jBase with ParquetArgs {}

object BioPipeline {

  val defaultCommandGroups = List(CommandGroup("BioPipeline", List(
    SingleEndBowtie2Pipeline,
    Bowtie2Pipeline,
    BwaPipeline)))

  def main(args: Array[String]) {
    new BioPipeline(defaultCommandGroups)(args)
  }

}

