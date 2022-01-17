import org.kohsuke.args4j.{Argument, Option => Args4jOption}
import org.bdgenomics.cannoli.cli.{Bowtie2Args => BowtieArgsCannoli}
import org.bdgenomics.adam.cli.CommandGroup
import org.bdgenomics.utils.cli._
import org.apache.spark.SparkContext
import grizzled.slf4j.Logging

import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.bdgenomics.adam.rdd.ADAMContext._

import org.bdgenomics.cannoli.Cannoli._
import Common.CommonSteps

class Bowtie2PipelineArgs extends BowtieArgsCannoli {
  @Args4jOption(required = false, name = "-prefix", usage = "Prefix of intermediate output files if enabled")
  var prefix: String = null

  @Args4jOption(required = false, name = "-intermediate_output", usage = "Enable intermediate output files - aligned, sorted")
  var intermediate_output: Boolean = false

  @Args4jOption(required = true, name = "-reference", usage = "Location to reference genome (e.g. .fasta, .fa)")
  var genomePath: String = null

  @Args4jOption(required = false, name = "-known_snps", usage = "A table of known SNPs")
  var snpsPath: String = null

  @Args4jOption(required = false, name = "-sequence_dictionary", usage = "Path to the sequence dictionary.")
  var sequenceDictionary: String = _

}

class Bowtie2Pipeline(protected val args: Bowtie2PipelineArgs) extends BDGSparkCommand[Bowtie2PipelineArgs] with Logging {
  val companion = Bowtie2Pipeline

  def run(sc: SparkContext) {
    val loadedReads = sc.loadFragments(args.inputPath)
    val alignment = loadedReads.alignWithBowtie2(args.asInstanceOf[BowtieArgsCannoli])
    alignment.continueAlignment(sc, args.genomePath, args.outputPath, args.snpsPath, args.intermediate_output, args.prefix, args.sequenceDictionary)
  }
}

object Bowtie2Pipeline extends BDGCommandCompanion {
  val commandName = "bowtie2Pipeline"
  val commandDescription = "Align reads with Bowtie2, sort by reference genome and marked duplicates."

  def apply(cmdLine: Array[String]) = {
    new Bowtie2Pipeline(Args4j[Bowtie2PipelineArgs](cmdLine))
  }
}

