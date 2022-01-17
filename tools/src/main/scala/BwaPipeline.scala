
import org.bdgenomics.cannoli.cli.{BwaMemArgs => BwaArgsCannoli}
import org.kohsuke.args4j.{Argument, Option => Args4jOption}
import org.bdgenomics.adam.cli.CommandGroup
import org.bdgenomics.utils.cli._
import org.apache.spark.SparkContext
import grizzled.slf4j.Logging

import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.bdgenomics.adam.rdd.ADAMContext._

import org.bdgenomics.cannoli.Cannoli._
import Common.CommonSteps

class BwaPipelineArgs extends BwaArgsCannoli {
  @Args4jOption(required = false, name = "-prefix", usage = "Prefix of intermediate output files if enabled")
  var prefix: String = null

  @Args4jOption(required = false, name = "-intermediate_output", usage = "Enable intermediate output files - aligned, sorted")
  var intermediate_output: Boolean = false

  @Args4jOption(required = true, name = "-reference", usage = "Location to reference genome (e.g. .fasta, .fa)")
  var genomePath: String = null

  @Args4jOption(required = false, name = "-known_snps", usage = "A table of known SNPs")
  var snpsPath: String = null
}

class BwaPipeline(protected val args: BwaPipelineArgs) extends BDGSparkCommand[BwaPipelineArgs] with Logging {
  val companion = BwaPipeline

  def run(sc: SparkContext) {
    val loadedReads = sc.loadInterleavedFastqAsFragments(args.inputPath)
    val alignment = loadedReads.alignWithBwaMem(args.asInstanceOf[BwaArgsCannoli])
    alignment.continueAlignment(sc, args.genomePath, args.outputPath, args.snpsPath, args.intermediate_output, args.prefix, args.sequenceDictionary)
  }
}

object BwaPipeline extends BDGCommandCompanion {
  val commandName = "bwaPipeline"
  val commandDescription = "Align reads with Bwa, sort by reference genome and marked duplicates."

  def apply(cmdLine: Array[String]) = {
    new BwaPipeline(Args4j[BwaPipelineArgs](cmdLine))
  }
}