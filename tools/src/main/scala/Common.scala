import org.apache.spark.SparkConf
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.models.SnpTable

import org.bdgenomics.adam.rdd.read.{AlignmentDataset, ReadDataset}

object Common {

  implicit class CommonSteps(alignments: AlignmentDataset) {
    def continueAlignment(sc: SparkContext, reference: String, output: String, snps: String, intermediate_output: Boolean, prefix: String, sequenceDictionary: String): Unit = {
      val aligned_seq = {
        sequenceDictionary match {
          case null => {
            val ref = sc.loadFastaDna(reference)
            val dict = ref.createSequenceDictionary()
            val copy = dict.sequences
            alignments.replaceSequences(copy)
          }
          case dict: String => {
            val sequences = sc.loadSequenceDictionary(dict)
            alignments.replaceSequences(sequences)
          }
        }
      }
      val sorted = aligned_seq.sortByReferencePosition()
      val marked = sorted.markDuplicates()

      if (intermediate_output) {
        alignments.saveAsSam(filePath = prefix + "_aligned.sam", asSingleFile = true)
        sorted.saveAsSam(filePath = prefix + "_sorted.sam", asSingleFile = true)
        marked.saveAsSam(filePath = prefix + "_marked.sam", asSingleFile = true)
      }
      val variants = sc.loadVariants(snps)
      val snpsTable = sc.broadcast(SnpTable(variants))
      val recalibrated = marked.recalibrateBaseQualities(snpsTable)
      val recalibrated_saved = recalibrated.saveAsSam(filePath = output, asSingleFile = true)
    }
  }

}
