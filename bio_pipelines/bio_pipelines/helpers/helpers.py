import glob
import itertools
import os
import re
""" Samples class IS USED IN THE DYNAMIC BASED PIPELINE"""

class Samples:
    template_fields = ['workspace']

    def __init__(self, workspace):
        self.workspace = workspace
        self.single_interleaved = []
        self.paired = []
        self.all_files_grouped = []
        self.all_files = []
        self.all_files += glob.glob(self.workspace + "*_1.fq")
        self.all_files += glob.glob(self.workspace + "*_2.fq")
        self.all_files += glob.glob(self.workspace + "*.fq")
        self.all_files += glob.glob(self.workspace + "*.fastq")
        self.all_files += glob.glob(self.workspace + "*.ifq")

        self.sum_single_interleaved = 0
        self.sum_paired = 0

        self.set_samples()
        self.set_paired_end_samples()

        self.extenstions = ["fastq", "fq", "ifq"]

    def set_id(self, file):
        if re.match(r".+\_[1|2]\.(fastq|fq)", file):
            print(os.path.basename(file).rsplit("_", 1)[0])
            return os.path.basename(file).rsplit("_", 1)[0]
        else:
            return os.path.basename(file).rsplit(".", 1)[0]

    def set_samples(self):
        samples = glob.glob(self.workspace + "*.fq") + glob.glob(self.workspace + "*.fastq") + glob.glob(
            self.workspace + "*.ifq")
        # exclude files ended with _1.fastq/fq _2.fastq/fq
        samples_filtered = [f for f in samples if not re.match(r".+\_(1|2)\.(fastq|fq)$", f)]
        if samples:
            self.sum_single_interleaved = len(samples)
            self.single_interleaved = [(key, list(group)) for key, group in
                                       itertools.groupby(samples_filtered, self.set_id)]

    def get_samples(self):
        return self.single_interleaved

    def set_paired_end_samples(self):
        samples = glob.glob(self.workspace + "*_1.fq") + glob.glob(self.workspace + "*_2.fq")
        samples.sort()
        if samples:
            self.paired = [(key, list(group)) for key, group in itertools.groupby(samples, self.set_id)]
            self.sum_paired = len(self.paired)

    def get_paired_samples(self):
        return self.paired

    def get_all_files_grouped(self):

        self.all_files_grouped += self.single_interleaved
        self.all_files_grouped += self.paired

        return self.all_files_grouped

    def get_all_files(self):
        return self.all_files

    @staticmethod
    def generate_interleaved_fastq_file_name(fastq1, fastq2):

        def process(fastq1, fastq2, end1, end2):
            if (fastq1.endswith(end1) and fastq2.endswith(end2)) or (fastq1.endswith(end2) and fastq2.endswith(end1)):
                fastq1 = fastq1.replace(end1, "")
                fastq2 = fastq2.replace(end2, "")
                if fastq1 == fastq2:
                    return fastq1 + ".ifq"
                else:
                    return None

        cases = [("_1.fq", "_2.fq"),
                 ("_1.fastq", "_2.fastq")]

        for case in cases:
            ret = process(fastq1, fastq2, case[0], case[1])
            if ret is not None:
                return ret

        return None

    def get_pipeline_samples(self):
        interleaved_files = self.single_interleaved
        for pair in self.paired:
            print(self.paired)
            interleaved_files += [(pair[0], [self.generate_interleaved_fastq_file_name(pair[1][0], pair[1][1])])]

        return interleaved_files
