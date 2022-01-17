from bio_pipelines.helpers.cannoli_commands_args import Bowtie2Aligner, BwaAligner
from bio_pipelines.operators.CannoliOperator import CannoliOperator as co


class CannoliAlignmentOperator(co):
    template_fields = ['sequence_dict', 'tool_args', '_num_executors', '_executor_memory', '_driver_memory', '_conn_id',
                       'cannoli_path', 'reference',
                       'id', 'image', 'additional_args', 'tool', 'snps', 'output', '_application', 'sample',
                       '_spark_binary']
    ui_color = '#b8e3e3'

    def __init__(self,
                 tool,
                 output,
                 id,
                 sample,
                 image="None",
                 reference=None,
                 additional_args="None",
                 snps=None,
                 sequence_dictionary="None",
                 *args,
                 **kwargs):

        self.tool = tool
        self.output = output
        self.id = id
        self.image = image
        self.additional_args = additional_args
        self.reference = reference
        self.sample = sample
        self.snps = snps
        self.sequence_dict = sequence_dictionary
        super().__init__(*args, **kwargs)

    def build_application_arguments(self):
        args = []
        if self.image != "None":
            args += ['-image']
            args += [self.image]
        if self.additional_args != "None":
            args += [self.additional_args]
        if self.sequence_dict != "None":
            args += ['-sequence_dictionary']
            args += [self.sequence_dict]

        if self.tool == "bowtie2":
            if self.tool_args != "None":
                args += ['-bowtie2_args']
                args += ['"' + self.tool_args + '"']

            if args == []:
                args = None

            aligner = Bowtie2Aligner(reference=self.reference)

            return aligner.alignment(self.id, self.sample, self.output,
                                     self.snps, args)
        if self.tool == "bwa":
            if self.tool_args != "None":
                args += ['-bwa_args']
                args += ['"' + self.tool_args + '"']
            if args == []:
                args = None
            aligner = BwaAligner(reference=self.reference)
            return aligner.alignment(self.id, self.sample, self.output,
                                     self.snps, args)
