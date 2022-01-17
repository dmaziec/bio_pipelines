from bio_pipelines.operators.SeqtenderOperator import SeqtenderOperator
from pyseqtender import SeqTenderAlignment
from pyseqtender import SeqTenderCommandBuilder


class SeqtenderAlignmentCommandOperator(SeqtenderOperator):
    ui_color = "#94CEDE"
    template_fields = ['_input', '_command']

    def __init__(self, input="", command="", output="", *args, **kwargs):
        self._input = input
        self._command = command
        self._output = output
        super().__init__(*args, **kwargs)

    def run(self):
        aligner = SeqTenderAlignment(self.ss, self._input, self._command)
        reads = aligner.pipe_reads()
        aligner.save_reads(self._output, reads)


class SeqtenderAlignmentOperator(SeqtenderAlignmentCommandOperator):
    template_fields = ['_num_executors', '_jars', '_master', '_executor_cores', '_executor_memory', '_driver_memory',
                       '_conf', '_tool', '_index', '_input', '_output', '_readGroup', '_readGroupId', '_interleaved']
    ui_color = "#9BC8EF"

    def __init__(self,
                 tool,
                 index,
                 input,
                 output,
                 readGroup="",
                 readGroupId="",
                 interleaved=False,
                 *args, **kwargs):
        self._tool = tool
        self._input = input
        self._index = index
        self._output = output
        self._readGroup = readGroup
        self._readGroupId = readGroupId
        self._interleaved = interleaved

        super().__init__(input=self._input, output=self._output, *args, **kwargs)

    def run(self):
        self._command = SeqTenderCommandBuilder(self.ss, self._input, self._index,
                                                self._tool, self._interleaved, self._readGroupId,
                                                self._readGroup).get_command()
        super().run()
