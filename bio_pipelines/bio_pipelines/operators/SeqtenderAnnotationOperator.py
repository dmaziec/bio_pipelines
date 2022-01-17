import os

from bio_pipelines.operators.SeqtenderOperator import SeqtenderOperator
from pyseqtender import SeqTenderAnnotation


class SeqtenderAnnotationCommandOperator(SeqtenderOperator):
    ui_color = "#0099cc"
    template_fields = ['_input', '_output', '_command', '_num_executors', '_jars', '_master', '_executor_cores',
                       '_executor_memory', '_driver_memory', '_conf']

    def __init__(self, input="", command="", output="", *args, **kwargs):
        self._input = input
        self._command = command
        self._output = output
        super().__init__(*args, **kwargs)

    def run(self):
        annotation = SeqTenderAnnotation(self.ss)
        variants = annotation.pipe_variants(self._input, self._command)
        annotation.save_variants(self._output, variants)


class SeqtenderVepOperator(SeqtenderAnnotationCommandOperator):
    ui_color = "#b3e6ff"
    template_fields = ['_additional_args', '_cache_dir', '_image', '_input', '_output', '_num_executors', '_jars',
                       '_master', '_executor_cores', '_executor_memory', '_driver_memory', '_conf']

    def __init__(self,
                 input,
                 output,
                 cache_dir,
                 image="quay.io/biocontainers/ensembl-vep:100.2--pl526hecda079_0",
                 additional_args=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._input = input
        self._output = output
        self._image = image
        self._cache_dir = cache_dir
        self._additional_args = additional_args

    def build_application_arguments(self):
        directory = os.path.dirname(self._cache_dir)
        volume = directory + ":" + directory
        command = ['docker', 'run', '--rm', '-i', '-v']
        command += [volume]
        command += [self._image]
        command += ['vep']
        command += ['--format']
        command += ['vcf']
        command += ['--output_file']
        command += ['STDOUT']
        command += ['--no_stats']
        command += ['--offline']
        command += ['--dir_cache']
        command += [self._cache_dir]
        command += ['--vcf']
        if self._additional_args:
            command += self._additional_args

        command_string = " ".join(command)
        self.log.info("Running VEP with command: " + command_string)
        return command_string

    def run(self):
        self._command = self.build_application_arguments()
        super().run()


class SeqtenderVcfToolsOperator(SeqtenderAnnotationCommandOperator):
    ui_color = "#0099cc"
    template_fields = ['_additional_args', '_image', '_volume', '_input', '_output', '_command', '_num_executors',
                       '_jars', '_master', '_executor_cores', '_executor_memory', '_driver_memory', '_conf']

    def __init__(self,
                 input,
                 output,
                 image=None,
                 additional_args=None,
                 volume=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._input = input
        self._output = output
        self._image = image
        self._volume = volume
        self._additional_args = additional_args

    def build_application_arguments(self):
        directory = os.path.dirname(self._output)
        volume = directory + ":" + directory
        command = ['docker', 'run', '--rm', '-i']

        command += ['-v']
        command += [volume]
        if self._image:
            command += [self._image]
        else:
            command += ["biocontainers/vcftools:v0.1.16-1-deb_cv1"]
        command += ['vcftools']
        command += ['--vcf']
        command += ['-']
        if self._additional_args:
            command += self._additional_args
        command += ['--stdout']

        command_string = " ".join(command)
        self.log.info("Running VcfTools with command: " + command_string)
        return command_string

    def run(self):
        self._command = self.build_application_arguments()
        super().run()
