from bio_pipelines.operators.CannoliOperator import CannoliOperator as co


class CannoliVariantCallingOperator(co):
    template_fields = ['tool_args', '_spark_binary', '_application', '_num_executors', '_executor_memory',
                       '_driver_memory', '_conn_id', 'cannoli_path', 'input', 'image', 'additional_args', 'output',
                       'reference']
    ui_color = '#f5aeca'

    def __init__(self,
                 tool,
                 input,
                 output,
                 reference,
                 image="None",
                 additional_args="None",
                 *args,
                 **kwargs):

        self.tool = tool
        self.input = input
        self.output = output
        self.reference = reference
        self.image = image
        self.additional_args = additional_args
        super().__init__(*args, **kwargs)

    def build_application_arguments(self):
        if self.tool == "freebayes":
            return self.build_freebayes_args()

    def build_freebayes_args(self):

        app_args = ['freebayes']
        app_args += [self.input]
        app_args += [self.output]
        app_args += ['-reference', self.reference]
        app_args += ['-use_docker']
        app_args += ['-single']

        if self.image != "None":
            app_args += ['-image']
            app_args += [self.image]
        if self.tool_args != "None":
            app_args += ['-freebayes_args']
            app_args += ['"' + self.tool_args + '"']
        if self.additional_args != "None":
            app_args += [self.additional_args]
        return app_args
