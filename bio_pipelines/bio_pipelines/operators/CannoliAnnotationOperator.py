from bio_pipelines.operators.CannoliOperator import CannoliOperator as co


class CannoliAnnotationOperator(co):
    template_fields = ['tool_args', '_spark_binary', '_application', '_num_executors', '_executor_memory',
                       '_driver_memory', '_conn_id', 'cannoli_path', 'input', 'output', 'cache', 'image',
                       'additional_args']
    ui_color = '#d8b8e3'

    def __init__(self,
                 tool,
                 input,
                 output,
                 cache,
                 image="None",
                 additional_args="None",

                 *args,
                 **kwargs):

        self.tool = tool
        self.input = input
        self.output = output
        self.image = image
        self.additional_args = additional_args
        self.cache = cache
        super().__init__(*args, **kwargs)

    def build_application_arguments(self):
        if self.tool == "vep":
            return self.build_vep_args()

    def build_vep_args(self):

        app_args = ['vep']
        app_args += [self.input]
        app_args += [self.output]
        app_args += ['-use_docker']
        app_args += ['-single']
        app_args += ['-cache']
        app_args += [self.cache]

        if self.image != "None":
            app_args += ['-image']
            app_args += [self.image]

        if self.additional_args != "None":
            app_args += [self.additional_args]

        if self.tool_args != "None":
            app_args += ['-vep_args']
            app_args += ['"' + self.tool_args + '"']

        return app_args
