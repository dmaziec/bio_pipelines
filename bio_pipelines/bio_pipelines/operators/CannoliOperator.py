from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.settings import WEB_COLORS
from airflow.utils.decorators import apply_defaults


class CannoliOperator(SparkSubmitOperator):
    template_fields = ['_num_executors', '_executor_memory', '_driver_memory', '_conn_id', 'cannoli_path']
    ui_color = WEB_COLORS['LIGHTBLUE']

    @apply_defaults
    def __init__(self,
                 cannoli_path='',
                 conf=None,
                 tool_args="None",
                 java_class=None,
                 *args,
                 **kwargs):

        self.cannoli_path = cannoli_path
        if java_class is None:
            self._java_class = 'org.bdgenomics.cannoli.cli.Cannoli'
        else:
            self._java_class = java_class

        self._conf = {}
        self._conf['spark.serializer'] = "org.apache.spark.serializer.KryoSerializer"
        self._conf['spark.kryo.registrator'] = "org.bdgenomics.adam.serialization.ADAMKryoRegistrator"
        if conf is not None:
            self._conf.update(conf)

        self.tool_args = "None"
        if tool_args != "None":
            self.tool_args = tool_args

        super().__init__(application=self.cannoli_path,
                         conf=self._conf,
                         java_class=self._java_class,
                         *args, **kwargs)

    def build_application_arguments(self):
        raise NotImplementedError()

    def execute(self, context):
        self._application_args = self.build_application_arguments()
        self._application = self.cannoli_path
        super().execute(context)
