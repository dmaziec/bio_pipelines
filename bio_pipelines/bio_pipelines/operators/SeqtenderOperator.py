from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession


class SeqtenderOperator(BaseOperator):
    template_fields = ['_num_executors', '_jars', '_master', '_executor_cores', '_executor_memory', '_driver_memory',
                       '_conf']
    ui_color = "#94CEDE"

    @apply_defaults
    def __init__(self,
                 num_executors=None,
                 jars=None,
                 master=None,
                 executor_cores=None,
                 executor_memory=None,
                 driver_memory=None,
                 conf={},
                 # maven_coordinates = "org.biodatageeks:seqtender_2.11:0.3.6",
                 *args,
                 **kwargs):
        self._num_executors = num_executors
        self._jars = jars
        self._master = master
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._conf = conf
        # self._maven_coordinates = maven_coordinates
        self.ss = None

        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.ss = SparkSession.builder
        if self._num_executors:
            self.ss = self.ss.config('spark.executor.instances', self._num_executors)

        self.ss = self.ss.config('spark.jars', self._jars)

        # self.ss = self.ss.config("spark.jars.packages", self._maven_coordinates)
        if self._master:
            self.ss = self.ss.master(self._master)
        if self._executor_cores:
            self.ss = self.ss.config('spark.executor.cores', self._executor_cores)
        if self._executor_memory:
            self.ss = self.ss.config('spark.executor.memory', self._executor_memory)
        if self._driver_memory:
            self.ss = self.ss.config('spark.driver.memory', self._driver_memory)
        for key in self._conf:
            self.ss = self.ss.config(key, str(self._conf[key]))
        self.ss = self.ss.getOrCreate()
        self.run()

    def run(self):
        pass
