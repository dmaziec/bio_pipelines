from airflow.plugins_manager import AirflowPlugin
from bio_pipelines.helpers.cannoli_commands_args import get_index_command, get_aligner
from bio_pipelines.operators.SeqtenderAlignmentOperator import SeqtenderAlignmentOperator, \
    SeqtenderAlignmentCommandOperator
from bio_pipelines.operators.SeqtenderAnnotationOperator import SeqtenderAnnotationCommandOperator, \
    SeqtenderVcfToolsOperator, SeqtenderVepOperator
from bio_pipelines.operators.CannoliAlignmentOperator import CannoliAlignmentOperator
from bio_pipelines.operators.CannoliVariantCallingOperator import CannoliVariantCallingOperator
from bio_pipelines.operators.CannoliAnnotationOperator import CannoliAnnotationOperator
from bio_pipelines.operators.spark_config_operator import SparkTaskCollector


class BioPipelinesPlugin(AirflowPlugin):
    name = "bio_pipeline"
    operators = [
        CannoliAlignmentOperator,
        CannoliVariantCallingOperator,
        CannoliAnnotationOperator,
        SparkTaskCollector,
        SeqtenderAlignmentOperator,
        SeqtenderAlignmentCommandOperator,
        SeqtenderAnnotationCommandOperator,
        SeqtenderVcfToolsOperator,
        SeqtenderVepOperator
    ]
    hooks = []
    executors = []
    macros = [
        get_index_command,
        get_aligner

    ]
    admin_views = []
    flask_blueprints = []
    menu_links = []
