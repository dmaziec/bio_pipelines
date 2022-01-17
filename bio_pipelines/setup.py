import os
import shutil
from pathlib import Path

from setuptools import find_packages, setup
from setuptools.command.develop import develop
from setuptools.command.install import install


def copy_dags_to_airflow_home():
    airflow_home = os.getenv("AIRFLOW_HOME", None)
    if airflow_home is not None:
        shutil.copy("cannoli.py", airflow_home + "/dags/")
    else:
        home = str(Path.home())
        airflow_dags = home + "/airflow/dags"
        if os.path.exists(airflow_dags):
            shutil.copy("cannoli.py", airflow_dags)


class PostInstallCommand(install):
    def run(self):
        install.run(self)
        copy_dags_to_airflow_home()


class PostDevelopCommand(develop):
    def run(self):
        develop.run(self)
        copy_dags_to_airflow_home()


setup(
    name="bio_pipelines",
    packages=find_packages(exclude=['tests*']),
    entry_points={
        'airflow.plugins': [
            'my_package = bio_pipelines.operators.airflow_plugin:BioPipelinesPlugin'
        ]},
    install_requires=['pyseqtender==0.1.1',
                      'apache-airflow==1.10.12',
                      'pysam==0.15.4',
                      'psycopg2==2.8.5'
                      ],
    cmdclass={
        'develop': PostDevelopCommand,
        'install': PostInstallCommand,
    }
)
