**How to install bio-pipelines?**
============================

Requirements:
| Tool | Version |
| ------ | ------ |
| Python | [3.6, 3.7, 3.8]|
| Scala | [2.11]  |
| Apache-Spark | [2.4.3] |
| sbt | [1.4.4] |

```bash
cd bio_pipelines
python3 -m venv PATH_TO_YOUR_VENV
source PATH_TO_YOUR_VENV/bin/activate
```

To install with Apache Airflow version 1.10.12:

`PYTHON_VERSION` is your installed Python version, or run:
```bash
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
```

then:

```bash
pip install . --constraint https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-${PYTHON_VERSION}.txt
```

**The DAG should appear either in ${AIRFLOW_HOME}/dags/ or in ~/airflow/dags, but if you have other location of your DAGS, then: **

place cannoli.py in your dags folder:

```bash
cp bio_pipelines/cannoli.py /path/to/your/dags/
```

Go to tools/ - to build BioPackage run:
```bash
sbt assembly
```

The value of the bio_pipe_jar variable should be:
the absolute path to the jar stored in:
```bash
tools/target/scala-2.11/BioPipeline-assembly-0.2-SNAPSHOT.jar
```

The value of the bio_pipe_cannoli variable should be:
the absolute path to the jar stored in:
```bash
tools/lib/cannoli-assembly-spark2_2.11-0.11.0-SNAPSHOT.jar
 ```

**How to set-up Apache Airflow?**
=============================

Open a terminal window, run:
```bash
airflow scheduler
```
 Open a second terminal window, run:
 ```bash
 airflow webserver -p 8080
 ```

 Go to your browser and go to URL:
 ```bash
 localhost:8080
 ```




