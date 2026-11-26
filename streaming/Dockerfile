# Image du job Spark Structured Streaming (phase 3, I1).
# Base officielle Apache Spark : Spark 4.2.1, Scala 2.13, Java 17, Python 3.10 (Ubuntu 22.04).
# pyspark est fourni par $SPARK_HOME/python (pas par pip). On n'ajoute que psycopg,
# à la version de uv.lock, pour la fusion idempotente depuis le driver (I4).
FROM apache/spark:4.2.1-scala2.13-java17-python3-ubuntu

USER root
RUN pip3 install --no-cache-dir "psycopg[binary]==3.3.9" \
    # Cache Ivy (I2) et checkpoint (I6) : dossiers créés ici, à l'utilisateur spark,
    # pour que les volumes nommés montés dessus héritent de ces droits
    && mkdir -p /opt/spark-ivy /opt/spark-checkpoints \
    && chown spark:spark /opt/spark-ivy /opt/spark-checkpoints
# Retour à l'utilisateur non privilégié de l'image (uid 185)
USER spark

WORKDIR /opt/app
ENV PYTHONPATH=/opt/app:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYTHONDONTWRITEBYTECODE=1
