FROM apache/spark:3.5.3

USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/postgresql-42.7.4.jar
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.7.1/clickhouse-jdbc-0.7.1-all.jar /opt/spark/jars/clickhouse-jdbc-0.7.1-all.jar
RUN chmod 644 /opt/spark/jars/postgresql-42.7.4.jar /opt/spark/jars/clickhouse-jdbc-0.7.1-all.jar
USER spark
