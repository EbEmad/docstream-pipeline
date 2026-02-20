FROM debezium/connect:2.5

USER root

# Create directory for Confluent libraries
RUN mkdir -p /kafka/connect/confluent-avro-converter

# Download Confluent Avro Converter and ALL dependencies
RUN cd /kafka/connect/confluent-avro-converter && \
    # Core Confluent packages
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.5.0/kafka-connect-avro-converter-7.5.0.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.5.0/kafka-connect-avro-data-7.5.0.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.5.0/kafka-avro-serializer-7.5.0.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.5.0/kafka-schema-serializer-7.5.0.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.5.0/kafka-schema-registry-client-7.5.0.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/7.5.0/kafka-schema-converter-7.5.0.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/common-config/7.5.0/common-config-7.5.0.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/common-utils/7.5.0/common-utils-7.5.0.jar && \
    # Apache Avro
    curl -sO https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar && \
    # Google Guava
    curl -sO https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar && \
    curl -sO https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar && \
    # Jackson (for Avro)
    curl -sO https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar && \
    curl -sO https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar && \
    curl -sO https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar && \
    # Apache Commons Compress (for Avro)
    curl -sO https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.24.0/commons-compress-1.24.0.jar && \

    curl -sO https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.5/minimal-json-0.9.5.jar && \
    curl -sO https://repo1.maven.org/maven2/com/google/re2j/re2j/1.7/re2j-1.7.jar && \
    curl -sO https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.7/slf4j-api-2.0.7.jar && \
    curl -sO https://repo1.maven.org/maven2/org/yaml/snakeyaml/2.2/snakeyaml-2.2.jar && \
    curl -sO https://repo1.maven.org/maven2/io/swagger/swagger-annotations/1.6.11/swagger-annotations-1.6.11.jar && \
    curl -sO https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-csv/2.15.2/jackson-dataformat-csv-2.15.2.jar && \
        curl -sO https://repo1.maven.org/maven2/io/confluent/logredactor/1.0.10/logredactor-1.0.10.jar && \
        curl -sO https://repo1.maven.org/maven2/io/confluent/logredactor-metrics/1.0.13/logredactor-metrics-1.0.13.jar

RUN mkdir -p /kafka/connect/debezium-openlineage

# Download Debezium OpenLineage modules
RUN cd /kafka/connect/debezium-openlineage && \
    curl -sO https://repo1.maven.org/maven2/io/debezium/debezium-openlineage-core/3.3.1.Final/debezium-openlineage-core-3.3.1.Final.jar && \
    curl -sO https://repo1.maven.org/maven2/io/debezium/debezium-openlineage-api/3.3.1.Final/debezium-openlineage-api-3.3.1.Final.jar && \
    curl -sO https://repo1.maven.org/maven2/io/openlineage/openlineage-java/1.22.0/openlineage-java-1.22.0.jar

USER kafka