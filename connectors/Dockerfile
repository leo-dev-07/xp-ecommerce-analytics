FROM confluentinc/cp-kafka-connect:7.6.0

# Install plugins
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:latest
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-azure-blob-storage:latest

USER root

# Copy all Confluent licensing & serializer JARs required by the Azure Blob connector
RUN cp /usr/share/java/confluent-control-center/confluent-licensing-new-*.jar \
       /usr/share/java/confluent-control-center/confluent-serializers-new-*.jar \
       /usr/share/java/confluent-control-center/kafka-protobuf-serializer-*.jar \
       /usr/share/java/confluent-control-center/kafka-protobuf-provider-*.jar \
       /usr/share/java/confluent-control-center/kafka-protobuf-types-*.jar \
       /usr/share/java/confluent-control-center/kafka-schema-serializer-*.jar \
       /usr/share/java/confluent-control-center/protobuf-java-*.jar \
       /usr/share/java/confluent-control-center/proto-google-common-protos-*.jar \
       /usr/share/confluent-hub-components/confluentinc-kafka-connect-azure-blob-storage/lib/connect-licensing-extensions-*.jar \
       /usr/share/java/kafka/ 2>/dev/null || true

USER appuser