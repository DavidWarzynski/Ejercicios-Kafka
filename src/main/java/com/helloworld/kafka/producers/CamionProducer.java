package com.helloworld.kafka.producers;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CamionProducer {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(final String[] args) throws IOException {

        final String topic = "km_posicion";

        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8085");
        props.put("acks", "all");

        final Producer<String, GenericRecord> producer = new KafkaProducer<>(props);

        String valueSchemaString = readFileFromResources("camion.value.avsc");
        Schema valueSchema = new Schema.Parser().parse(valueSchemaString);

        int camiones = 5;
        double avance = 1.3;
        int kmMin = 10;
        int kmMax = 20;
        int mediciones = 10;

        Random rnd = new Random();

        for (int i = 0; i < camiones; i++) {
            String matricula = "Camion_" + (i + 1);
            float latitud = rnd.nextFloat();
            float longitud = rnd.nextFloat();
            int velocidad = rnd.nextInt(100);

            int kmInicio = rnd.nextInt(kmMax - kmMin + 1) + kmMin;

            for (int j = 0; j < mediciones; j++) {
                double kmActual = kmInicio + (j * avance);
                String timestamp = Instant.now().minus(j, ChronoUnit.MINUTES).toString();

                GenericRecord record = new GenericData.Record(valueSchema);
                record.put("matricula", matricula);
                record.put("latitud", latitud);
                record.put("longitud", longitud);
                record.put("km", kmActual);
                record.put("velocidad", velocidad);
                record.put("timestamp", timestamp);

                producer.send(new ProducerRecord<>(topic, matricula, record),
                        (metadata, exception) -> getFutureRecordMetadata(matricula, record, metadata, exception));
            }
        }
        producer.flush();
        producer.close();
    }

    public static void getFutureRecordMetadata(String matricula, GenericRecord record, RecordMetadata metadata,
                                               Exception exception) {
        if (exception != null)
            exception.printStackTrace();
        else
            System.out.printf("Produced event to topic %s: Matricula= %s, Record = %s, Partition=%d%n",
                    metadata.topic(), matricula, record, metadata.partition());
    }

    public static String readFileFromResources(String fileName) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new RuntimeException("Error al leer el archivo " + fileName + " desde src/main/resources", e);
        }
    }
}