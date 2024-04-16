package com.helloworld.kafka.producers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProducerCamiones {

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
    public static void main(final String[] args) throws IOException {
        
    	// Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        final String topic = "km_posicion";

        String[] camiones = {"camion_1", "camion_2", "camion_3", "camion_4", "camion_5"};
        final Producer<String, String> producer = new KafkaProducer<>(props);
            
        final Random rnd = new Random();
        for (int i = 0; i < camiones.length; i++) {
            String camion = camiones[i];
            int startKilometer = rnd.nextInt(11) + 10;

            for (int j = 0; j < 10; j++) {
                int kilometer = startKilometer + (int) (j * 1.3);
                String message = "Camion: " + camion + ", Kilómetro: " + kilometer;

                producer.send(new ProducerRecord<>(topic, String.valueOf(i + 1), message));
            }
        }
        producer.flush();
        producer.close();

    }
    
    public static void getFutureRecordMetadata(String camion, String km,RecordMetadata metadata, Exception exception) {
        if (exception != null)
            exception.printStackTrace();
        else
            System.out.printf("Produced event to topic %s: camion= %-10s km = %-20s partition=%d%n", 
            					metadata.topic(), camion, km, metadata.partition());
    }
    
}

