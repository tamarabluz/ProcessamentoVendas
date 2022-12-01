package org.KafkaVendas.Service;

import org.KafkaVendas.Deserializer.VendaDeserializer;
import org.KafkaVendas.Model.Vendas;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class ProcessadorVendas {
    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VendaDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-processamento");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        try (KafkaConsumer<String, Vendas> consumer = new KafkaConsumer<>(properties)) {

            consumer.subscribe(Arrays.asList("vendas-ingressos"));

            while (true) {
                ConsumerRecords<String, Vendas> venda = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, Vendas> record : venda) {
                    Vendas vendas = record.value();

                    if (new Random().nextBoolean()) {
                        vendas.setStatus("APROVADA");
                    } else {
                        vendas.setStatus("REPROVADA");
                    }
                    Thread.sleep(500);
                    System.out.println(vendas);


                }
            }
        }
    }
}
