package org.KafkaVendas.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.KafkaVendas.Model.Vendas;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class VendaDeserializer implements Deserializer <Vendas>{

    @Override
    public Vendas deserialize(String s, byte[] vendas) {
        try {
            return new ObjectMapper().readValue(vendas, Vendas.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
