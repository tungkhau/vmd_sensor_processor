package com.tikeysoft;

import com.tikeysoft.aem.AemDataHandler;
import org.eclipse.paho.client.mqttv3.MqttException;

public class Main {
    public static void main(String[] args) {
        String broker = "tcp://localhost:1883";
        AemDataHandler mqttHandler = new AemDataHandler();
        try {
            mqttHandler.connectAndSubscribe(broker);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}