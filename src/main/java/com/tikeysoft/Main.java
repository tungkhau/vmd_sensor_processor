package com.tikeysoft;

import com.tikeysoft.aem.AemDataHandler;

public class Main {
    public static void main(String[] args) {
        String broker = "tcp://host.docker.internal:1883";
        AemDataHandler mqttHandler = new AemDataHandler();
        mqttHandler.connectAndSubscribe(broker);
    }
}