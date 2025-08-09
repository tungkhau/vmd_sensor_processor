package com.tikeysoft.aem;

import java.time.Instant;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AemDataHandler {
    private MqttClient client;
    String clientId = "AemDataHandler";
    String[] topics = {
            "AE_01/condition/die_temp",
            "AE_01/condition/billet_temp",
            "AE_01/condition/ramp_pressure",
            "AE_01/production/billet",
            "AE_01/production/billet_waste",
            "AE_01/production/semi_profile_A",
            "AE_01/production/semi_profile_B",
            "AE_01/signal/end",
            "AE_01/signal/billet_detecting",
            "AE_01/signal/heart_beat",
            "AE_01/signal/puller_A",
            "AE_01/signal/puller_B",
            "AE_01/signal/cutter",
            "AE_01/signal/billet_cutting"
    };
    private String billetCache = "0";
    private String billetWasteCache ="0";
    private String semiProfileACache ="0";
    private String semiProfileBCache="0";

    private boolean heartBeat = false;
    private boolean cutter = false;
    private boolean billetDetecting = false;


    private Instant lastDieTemp = Instant.now();
    private Instant lastBilletTemp = Instant.now();
    private Instant lastRampPressure = Instant.now();

    private Instant lastBillet = Instant.now();
    private Instant lastBilletWaste = Instant.now();
    private Instant lastSemiProfileA = Instant.now();
    private Instant lastSemiProfileB = Instant.now();

    public void connectAndSubscribe(String broker) throws MqttException {
        client = new MqttClient(broker, clientId);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);

        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                System.out.println("Connection lost: " + cause.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                if (message.isRetained()) return;
                String payload = new String(message.getPayload());

                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(payload);
                JsonNode data = json.get("data");

                if (data != null) {
                    JsonNode payloadData = data.get("payload");

                    if (payloadData != null) {
                        payloadData.fields().forEachRemaining(entry -> {
                            if (entry.getKey().contains("/iolinkmaster/port")) {
                                JsonNode portData = entry.getValue();
                                String hexString = portData.get("data").asText();
                                try {
                                    convertData(topic, hexString);
                                } catch (MqttException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
                    } else {
                        System.out.println("Payload data is null");
                    }
                } else {
                    System.out.println("Data is null");
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Not used in this example
            }
        });

        System.out.println("Connecting to broker: " + broker);
        client.connect(connOpts);
        System.out.println("Connected");

        for (String topic : topics) {
            client.subscribe(topic);
            System.out.println("Subscribed to topic: " + topic);
        }
    }

    private void convertData(String topic, String hexString) throws MqttException {
        Instant now = Instant.now();
        int conditionInterval = 5;
        int productionInterval = 30;
        switch (topic) {
            // Publish Condition values
            case "AE_01/condition/die_temp":
                if (heartBeat && now.isAfter(lastDieTemp.plusSeconds(conditionInterval))) {
                    client.publish("processed/AE_01/condition/die_temp", new MqttMessage(String.valueOf(AemDataConverter.convertDieTemp(hexString)).getBytes()));
                    lastDieTemp = now;
                }
                break;
            case "AE_01/condition/billet_temp":
                if (heartBeat && now.isAfter(lastBilletTemp.plusSeconds(conditionInterval))) {
                    client.publish("processed/AE_01/condition/billet_temp", new MqttMessage(String.valueOf(AemDataConverter.convertBilletTemp(hexString)).getBytes()));
                    lastBilletTemp = now;
                }
                break;
            case "AE_01/condition/ramp_pressure":
                if (heartBeat && now.isAfter(lastRampPressure.plusSeconds(conditionInterval))) {
                    client.publish("processed/AE_01/condition/ramp_pressure", new MqttMessage(String.valueOf(AemDataConverter.convertRampPressure(hexString)).getBytes()));
                    lastRampPressure = now;
                }
                break;

            // Store Signal values
            case "AE_01/signal/heart_beat":
                heartBeat = AemDataConverter.convertHeartBeat(hexString);
                int heartBeatValue = heartBeat ? 1 : 0;
                client.publish("processed/AE_01/signal/heart_beat", new MqttMessage(String.valueOf(heartBeatValue).getBytes()));
                break;
            case "AE_01/signal/cutter":
                cutter = AemDataConverter.convertCutter(hexString);
                break;
            case "AE_01/signal/billet_detecting":
                billetDetecting = AemDataConverter.convertBilletDetecting(hexString);
                break;

            // Cache Production values
            case "AE_01/production/billet":
                billetCache = String.valueOf(AemDataConverter.convertBillet(hexString));
                break;
            case "AE_01/production/billet_waste":
                billetWasteCache = String.valueOf(AemDataConverter.convertBilletWaste(hexString));
                break;
            case "AE_01/production/semi_profile_A":
                semiProfileACache = String.valueOf(AemDataConverter.convertSemiProfileA(hexString));
                break;
            case "AE_01/production/semi_profile_B":
                semiProfileBCache = String.valueOf(AemDataConverter.convertSemiProfileB(hexString));
                break;

            // Process Billet value
            case "AE_01/signal/billet_cutting":
                if (billetDetecting && !AemDataConverter.convertBilletCutting(hexString) && now.isAfter(lastBillet.plusSeconds(productionInterval))) {
                    client.publish("processed/AE_01/production/billet", new MqttMessage(billetCache.getBytes()));
                    lastBillet = now;
                }
                break;

            // Process Billet waste value
            case "AE_01/signal/end":
                if (AemDataConverter.convertSignalEnd(hexString) && now.isAfter(lastBilletWaste.plusSeconds(productionInterval))) {
                    client.publish("processed/AE_01/production/billet_waste", new MqttMessage(billetWasteCache.getBytes()));
                    lastBilletWaste = now;
                }
                break;

            // Process Semi profile value
            case "AE_01/signal/puller_A":
                if (heartBeat && AemDataConverter.convertPullerA(hexString) && !cutter && now.isAfter(lastSemiProfileA.plusSeconds(productionInterval))) {
                    System.out.println(heartBeat +" "+ AemDataConverter.convertPullerA(hexString) + " " + !cutter + " " + now.isAfter(lastSemiProfileA.plusSeconds(productionInterval)));
                    client.publish("processed/AE_01/production/semi_profile", new MqttMessage(semiProfileBCache.getBytes()));
                    lastSemiProfileA = now;
                }
                break;
            case "AE_01/signal/puller_B":
                if (heartBeat && AemDataConverter.convertPullerB(hexString) && !cutter && now.isAfter(lastSemiProfileB.plusSeconds(productionInterval))) {
                    client.publish("processed/AE_01/production/semi_profile", new MqttMessage(semiProfileACache.getBytes()));
                    lastSemiProfileB = now;
                }
                break;

            default:
                throw new IllegalArgumentException("Unknown topic: " + topic);
        }
    }
}