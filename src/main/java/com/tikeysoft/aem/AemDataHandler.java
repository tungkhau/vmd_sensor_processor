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
    String brokerUrl;
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
    private String billetWasteCache = "0";
    private String semiProfileACache = "0";
    private String semiProfileBCache = "0";

    private boolean heartBeat = false;
    private boolean cutter = false;
    private boolean billetDetecting = false;
    private boolean pullerA = false;
    private boolean pullerB = false;

    private Instant lastDieTemp = Instant.now();
    private Instant lastBilletTemp = Instant.now();
    private Instant lastRampPressure = Instant.now();

    private Instant lastBillet = Instant.now();
    private Instant lastBilletWaste = Instant.now();
    private Instant lastSemiProfile = Instant.now();

    private Instant lastHeartBeat = Instant.now();

    public void connectAndSubscribe(String broker) {
        this.brokerUrl = broker;

        try {
            client = new MqttClient(broker, clientId);

            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setAutomaticReconnect(true); // auto reconnect enabled
            connOpts.setKeepAliveInterval(60);    // send ping every 60 seconds to avoid timeout
            connOpts.setConnectionTimeout(30);    // 30 seconds max wait for connection

            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.err.println("Connection lost: " + cause.getMessage());
                    // AutoReconnect will handle reconnection
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    if (message.isRetained()) return;

                    String payload = new String(message.getPayload());

                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode json = mapper.readTree(payload);
                        JsonNode data = json.get("data");

                        if (data != null) {
                            JsonNode payloadData = data.get("payload");
                            if (payloadData != null) {
                                payloadData.fields().forEachRemaining(entry -> {
                                    if (entry.getKey().contains("/iolinkmaster/port")) {
                                        try {
                                            JsonNode portData = entry.getValue();
                                            String hexString = portData.get("data").asText();
                                            convertData(topic, hexString);
                                        } catch (Exception e) {
                                            System.err.println("Error converting data for topic " + topic + ": " + e.getMessage());
                                        }
                                    }
                                });
                            } else {
                                System.out.println("Payload data is null for topic " + topic);
                            }
                        } else {
                            System.out.println("Data is null for topic " + topic);
                        }
                    } catch (Exception e) {
                        System.err.println("Invalid message format for topic " + topic + ": " + e.getMessage());
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Not used
                }
            });

            System.out.println("Connecting to broker: " + broker);
            client.connect(connOpts);
            System.out.println("Connected");

            subscribeTopics();

        } catch (MqttException e) {
            System.err.println("Error connecting: " + e.getMessage());
        }
    }

    private void subscribeTopics() {
        try {
            for (String topic : topics) {
                client.subscribe(topic);
                System.out.println("Subscribed to topic: " + topic);
            }
        } catch (MqttException e) {
            System.err.println("Subscription failed: " + e.getMessage());
        }
    }

    private void convertData(String topic, String hexString) {
        try {
            Instant now = Instant.now();
            int signalInterval = 1;
            int conditionInterval = 5;
            int productionInterval = 30;

            switch (topic) {
                case "AE_01/condition/die_temp":
                    if (heartBeat && now.isAfter(lastDieTemp.plusSeconds(conditionInterval))) {
                        client.publish("processed/AE_01/condition/die_temp",
                                new MqttMessage(String.valueOf(AemDataConverter.convertDieTemp(hexString)).getBytes()));
                        lastDieTemp = now;
                    }
                    break;

                case "AE_01/condition/billet_temp":
                    if (heartBeat && now.isAfter(lastBilletTemp.plusSeconds(conditionInterval))) {
                        client.publish("processed/AE_01/condition/billet_temp",
                                new MqttMessage(String.valueOf(AemDataConverter.convertBilletTemp(hexString)).getBytes()));
                        lastBilletTemp = now;
                    }
                    break;

                case "AE_01/condition/ramp_pressure":
                    if (heartBeat && now.isAfter(lastRampPressure.plusSeconds(conditionInterval))) {
                        client.publish("processed/AE_01/condition/ramp_pressure",
                                new MqttMessage(String.valueOf(AemDataConverter.convertRampPressure(hexString)).getBytes()));
                        lastRampPressure = now;
                    }
                    break;

                case "AE_01/signal/heart_beat":
                    heartBeat = AemDataConverter.convertHeartBeat(hexString);
                    int heartBeatValue = heartBeat ? 1 : 0;
                    if (now.isAfter(lastHeartBeat.plusSeconds(signalInterval))) {
                        client.publish("processed/AE_01/signal/heart_beat",
                                new MqttMessage(String.valueOf(heartBeatValue).getBytes()));
                        lastHeartBeat = now;
                    }
                    break;

                case "AE_01/signal/cutter":
                    if (!AemDataConverter.convertCutter(hexString)) {
                        if (pullerA && heartBeat && now.isAfter(lastSemiProfile.plusSeconds(productionInterval))) {
                            client.publish("processed/AE_01/production/semi_profile", new MqttMessage(semiProfileBCache.getBytes()));
                            lastSemiProfile = now;
                        }
                        if (pullerB && heartBeat && now.isAfter(lastSemiProfile.plusSeconds(productionInterval))) {
                            client.publish("processed/AE_01/production/semi_profile", new MqttMessage(semiProfileACache.getBytes()));
                            lastSemiProfile = now;
                        }
                    }
                    break;

                case "AE_01/signal/billet_detecting":
                    billetDetecting = AemDataConverter.convertBilletDetecting(hexString);
                    break;

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

                case "AE_01/signal/billet_cutting":
                    if (billetDetecting && !AemDataConverter.convertBilletCutting(hexString)
                            && now.isAfter(lastBillet.plusSeconds(productionInterval))) {
                        client.publish("processed/AE_01/production/billet", new MqttMessage(billetCache.getBytes()));
                        lastBillet = now;
                    }
                    break;

                case "AE_01/signal/end":
                    if (AemDataConverter.convertSignalEnd(hexString)
                            && now.isAfter(lastBilletWaste.plusSeconds(productionInterval))) {
                        client.publish("processed/AE_01/production/billet_waste",
                                new MqttMessage(billetWasteCache.getBytes()));
                        lastBilletWaste = now;
                    }
                    break;

                case "AE_01/signal/puller_A":
                    pullerA = AemDataConverter.convertPullerA(hexString);
                    break;

                case "AE_01/signal/puller_B":
                    pullerB = AemDataConverter.convertPullerB(hexString);
                    break;

                default:
                    System.out.println("Ignoring unknown topic: " + topic);
            }
        } catch (Exception e) {
            System.err.println("Error processing topic " + topic + ": " + e.getMessage());
        }
    }
}
