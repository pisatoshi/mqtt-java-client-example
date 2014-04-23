package jp.pika_chu.mqtt;

import java.sql.Timestamp;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttPublish implements MqttCallback {

    public MqttPublish() {
    }

    public static void main(String[] args) {
        new MqttPublish().publish();
    }

    public void publish() {
        try {
            MqttClient client = new MqttClient("tcp://localhost:61613", "pahomqttpublish1");
            client.setCallback(this);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName("admin");
            options.setPassword("password".toCharArray());
            options.setWill("pahodemo/test", "crashed".getBytes(), 2, true);

            client.connect(options);

            MqttMessage message = new MqttMessage();
            message.setPayload("A single message".getBytes());
            client.publish("pahodemo/test", message);

            client.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void connectionLost(Throwable msg) {
        System.out.println(msg.getMessage());
    }

    public void deliveryComplete(IMqttDeliveryToken arg0) {
        System.out.println("Delivery completed.");
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String time = new Timestamp(System.currentTimeMillis()).toString();
        System.out.println("Time:\t" + time
                + "  Topic:\t" + topic
                + "  Message:\t" + new String(message.getPayload())
                + "  QoS:\t" + message.getQos());
    }
}
