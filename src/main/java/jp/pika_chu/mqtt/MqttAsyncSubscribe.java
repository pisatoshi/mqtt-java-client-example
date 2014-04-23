package jp.pika_chu.mqtt;

import java.io.IOException;
import java.sql.Timestamp;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttAsyncSubscribe implements MqttCallback {

    public MqttAsyncSubscribe() {
    }

    public static void main(String[] args) {
        new MqttAsyncSubscribe().asyncSubscribe();
    }

    public void asyncSubscribe() {
        try {
            MqttAsyncClient asyncClient = new MqttAsyncClient("tcp://localhost:61613", "pahomqttpublish1");
            asyncClient.setCallback(this);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName("admin");
            options.setPassword("password".toCharArray());
            options.setWill("pahodemo/test", "crashed".getBytes(), 2, true);

            IMqttToken conToken = asyncClient.connect(options);
            conToken.waitForCompletion();

            IMqttToken subToken = asyncClient.subscribe("pahodemo/test", 2);
            subToken.waitForCompletion();
            try {
                System.in.read();
            } catch (IOException e) {
                // If we can't read we'll just exit
            }

            IMqttToken discToken = asyncClient.disconnect();
            discToken.waitForCompletion();
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
