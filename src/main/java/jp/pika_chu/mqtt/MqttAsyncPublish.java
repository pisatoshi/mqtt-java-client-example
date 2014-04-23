package jp.pika_chu.mqtt;

import java.sql.Timestamp;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttAsyncPublish implements MqttCallback {

    public MqttAsyncPublish() {
    }

    public static void main(String[] args) {
        new MqttAsyncPublish().asyncPublish();
    }

    public void asyncPublish() {
        try {
            MqttAsyncClient asyncClient = new MqttAsyncClient("tcp://localhost:61613", "pahomqttpublish1");
            asyncClient.setCallback(this);
 
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName("admin");
            options.setPassword("password".toCharArray());
            options.setWill("pahodemo/test", "crashed".getBytes(), 2, true);

            IMqttToken conToken = asyncClient.connect(options);
            conToken.waitForCompletion();

            MqttMessage message = new MqttMessage();
            message.setPayload("A single message".getBytes());
            IMqttDeliveryToken pubToken = asyncClient.publish("pahodemo/test", message);
            pubToken.waitForCompletion();

            IMqttToken discToken = asyncClient.disconnect();
            discToken.waitForCompletion();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void connectionLost(Throwable msg) {
        System.out.println(msg.getMessage());
    }

    public void deliveryComplete(IMqttDeliveryToken deliveryToken) {
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
