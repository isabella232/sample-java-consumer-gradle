package io.kafkaesque;

import org.apache.pulsar.client.api.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class simpleConsumer {

    private static final String SERVICE_URL = "pulsar+ssl://useast1.gcp.kafkaesque.io:6651";

    public static void main(String[] args) throws IOException
    {

        // Create client object
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(
                        AuthenticationFactory.token("<INSERT CLIENT TOKEN HERE>")
                )
                .build();

        // Create consumer on a topic with a subscription
        Consumer consumer = client.newConsumer()
                .topic("chris-kafkaesque-io/local-useast1-gcp/tc1-messages")
                .subscriptionName("my-subscription")
                .subscribe();

        boolean receivedMsg = false;
        // Loop until a message is received
        do {
            // Block for up to 1 second for a message
            Message msg = consumer.receive(1, TimeUnit.SECONDS);

            if(msg != null){
                System.out.printf("Message received: %s", new String(msg.getData()));

                // Acknowledge the message to remove it from the message backlog
                consumer.acknowledge(msg);

                receivedMsg = true;
            }

        } while (!receivedMsg);

        //Close the consumer
        consumer.close();

        // Close the client
        client.close();

    }

}