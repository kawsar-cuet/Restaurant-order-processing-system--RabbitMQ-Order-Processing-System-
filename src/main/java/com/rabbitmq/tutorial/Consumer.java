package com.rabbitmq.tutorial;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

/**
 * Consumer - Receives and processes messages from RabbitMQ queue
 * 
 * Think of this as the kitchen that receives and processes
 * customer orders from the ordering system.
 */
public class Consumer {
    
    private static final String QUEUE_NAME = "order_queue";

    public static void main(String[] args) throws Exception {
        // Connection factory - configuration for connecting to RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        
        // Declare the same queue (ensures it exists)
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        
        System.out.println("🍳 Consumer Started - Waiting for orders...\n");
        
        // Callback function that processes each message
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("📥 Received: " + message);
            
            try {
                // Simulate processing time (cooking the food)
                processOrder(message);
                System.out.println("✅ Processed: " + message + "\n");
            } catch (InterruptedException e) {
                System.err.println("❌ Error processing: " + message);
                Thread.currentThread().interrupt();
            }
        };
        
        // Start consuming messages
        // Parameters: queue name, auto-ack, deliver callback, cancel callback
        // auto-ack = true means messages are automatically acknowledged
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
        
        System.out.println("⏳ Press CTRL+C to exit");
    }
    
    /**
     * Simulates order processing (e.g., preparing food)
     */
    private static void processOrder(String order) throws InterruptedException {
        System.out.println("🔄 Processing order...");
        Thread.sleep(2000);  // Simulate 2 seconds of work
    }
}
