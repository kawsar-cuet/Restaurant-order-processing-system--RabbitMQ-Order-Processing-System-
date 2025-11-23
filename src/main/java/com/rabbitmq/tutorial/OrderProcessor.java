package com.rabbitmq.tutorial;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * OrderProcessor - Advanced example with manual acknowledgment
 * 
 * This demonstrates:
 * - Manual message acknowledgment (ensures reliability)
 * - Prefetch count (controls how many messages a worker gets at once)
 * - Error handling
 */
public class OrderProcessor {
    
    private static final String QUEUE_NAME = "order_queue";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        
        // Set prefetch count to 1
        // This means the worker will only get 1 message at a time
        // It won't receive a new message until it acknowledges the previous one
        channel.basicQos(1);
        
        System.out.println("🍳 Order Processor Started (with manual ACK)...\n");
        
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("📥 Received: " + message);
            
            try {
                processOrderWithValidation(message);
                
                // Manual acknowledgment - tells RabbitMQ we successfully processed the message
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println("✅ Acknowledged: " + message + "\n");
                
            } catch (Exception e) {
                System.err.println("❌ Failed to process: " + message);
                
                // Negative acknowledgment - tells RabbitMQ to requeue the message
                // Parameters: delivery tag, multiple, requeue
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                System.out.println("🔄 Requeued: " + message + "\n");
            }
        };
        
        // auto-ack = false means we manually acknowledge messages
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
        
        System.out.println("⏳ Press CTRL+C to exit");
    }
    
    /**
     * Process order with validation
     */
    private static void processOrderWithValidation(String order) throws Exception {
        System.out.println("🔄 Processing order...");
        
        // Simulate validation
        if (order.contains("Pizza")) {
            System.out.println("🍕 Making pizza...");
        } else if (order.contains("Burger")) {
            System.out.println("🍔 Grilling burger...");
        } else if (order.contains("Salad")) {
            System.out.println("🥗 Preparing salad...");
        } else {
            System.out.println("👨‍🍳 Preparing order...");
        }
        
        // Simulate processing time
        TimeUnit.SECONDS.sleep(2);
        
        System.out.println("✨ Order ready!");
    }
}
