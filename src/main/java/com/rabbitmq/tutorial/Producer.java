package com.rabbitmq.tutorial;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

/**
 * Producer - Sends messages to RabbitMQ queue
 * 
 * Think of this as a restaurant's ordering system that sends
 * customer orders to the kitchen.
 */
public class Producer {
    
    private static final String QUEUE_NAME = "order_queue";

    public static void main(String[] args) {
        // Connection factory - configuration for connecting to RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");  // RabbitMQ server location
        factory.setPort(5672);         // Default RabbitMQ port
        
        // Try-with-resources ensures connection and channel are closed automatically
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            
            // Declare a queue (creates if doesn't exist)
            // Parameters: queue name, durable, exclusive, auto-delete, arguments
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            
            // Sample order messages
            String[] orders = {
                "Order #1001: 2x Pizza, 1x Coke",
                "Order #1002: 1x Burger, 1x Fries",
                "Order #1003: 3x Salad, 2x Water",
                "Order #1004: 1x Pasta, 1x Wine",
                "Order #1005: 2x Sandwich, 2x Coffee"
            };
            
            System.out.println("📤 Producer Started - Sending Orders...\n");
            
            // Send each order to the queue
            for (String order : orders) {
                // Publish message to the queue
                // Parameters: exchange, routing key (queue name), props, message body
                channel.basicPublish("", QUEUE_NAME, null, order.getBytes(StandardCharsets.UTF_8));
                System.out.println("✅ Sent: " + order);
                
                // Small delay to simulate real-world ordering
                Thread.sleep(1000);
            }
            
            System.out.println("\n✨ All orders sent successfully!");
            
        } catch (Exception e) {
            System.err.println("❌ Error in Producer: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
