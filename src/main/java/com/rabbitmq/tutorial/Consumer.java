package com.rabbitmq.tutorial;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * Consumer - Enhanced version for multi-user messaging
 * 
 * Improvements:
 * ✅ Consumes per-user queues (each user has own inbox)
 * ✅ Structured message handling (UserMessage objects)
 * ✅ Auto-ack (simple) vs manual-ack (reliable)
 * ✅ Proper message deserialization
 * ✅ Simulates real-time message delivery
 */
public class Consumer {
    
    private String userId;  // Which user's queue to consume
    private Connection connection;
    private Channel channel;

    public Consumer(String userId) throws Exception {
        this.userId = userId;
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    /**
     * Consume messages for this user (auto-acknowledge)
     * Simple approach: messages auto-deleted after delivery
     * Used when you don't need strict reliability (or want fast throughput)
     */
    public void consumeMessagesAutoAck() throws Exception {
        String queueName = "user_" + userId + "_messages";
        
        // Declare queue (durable, so it survives restart)
        channel.queueDeclare(queueName, true, false, false, null);
        
        System.out.println("🔔 Consumer for user '" + userId + "' - Waiting for messages...\n");
        
        // Callback: executed when message arrives
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageJson = new String(delivery.getBody());
            
            try {
                // Deserialize and display
                UserMessage msg = UserMessage.fromJson(messageJson);
                System.out.println("📥 [" + userId + "] Received: " + msg);
                
                // Simulate reading the message
                Thread.sleep(1000);
                System.out.println("   ✓ Message read\n");
                
            } catch (Exception e) {
                System.err.println("❌ Error processing message: " + e.getMessage());
            }
        };
        
        // Start consuming (auto-ack = true)
        // With auto-ack: messages deleted automatically
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
        
        System.out.println("⏳ Press CTRL+C to stop consuming\n");
        
        // Keep running
        Thread.currentThread().join();
    }

    /**
     * Advanced: consume with manual acknowledgment (more reliable)
     * Ensures message not lost if consumer crashes
     */
    public void consumeMessagesManualAck() throws Exception {
        String queueName = "user_" + userId + "_messages";
        
        channel.queueDeclare(queueName, true, false, false, null);
        channel.basicQos(1);  // Process 1 message at a time
        
        System.out.println("🔔 Consumer for user '" + userId + "' (MANUAL ACK mode)\n");
        
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageJson = new String(delivery.getBody());
            
            try {
                UserMessage msg = UserMessage.fromJson(messageJson);
                System.out.println("📥 [" + userId + "] Received: " + msg);
                
                // Process message
                Thread.sleep(1000);
                System.out.println("   ✓ Processed\n");
                
                // Manual ACK: tell broker we successfully processed this message
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                
            } catch (Exception e) {
                System.err.println("❌ Error: " + e.getMessage());
                
                // Negative ACK: requeue the message to try again
                try {
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    System.out.println("   🔄 Requeued for retry\n");
                } catch (Exception ex) {
                    System.err.println("Error requeuing: " + ex.getMessage());
                }
            }
        };
        
        // Start consuming (auto-ack = false → manual ACK required)
        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {});
        
        System.out.println("⏳ Press CTRL+C to stop\n");
        Thread.currentThread().join();
    }

    public void close() throws Exception {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: java Consumer <userId> [--manual-ack]");
            System.out.println("Example: java Consumer bob");
            System.out.println("Example: java Consumer bob --manual-ack");
            System.exit(1);
        }
        
        String userId = args[0];
        boolean manualAck = args.length > 1 && args[1].equals("--manual-ack");
        
        Consumer consumer = null;
        try {
            consumer = new Consumer(userId);
            
            if (manualAck) {
                consumer.consumeMessagesManualAck();
            } else {
                consumer.consumeMessagesAutoAck();
            }
            
        } catch (Exception e) {
            System.err.println("❌ Error in Consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (consumer != null) {
                    consumer.close();
                }
            } catch (Exception e) {
                System.err.println("Error closing consumer: " + e.getMessage());
            }
        }
    }
}
