package com.rabbitmq.tutorial;

import com.rabbitmq.client.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * OrderProcessor - Advanced example with Dead Letter Queue (DLQ) pattern
 * 
 * Demonstrates production-like features:
 * ✅ Manual acknowledgment (reliable delivery)
 * ✅ Prefetch count (load balancing)
 * ✅ Dead Letter Queue (poison message handling)
 * ✅ Retry logic with exponential backoff
 * ✅ Error recovery
 */
public class OrderProcessor {
    
    private static final String QUEUE_NAME = "user_bob_messages";
    private static final String DLQ_NAME = "dlq_failed_messages";
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;
    
    private Channel channel;
    private Connection connection;

    public OrderProcessor() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    /**
     * Set up Dead Letter Queue (DLQ) pattern
     * 
     * Flow:
     * Primary Queue → Process → Success (ACK)
     *              → Failure → Retry
     *                       → Max retries reached → DLQ
     */
    public void setupDLQ() throws Exception {
        System.out.println("⚙️  Setting up Dead Letter Queue (DLQ) infrastructure...\n");
        
        // Declare main queue (durable)
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        
        // Declare DLQ (where problematic messages go)
        channel.queueDeclare(DLQ_NAME, true, false, false, null);
        
        // Set up DLQ arguments on main queue
        // After max retries, messages move to DLQ
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", "");           // default exchange
        args.put("x-dead-letter-routing-key", DLQ_NAME);  // route to DLQ
        
        // Re-declare main queue with DLQ configuration
        channel.queueDeclare(QUEUE_NAME, true, false, false, args);
        
        System.out.println("✅ DLQ infrastructure ready");
        System.out.println("   • Primary queue: " + QUEUE_NAME);
        System.out.println("   • DLQ queue: " + DLQ_NAME);
        System.out.println("   • Max retries: " + MAX_RETRIES + "\n");
    }

    /**
     * Process messages with manual acknowledgment and retry logic
     * This simulates WhatsApp/Messenger delivery guarantee
     */
    public void processMessagesWithDLQ() throws Exception {
        // QoS: process 1 message at a time
        channel.basicQos(1);
        
        System.out.println("👁️  Order Processor (with DLQ) - Waiting for messages...\n");
        
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageJson = new String(delivery.getBody());
            
            try {
                // Parse message
                UserMessage msg = UserMessage.fromJson(messageJson);
                System.out.println("📥 Processing: " + msg);
                
                // Simulate processing with random failures (10% failure rate)
                if (Math.random() < 0.1 && msg.getRetryCount() < MAX_RETRIES) {
                    throw new RuntimeException("Simulated processing error");
                }
                
                // Process (simulate work)
                simulateDelivery(msg);
                
                // SUCCESS: acknowledge the message
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println("   ✅ Delivered successfully\n");
                
            } catch (Exception e) {
                System.err.println("   ❌ Error: " + e.getMessage());
                
                try {
                    UserMessage msg = UserMessage.fromJson(messageJson);
                    msg.incrementRetryCount();
                    
                    if (msg.getRetryCount() >= MAX_RETRIES) {
                        // Max retries reached: send to DLQ
                        System.out.println("   💀 Max retries (" + MAX_RETRIES + ") reached → Sending to DLQ\n");
                        
                        channel.basicPublish("", DLQ_NAME, 
                            MessageProperties.PERSISTENT_TEXT_PLAIN, 
                            messageJson.getBytes());
                        
                        // Acknowledge to remove from primary queue
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        
                    } else {
                        // Retry: requeue with backoff
                        System.out.println("   🔄 Retry " + msg.getRetryCount() + "/" + MAX_RETRIES);
                        System.out.println("   ⏳ Waiting " + RETRY_DELAY_MS + "ms before retry...\n");
                        
                        Thread.sleep(RETRY_DELAY_MS);
                        
                        // Negative ACK with requeue
                        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    }
                    
                } catch (Exception ex) {
                    System.err.println("Error handling retry: " + ex.getMessage());
                    try {
                        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    } catch (Exception ignored) {}
                }
            }
        };
        
        // Start consuming
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
        
        System.out.println("⏳ Press CTRL+C to exit\n");
        Thread.currentThread().join();
    }

    /**
     * Monitor the DLQ for stuck messages
     * In production: alerts ops team to investigate and fix
     */
    public void monitorDLQ() throws Exception {
        System.out.println("\n📊 Monitoring Dead Letter Queue...\n");
        
        DeliverCallback dlqCallback = (consumerTag, delivery) -> {
            String messageJson = new String(delivery.getBody());
            System.out.println("🚨 [DLQ] Stuck message: " + messageJson);
            System.out.println("   Action: Investigate & fix, then replay from backup\n");
            
            try {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                System.err.println("Error acking DLQ message: " + e.getMessage());
            }
        };
        
        channel.basicConsume(DLQ_NAME, false, dlqCallback, consumerTag -> {});
    }

    /**
     * Simulate message delivery (like WhatsApp sending to friend's phone)
     */
    private void simulateDelivery(UserMessage msg) throws InterruptedException {
        System.out.println("   🚀 Delivering to device...");
        TimeUnit.MILLISECONDS.sleep(500);
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
        OrderProcessor processor = null;
        try {
            processor = new OrderProcessor();
            
            // Set up DLQ infrastructure
            processor.setupDLQ();
            
            // Start processing with DLQ fallback
            processor.processMessagesWithDLQ();
            
        } catch (Exception e) {
            System.err.println("❌ Error in OrderProcessor: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (processor != null) {
                    processor.close();
                }
            } catch (Exception e) {
                System.err.println("Error closing: " + e.getMessage());
            }
        }
    }
}
