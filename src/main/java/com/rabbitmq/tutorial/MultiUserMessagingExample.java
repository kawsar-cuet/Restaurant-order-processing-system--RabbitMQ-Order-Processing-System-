package com.rabbitmq.tutorial;

import com.rabbitmq.client.*;
import java.util.concurrent.TimeUnit;

/**
 * MultiUserMessagingExample - Realistic WhatsApp/Messenger scenario
 * 
 * Demonstrates:
 * ✅ Multiple users sending messages (1:1 and group chats)
 * ✅ Per-user queue consumption (independent message streams)
 * ✅ Delivery state progression (PENDING → DELIVERED → READ)
 * ✅ Concurrent producer and consumer operations
 * ✅ Real-world patterns at scale
 * 
 * Scenario:
 * - Alice sends message to Bob
 * - Bob sends message to Alice
 * - Alice sends group message to Bob and Charlie
 * - Each user consumes messages from their own queue
 */
public class MultiUserMessagingExample {
    
    private static final String ALICE_QUEUE = "user_alice_messages";
    private static final String BOB_QUEUE = "user_bob_messages";
    private static final String CHARLIE_QUEUE = "user_charlie_messages";
    
    // Shared connection (in real systems, connection pooling is used)
    private static Connection connection;
    
    public static void main(String[] args) throws Exception {
        // Initialize RabbitMQ connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        
        connection = factory.newConnection();
        
        System.out.println("╔════════════════════════════════════════════════════════════╗");
        System.out.println("║  Multi-User Messaging Example (WhatsApp-like System)      ║");
        System.out.println("║  Demonstrates: Per-user queues, delivery states, concurrency║");
        System.out.println("╚════════════════════════════════════════════════════════════╝\n");
        
        // Setup queues
        setupQueues();
        
        // Start consumers in separate threads
        startConsumerThread(ALICE_QUEUE, "Alice");
        startConsumerThread(BOB_QUEUE, "Bob");
        startConsumerThread(CHARLIE_QUEUE, "Charlie");
        
        // Give consumers time to start
        Thread.sleep(1000);
        
        // Simulate messaging scenario
        runMessagingScenario();
        
        // Keep running
        System.out.println("\n⏳ Messaging demo running... Press CTRL+C to exit\n");
        Thread.currentThread().join();
    }
    
    /**
     * Set up durable queues for each user
     */
    private static void setupQueues() throws Exception {
        Channel channel = connection.createChannel();
        
        System.out.println("⚙️  Creating queues for users...\n");
        
        for (String queue : new String[]{ALICE_QUEUE, BOB_QUEUE, CHARLIE_QUEUE}) {
            channel.queueDeclare(queue, true, false, false, null);
            System.out.println("   ✅ " + queue);
        }
        
        System.out.println("\n");
        channel.close();
    }
    
    /**
     * Start a consumer in a separate thread
     */
    private static void startConsumerThread(String queueName, String userName) {
        new Thread(() -> {
            try {
                consumeMessages(queueName, userName);
            } catch (Exception e) {
                System.err.println("Error in " + userName + " consumer: " + e.getMessage());
            }
        }).start();
    }
    
    /**
     * Consume messages for a specific user
     */
    private static void consumeMessages(String queueName, String userName) throws Exception {
        Channel channel = connection.createChannel();
        channel.basicQos(1);  // Process 1 message at a time
        
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageJson = new String(delivery.getBody());
            
            try {
                UserMessage msg = UserMessage.fromJson(messageJson);
                
                // Display received message with delivery state
                System.out.println("📱 [" + userName + "] Received message:");
                System.out.println("   From: " + msg.getSenderId());
                System.out.println("   Content: " + msg.getContent());
                System.out.println("   State: " + msg.getState());
                System.out.println("   Time: " + new java.text.SimpleDateFormat("HH:mm:ss")
                    .format(new java.util.Date(msg.getTimestamp())));
                
                // Simulate reading the message after 2 seconds
                Thread.sleep(2000);
                msg.markAsRead();
                System.out.println("   ✓✓ Marked as READ\n");
                
                // Acknowledge the message
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                
            } catch (Exception e) {
                System.err.println("Error processing message for " + userName + ": " + e.getMessage());
                try {
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                } catch (Exception ignored) {}
            }
        };
        
        // Start consuming
        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {});
        
        System.out.println("👁️  " + userName + " is listening on: " + queueName);
    }
    
    /**
     * Simulate realistic messaging scenario
     * 
     * Timeline:
     * T+1s: Alice sends to Bob
     * T+3s: Bob sends to Alice
     * T+5s: Alice sends to Bob, Charlie (group)
     * T+7s: Charlie sends to Alice
     */
    private static void runMessagingScenario() throws Exception {
        // Timeline for message sends
        simulateDelay(1, "Alice sends to Bob");
        sendMessage("alice", "bob", "Hey Bob, how are you?");
        
        simulateDelay(2, "Bob receives and replies");
        sendMessage("bob", "alice", "Hi Alice! I'm good, thanks for asking!");
        
        simulateDelay(2, "Alice sends group message");
        sendMessage("alice", "bob", "Group msg: Anyone want to grab coffee?");
        sendMessage("alice", "charlie", "Group msg: Anyone want to grab coffee?");
        
        simulateDelay(2, "Charlie joins conversation");
        sendMessage("charlie", "alice", "Coffee sounds great!");
    }
    
    /**
     * Send a message from sender to recipient
     */
    private static void sendMessage(String senderId, String recipientId, String content) 
            throws Exception {
        
        Channel channel = connection.createChannel();
        
        // Determine recipient queue
        String queueName = "user_" + recipientId + "_messages";
        
        // Declare queue (idempotent)
        channel.queueDeclare(queueName, true, false, false, null);
        
        // Create structured message
        UserMessage msg = new UserMessage(
            java.util.UUID.randomUUID().toString(),  // messageId
            senderId,                                  // senderId
            recipientId,                               // recipientId
            content,                                   // content
            System.currentTimeMillis(),                // timestamp
            UserMessage.DeliveryState.PENDING,         // initial state
            0                                          // retryCount
        );
        
        // Send persistent message (survives broker restart)
        channel.basicPublish("", queueName, 
            MessageProperties.PERSISTENT_TEXT_PLAIN, 
            msg.toJson().getBytes());
        
        System.out.println("💬 [" + senderId.toUpperCase() + "] → [" + recipientId.toUpperCase() + "]: " 
            + content + "\n");
        
        channel.close();
    }
    
    /**
     * Simulate delay and display timeline
     */
    private static void simulateDelay(int seconds, String action) throws InterruptedException {
        System.out.println("⏳ Waiting " + seconds + "s... (" + action + ")\n");
        TimeUnit.SECONDS.sleep(seconds);
    }
    
    /**
     * Clean up resources
     */
    public static void closeConnection() throws Exception {
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }
}
