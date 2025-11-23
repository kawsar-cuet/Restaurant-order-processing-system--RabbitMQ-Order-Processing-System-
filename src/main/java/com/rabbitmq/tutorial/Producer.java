package com.rabbitmq.tutorial;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/**
 * Producer - Enhanced version with production-like features
 * 
 * Improvements over basic version:
 * ✅ DURABLE queues - survive broker restart
 * ✅ PERSISTENT messages - survive process crash
 * ✅ Per-user queues - messages organized by recipient
 * ✅ Idempotency - each message has unique ID
 * ✅ Proper error handling
 */
public class Producer {
    
    private Connection connection;
    private Channel channel;

    public Producer() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    /**
     * Send a message to a specific user's queue
     * In production: this is how WhatsApp sends messages to individual users
     */
    public void sendMessageToUser(String senderId, String recipientId, String content) throws Exception {
        // Per-user queue (like each WhatsApp user has their own message inbox)
        String queueName = "user_" + recipientId + "_messages";
        
        // Declare queue as DURABLE (survives broker restart)
        // Parameters: name, durable, exclusive, autoDelete, arguments
        channel.queueDeclare(queueName, true, false, false, null);
        
        // Create structured message (like real WhatsApp messages)
        UserMessage userMessage = new UserMessage(senderId, recipientId, content);
        String messageJson = UserMessage.toJson(userMessage);
        
        // Publish with PERSISTENT flag (survives process crash)
        // Parameters: exchange, routing key, properties, body
        channel.basicPublish(
            "",                                              // default exchange
            queueName,                                       // routing key = queue name
            MessageProperties.PERSISTENT_TEXT_PLAIN,       // PERSISTENT flag (important!)
            messageJson.getBytes()
        );
        
        System.out.println("✅ Sent: " + userMessage);
    }

    /**
     * Send a broadcast message to a group (fan-out pattern)
     * In production: group chats use topic exchanges with fan-out
     */
    public void sendGroupMessage(String senderId, String groupId, String[] recipients, String content) throws Exception {
        // Group queue (like group chat inbox)
        String groupQueue = "group_" + groupId + "_messages";
        
        // Declare queue as durable
        channel.queueDeclare(groupQueue, true, false, false, null);
        
        // Create message for group
        UserMessage groupMessage = new UserMessage(senderId, groupId, content);
        String messageJson = UserMessage.toJson(groupMessage);
        
        // Publish to group queue
        channel.basicPublish(
            "",
            groupQueue,
            MessageProperties.PERSISTENT_TEXT_PLAIN,
            messageJson.getBytes()
        );
        
        System.out.println("📢 Group message sent to " + groupId + ": " + content);
    }

    /**
     * Demonstrate 1:1 messaging (like WhatsApp chats)
     */
    public void demonstrateMessaging() throws Exception {
        System.out.println("📤 Producer - Enhanced with Durable Messages\n");
        System.out.println("Demonstrating 1:1 messaging (WhatsApp style)\n");
        
        // User A sends messages to User B
        sendMessageToUser("alice", "bob", "Hello Bob! How are you?");
        Thread.sleep(500);
        
        sendMessageToUser("alice", "bob", "Did you see my message?");
        Thread.sleep(500);
        
        sendMessageToUser("charlie", "bob", "Hi Bob, it's Charlie!");
        Thread.sleep(500);
        
        // Bob receives from multiple senders
        
        System.out.println("\n📢 Demonstrating group messaging\n");
        
        // Group chat: alice sends to team
        sendGroupMessage("alice", "team_engineering", 
            new String[]{"bob", "charlie", "diana"}, 
            "Team standup in 5 minutes!");
        Thread.sleep(500);
        
        System.out.println("\n✨ All messages sent with DURABLE & PERSISTENT flags!");
        System.out.println("   Messages will survive broker restart\n");
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
        Producer producer = null;
        try {
            producer = new Producer();
            producer.demonstrateMessaging();
        } catch (Exception e) {
            System.err.println("❌ Error in Producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (Exception e) {
                System.err.println("Error closing producer: " + e.getMessage());
            }
        }
    }
}
