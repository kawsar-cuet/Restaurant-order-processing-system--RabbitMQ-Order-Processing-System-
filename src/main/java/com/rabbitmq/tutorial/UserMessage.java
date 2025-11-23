package com.rabbitmq.tutorial;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * UserMessage - Represents a real message structure in a messaging system
 * 
 * This models WhatsApp/Messenger message format:
 * - Unique message ID (idempotency key)
 * - Sender & recipient
 * - Content
 * - Timestamps
 * - Delivery state
 */
public class UserMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private String messageId;          // Unique ID for idempotency
    private String senderId;           // Who sent it
    private String recipientId;        // Who receives it
    private String content;            // Message text
    private long timestamp;            // When sent
    private DeliveryState state;       // PENDING, DELIVERED, READ
    private int retryCount;            // Number of delivery attempts

    /**
     * Delivery states (like WhatsApp checkmarks)
     * ✓   DELIVERED - message reached server
     * ✓✓  READ - recipient read it
     */
    public enum DeliveryState {
        PENDING,           // Queued, not yet delivered
        DELIVERED,         // ✓ delivered to recipient
        READ               // ✓✓ read by recipient
    }

    // Constructor
    public UserMessage(String senderId, String recipientId, String content) {
        this.messageId = UUID.randomUUID().toString();
        this.senderId = senderId;
        this.recipientId = recipientId;
        this.content = content;
        this.timestamp = System.currentTimeMillis();
        this.state = DeliveryState.PENDING;
        this.retryCount = 0;
    }

    // Getters
    public String getMessageId() {
        return messageId;
    }

    public String getSenderId() {
        return senderId;
    }

    public String getRecipientId() {
        return recipientId;
    }

    public String getContent() {
        return content;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public DeliveryState getState() {
        return state;
    }

    public int getRetryCount() {
        return retryCount;
    }

    // Setters for state changes
    public void setState(DeliveryState state) {
        this.state = state;
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    @Override
    public String toString() {
        return String.format(
            "UserMessage{id=%s, from=%s→to=%s, content='%s', state=%s, retry=%d}",
            messageId, senderId, recipientId, content, state, retryCount
        );
    }

    /**
     * Convert to JSON for RabbitMQ (simple string representation)
     * In production: use Jackson/Gson for proper serialization
     */
    public static String toJson(UserMessage msg) {
        return String.format(
            "{\"messageId\":\"%s\",\"senderId\":\"%s\",\"recipientId\":\"%s\",\"content\":\"%s\",\"state\":\"%s\"}",
            msg.getMessageId(), msg.getSenderId(), msg.getRecipientId(), msg.getContent(), msg.getState()
        );
    }

    /**
     * Parse from JSON (simple implementation)
     * In production: use proper JSON parser
     */
    public static UserMessage fromJson(String json) {
        // Simplified: just extract fields
        // In production: use Jackson ObjectMapper
        try {
            String senderId = extractField(json, "senderId");
            String recipientId = extractField(json, "recipientId");
            String content = extractField(json, "content");
            UserMessage msg = new UserMessage(senderId, recipientId, content);
            msg.messageId = extractField(json, "messageId");
            msg.state = DeliveryState.valueOf(extractField(json, "state"));
            return msg;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UserMessage from JSON: " + json, e);
        }
    }

    private static String extractField(String json, String fieldName) {
        String pattern = "\"" + fieldName + "\":\"";
        int startIdx = json.indexOf(pattern);
        if (startIdx == -1) {
            return "";
        }
        startIdx += pattern.length();
        int endIdx = json.indexOf("\"", startIdx);
        return json.substring(startIdx, endIdx);
    }
}
