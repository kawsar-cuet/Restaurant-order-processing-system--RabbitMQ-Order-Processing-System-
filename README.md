# 🐰 RabbitMQ Order Processing System

A simple Java project demonstrating **RabbitMQ message queue** concepts through a restaurant order processing system.

## 📚 What You'll Learn

- **Message Queues Basics**: Producer-Consumer pattern
- **RabbitMQ**: Setting up and using RabbitMQ with Java
- **Asynchronous Processing**: Decoupling sender and receiver
- **Message Acknowledgment**: Ensuring reliable message delivery

## 🎯 Project Overview

This project simulates a restaurant ordering system:
- **Producer**: Customer ordering system (sends orders)
- **Queue**: Order queue (stores pending orders)
- **Consumer**: Kitchen (receives and processes orders)

```
Customer Order → Producer → RabbitMQ Queue → Consumer → Kitchen Processing
```

## 📋 Prerequisites

1. **Java 11+** installed
2. **Maven** installed
3. **RabbitMQ Server** running locally

## 🚀 Setup Instructions

### Step 1: Install RabbitMQ

**Windows (using Chocolatey):**
```powershell
choco install rabbitmq
```

**Or download from:** https://www.rabbitmq.com/download.html

**Start RabbitMQ:**
```powershell
# RabbitMQ should start automatically, or run:
rabbitmq-server
```

**Verify RabbitMQ is running:**
- Open browser: http://localhost:15672
- Login: username=`guest`, password=`guest`

### Step 2: Build the Project

```powershell
mvn clean compile
```

### Step 3: Run the Examples

**Terminal 1 - Start Consumer (Kitchen):**
```powershell
mvn exec:java -Dexec.mainClass="com.rabbitmq.tutorial.Consumer"
```

**Terminal 2 - Send Messages (Producer):**
```powershell
mvn exec:java -Dexec.mainClass="com.rabbitmq.tutorial.Producer"
```

**Alternative - Advanced Consumer with Manual ACK:**
```powershell
mvn exec:java -Dexec.mainClass="com.rabbitmq.tutorial.OrderProcessor"
```

## 📁 Project Structure

```
Java/
├── src/main/java/com/rabbitmq/tutorial/
│   ├── Producer.java           # Sends messages to queue
│   ├── Consumer.java           # Receives messages (auto-ack)
│   └── OrderProcessor.java     # Advanced consumer (manual-ack)
├── pom.xml                     # Maven dependencies
└── README.md                   # This file
```

## 🔍 Key Concepts Explained

### 1. **Producer (Sender)**
```java
channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
```
- Sends messages to a queue
- Doesn't wait for processing
- Can send multiple messages quickly

### 2. **Consumer (Receiver)**
```java
channel.basicConsume(QUEUE_NAME, true, deliverCallback, cancelCallback);
```
- Listens for messages continuously
- Processes each message
- `true` = auto-acknowledgment (simple but less reliable)

### 3. **Manual Acknowledgment**
```java
channel.basicAck(deliveryTag, false);  // Success
channel.basicNack(deliveryTag, false, true);  // Failure - requeue
```
- More reliable - message won't be lost if processing fails
- Consumer must explicitly confirm processing

### 4. **Prefetch Count**
```java
channel.basicQos(1);
```
- Controls load balancing
- Worker gets 1 message at a time
- Won't receive new message until current one is acknowledged

## 🎮 Try This!

1. **Start Consumer first**, then Producer
2. **Start multiple Consumers** - watch load balancing!
3. **Send messages while Consumer is down** - they'll queue up
4. **Compare auto-ack vs manual-ack** behavior

## 🆚 RabbitMQ vs Kafka

| Feature | RabbitMQ | Kafka |
|---------|----------|-------|
| **Best For** | Task queues, request-reply | Event streaming, logs |
| **Message Retention** | Until consumed | Configurable time-based |
| **Ordering** | Per queue | Per partition |
| **Learning Curve** | Easier | Steeper |
| **Performance** | ~20K msgs/sec | ~100K+ msgs/sec |

## 🐛 Troubleshooting

**Connection Refused?**
- Ensure RabbitMQ is running: check http://localhost:15672
- Check port 5672 is not blocked

**Class Not Found?**
- Run `mvn clean compile` first

**Messages Not Being Consumed?**
- Make sure Consumer is running
- Check queue in RabbitMQ management console

## 📖 Next Steps

- Add multiple queues for different order types
- Implement topic exchanges (routing by pattern)
- Add dead letter queues for failed messages
- Try priority queues
- Explore RabbitMQ management API

## 🔗 Resources

- [RabbitMQ Official Tutorials](https://www.rabbitmq.com/getstarted.html)
- [Java Client API Guide](https://www.rabbitmq.com/api-guide.html)
- [RabbitMQ Management Plugin](https://www.rabbitmq.com/management.html)

---

**Happy Learning! 🎉**
