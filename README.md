# Kafka Hands-On Project: "Order Processing System"

A progressive project. Each level builds on the previous one. Do them **in order** — don't skip. Try to solve each before looking anything up.

> **Theme:** You're building a mini system where orders are placed (producer) and processed (consumer). Simple domain, so you focus on Kafka, not business logic.

---

## Level 0: Setup (Do this first) // Done

**Goal:** Get Kafka running locally so you have something to connect to.

**Tasks:**

1. Install Kafka locally (easiest: use Docker, or download Kafka binaries)
2. Start Kafka (with KRaft mode, no separate Zookeeper needed in newer versions)
3. Create a topic called `orders` with **3 partitions** using the CLI
4. Verify it exists: list topics and describe the `orders` topic

**Success check:** Running the "describe topic" command shows `orders` with 3 partitions.

**Concepts practiced:** topic creation, partitions, broker basics
** Done \*\*\***

---

## Level 1: Hello World Producer

**Goal:** Send your first message.

**Tasks:**

1. Write a Java `KafkaProducer` that sends **one** message to the `orders` topic
2. Message: topic = `orders`, key = `"order-1"`, value = `"Laptop"`
3. Print a confirmation after sending
4. Close the producer properly

**Success check:** Use the CLI console consumer to see your message arrive.

**Concepts practiced:** producer setup, ProducerRecord (topic, key, value), serializers

---

## Level 2: Hello World Consumer

**Goal:** Read the message you sent.

**Tasks:**

1. Write a Java `KafkaConsumer` in a consumer group called `order-processors`
2. Subscribe to the `orders` topic
3. Poll in a loop and print each message's key, value, **partition**, and **offset**
4. Run Level 1 producer again → watch the consumer print it

**Success check:** Consumer prints the message with its partition number and offset.

**Concepts practiced:** consumer setup, poll(), consumer group, offset, deserializers

---

## Level 3: Many Messages + Watch Partitioning

**Goal:** See how keys map to partitions.

**Tasks:**

1. Modify the producer to send **10 orders** in a loop
2. Use different keys: `"user-A"`, `"user-B"`, `"user-C"` (rotate them)
3. In the consumer, print which **partition** each message landed in
4. **Observe:** do all messages with the same key land in the same partition?

**Success check:** You can confirm `"user-A"` messages always go to the same partition.

**Concepts practiced:** key → partition hashing, partition distribution, ordering per key

---

## Level 4: Consumer Group Parallelism

**Goal:** Prove the "1 partition → 1 consumer" rule.

**Tasks:**

1. Run **2 instances** of your Level 2 consumer (same group `order-processors`), in two terminals
2. Send 10 messages from the producer
3. **Observe:** the 3 partitions get split between the 2 consumers (one handles 2, one handles 1)
4. Now start a **3rd** consumer instance → observe rebalancing (each gets 1 partition)
5. Start a **4th** consumer → observe it sits **idle** (no partition left)

**Success check:** You see partitions redistribute as consumers join/leave.

**Concepts practiced:** consumer groups, partition assignment, rebalancing, idle consumers

---

## Level 5: Two Independent Consumer Groups

**Goal:** Prove different groups read the same data independently.

**Tasks:**

1. Run one consumer in group `order-processors` (simulates order fulfillment)
2. Run another consumer in group `analytics` (simulates a reporting system)
3. Send messages from the producer
4. **Observe:** BOTH groups receive **all** the messages (independent offsets)

**Success check:** Same message appears in both consumer groups' output.

**Concepts practiced:** pub-sub, independent offsets per group

---

## Level 6: Manual Offset Commit + Crash Recovery

**Goal:** Understand at-least-once delivery.

**Tasks:**

1. Turn OFF auto-commit (`enable.auto.commit = false`)
2. In the consumer loop: process the message, THEN `commitSync()`
3. Now simulate a crash: read a message, print it, but **kill the app before committing** (comment out the commit, or throw an exception)
4. Restart the consumer → **observe:** it re-reads the uncommitted message

**Success check:** The same message gets processed twice after the "crash".

**Concepts practiced:** manual commit, offset semantics, at-least-once, duplicates

---

## Level 7 (Stretch): Send a JSON Object

**Goal:** Move beyond plain strings.

**Tasks:**

1. Create an `Order` class: `{ orderId, item, price }`
2. Convert it to JSON (use Jackson `ObjectMapper`) and send as the value (String serializer is fine — just send the JSON string)
3. On the consumer side, parse the JSON string back into an `Order` object
4. Print the individual fields

**Success check:** Consumer prints `orderId`, `item`, `price` separately from a received order.

**Concepts practiced:** real-world serialization, structured data over Kafka

---

## Level 8 (Stretch): Grow the Topic

**Goal:** See that partitions don't auto-scale.

**Tasks:**

1. With everything running, **increase** partitions on `orders` from 3 to 6 via CLI
2. Observe your consumer group rebalance to cover the new partitions
3. Try to **decrease** partitions → observe Kafka rejects it

**Success check:** You confirm you can grow but not shrink partitions.

**Concepts practiced:** altering topics, partition scaling limits

---

## Suggested Order of Attack

```
Level 0 → 1 → 2   (get the pipe working)
Level 3 → 4 → 5   (understand partitions & groups)
Level 6           (understand offsets/reliability)
Level 7 → 8       (stretch: real-world touches)
```

## Tips

- Keep producer and consumer as **separate main classes** so you can run them independently
- Use lots of `System.out.println` — seeing partition numbers and offsets is where the learning happens
- Don't aim for clean code first; aim to **observe behavior**. Refactor later.

Good luck — the "aha" moments come at Level 4 and Level 6. Pay close attention there.
# Kafka-Order-Processing-System
