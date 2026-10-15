# distributed-notifier

[![npm version](https://img.shields.io/npm/v/distributed-notifier.svg?style=flat-square&color=0284c7)](https://www.npmjs.com/package/distributed-notifier)
[![npm downloads](https://img.shields.io/npm/dm/distributed-notifier.svg?style=flat-square&color=blue)](https://www.npmjs.com/package/distributed-notifier)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0_Strict-3178c6.svg?style=flat-square)](https://www.typescriptlang.org/)
[![Build & Test](https://img.shields.io/badge/Tests-38_Passing-10b981.svg?style=flat-square)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-gray.svg?style=flat-square)](LICENSE)

**A production-grade, distributed notification engine engineered for high-throughput, fault-tolerant delivery across Push (FCM/APNs), In-App WebSockets, Email, SMS, and Webhooks.**

---

## Table of Contents

1. [Package Overview](#1-package-overview)
2. [Key Architectural Highlights](#2-key-architectural-highlights)
3. [Technology Stack](#3-technology-stack)
4. [Folder Structure](#4-folder-structure)
5. [Architecture & Flow Diagrams](#5-architecture--flow-diagrams)
   - [5.1 High-Level Distributed Architecture](#51-high-level-distributed-architecture)
   - [5.2 Asynchronous Dispatch vs Synchronous Delivery](#52-asynchronous-dispatch-vs-synchronous-delivery)
   - [5.3 Notification Lifecycle Flow](#53-notification-lifecycle-flow)
   - [5.4 Circuit Breaker State Machine](#54-circuit-breaker-state-machine)
   - [5.5 Retry, Fallback & Dead Letter Queue (DLQ)](#55-retry-fallback--dead-letter-queue-dlq)
   - [5.6 Graceful Shutdown & Resource Lifecycle](#56-graceful-shutdown--resource-lifecycle)
6. [Modes of Operation](#6-modes-of-operation)
   - [In-Memory Mode (Zero-Infrastructure)](#in-memory-mode-zero-infrastructure)
   - [Distributed Mode (True Redis Shared State)](#distributed-mode-true-redis-shared-state)
7. [Installation & Configuration](#7-installation--configuration)
8. [Complete Usage Examples](#8-complete-usage-examples)
   - [8.1 Asynchronous Background Dispatch (`dispatch`)](#81-asynchronous-background-dispatch-dispatch)
   - [8.2 Direct Synchronous Delivery (`sendSync`)](#82-direct-synchronous-delivery-sendsync)
   - [8.3 Category & Topic Preferences (Opt-Out with Security Override)](#83-category--topic-preferences-opt-out-with-security-override)
   - [8.4 Reusable Multi-Channel Template Guide (HTML Email, SMS, Push, In-App, Webhook)](#84-reusable-multi-channel-template-guide-html-email-sms-push-in-app-webhook)
   - [8.5 Provider Traffic Shaping & Backpressure](#85-provider-traffic-shaping--backpressure)
   - [8.6 Durable Dead Letter Queue (DLQ) Inspection](#86-durable-dead-letter-queue-dlq-inspection)
   - [8.7 Bounded Bulk Delivery (`sendBulk`)](#87-bounded-bulk-delivery-sendbulk)
   - [8.8 Built-in Webhook Delivery (Slack, Discord, Custom APIs)](#88-built-in-webhook-delivery-slack-discord-custom-apis)
   - [8.9 Observability & Lifecycle Event Hooks (`EventEmitter`)](#89-observability--lifecycle-event-hooks-eventemitter)
9. [Adding Custom Providers](#9-adding-custom-providers)
10. [Resource Safety & Memory Guarantees](#10-resource-safety--memory-guarantees)
11. [Testing & Verification](#11-testing--verification)
12. [Frequently Asked Questions (FAQ)](#12-frequently-asked-questions-faq)
13. [Author & Maintainer](#13-author--maintainer)
14. [License](#14-license)

---

## 1. Package Overview

Sending notifications in distributed environments involves critical challenges: third-party provider timeouts (APNs/FCM), duplicate deliveries during network retries, spamming users during quiet hours, cross-server race conditions in cluster deployments, and thread exhaustion when downstream HTTP APIs degrade.

`distributed-notifier` is engineered as a complete, self-contained notification engine that can run in:
* **Zero-Dependency In-Memory Mode**: Using a Binary Min-Heap $O(\log N)$ priority scheduler, token bucket rate limiter, and sliding-window deduplication for local development and single-server apps.
* **True Distributed Redis Mode**: Powered by BullMQ background workers, atomic Redis `SET NX EX` deduplication, atomic Redis Lua script token buckets, and persistent Redis Dead Letter Queues for multi-instance horizontal scaling.

### Why distributed-notifier?

| Feature | `distributed-notifier` | Cloud SaaS (Novu / Knock / Courier) | DIY Custom Scripts |
| :--- | :--- | :--- | :--- |
| **Hosting & Privacy** | **100% Self-Hosted (Zero Data Leakage)** | Third-Party Cloud (Monthly Fees) | Self-Hosted |
| **Local Zero-Setup Mode** | **Yes (Built-in In-Memory Min-Heap)** | No (Requires Cloud Account / Docker) | Ad-hoc / Fragile |
| **Distributed Multi-Node** | **Yes (BullMQ + Redis Lua Scripts)** | Vendor Managed | Difficult to Build |
| **Egress Traffic Shaper** | **Yes (Leaky Bucket Request Pacing)** | Vendor Rate Limits | Manual Try/Catch |
| **Resilience & Breakers** | **Yes (Leak-Free Circuit Breaker)** | Platform-dependent | DIY |
| **Vectorized Multicast** | **Yes (Chunks 500 FCM Tokens)** | Limited | Manual |
| **License & Pricing** | **MIT (Free & Open Source Forever)** | Usage-Based Tiers ($$$) | Free |

---

## 2. Key Architectural Highlights

* **Immediate Dispatch vs Synchronous Send**:
  * `notifier.dispatch(payload)`: Sub-millisecond enqueue returning a stable `jobId` for instant HTTP API responses.
  * `notifier.sendSync(payload)`: Awaits direct third-party provider delivery with full multi-channel fallback trace.
* **True Distributed State in Redis Mode**:
  * Cross-node Idempotency (`RedisDeduplicationEngine`) via atomic `SET key 1 NX EX <ttl>`.
  * Cluster-wide Token Bucket Rate Limiting (`RedisTokenBucketRateLimiter`) via atomic Redis Lua script.
  * Durable Dead Letter Queue (`RedisDeadLetterQueue`) indexed by Sorted Set with automatic bounded trimming.
* **Provider Traffic Shaper (Leaky Bucket)**:
  * Integrated directly into `BaseProvider` to smooth burst traffic and eliminate HTTP 429 Too Many Requests errors.
  * Backpressure capacity limits prevent memory leaks during extended downstream outages.
* **Leak-Free Circuit Breaker**:
  * Guaranteed timer cleanup in `Promise.race()` prevents lingering Node.js timer handles.
  * Concurrency guard on `HALF_OPEN` state prevents test probe stampedes.
* **Advanced Template Engine**:
  * Nested object paths (`{{user.profile.name}}`, `{{order.items.0.price}}`).
  * Conditionals (`{{#if isVip}}VIP Special{{else}}Standard{{/if}}`).
  * Multi-language localization (i18n) with automatic locale fallback.
  * Safe HTML escaping with optional raw `{{{var}}}` injection.
* **Category & Topic Subscriptions**:
  * Recipient opt-out preferences per topic (e.g., disable `marketing` while keeping `security` and `transactional`).
* **AWS-Standard Full Jitter Backoff**:
  * Eliminates the Thundering Herd Problem across distributed retries.
* **Vectorized Multicast Push**:
  * Chunks device tokens into batches of 500 (FCM standard) for 95% reduced network roundtrips.

---

## 3. Technology Stack

| Layer | Technology | Role & Purpose |
| :--- | :--- | :--- |
| **Language** | **TypeScript 5 (Strict)** | Strict types, declaration maps, and interface contracts. |
| **Validation** | **Zod** | Pre-flight payload validation at public boundaries. |
| **In-Memory Queue** | **Binary Min-Heap ($O(\log N)$)** | High-throughput priority scheduling without linear scan overhead. |
| **Distributed Queue** | **BullMQ & ioredis** | Multi-node cluster background job queue and retry pipeline. |
| **Rate Limiter** | **Token Bucket (In-Memory & Redis Lua)** | Per-recipient anti-spam and provider rate limiting. |
| **Resilience** | **Circuit Breaker & Leaky Bucket Shaper** | Downstream fault-tolerance and egress throttling. |
| **Testing** | **Node.js Native Test Runner (`--test`)** | Fast, zero-dependency test runner with `--test-force-exit`. |

---

## 4. Folder Structure

```text
packages/distributed-notifier/
├── src/
│   ├── types/
│   │   ├── notification.ts     # Recipient, Payload, Channel, Result & Dispatch interfaces
│   │   ├── provider.ts         # IProvider contract & MulticastResult
│   │   ├── queue.ts            # IQueueEngine, Job, and RetryPolicy
│   │   └── config.ts           # NotifierConfig and connection options
│   ├── validations/
│   │   └── schemas.ts          # Zod validation schemas
│   ├── engines/
│   │   ├── queue/
│   │   │   ├── in-memory-queue.ts  # Min-Heap priority queue with backpressure
│   │   │   └── bullmq-queue.ts     # BullMQ distributed queue adapter
│   │   ├── deduplication.ts        # InMemory & RedisDeduplicationEngine
│   │   ├── rate-limiter.ts         # InMemory & RedisTokenBucketRateLimiter
│   │   ├── template-engine.ts      # Nested paths, conditionals & i18n renderer
│   │   ├── quiet-hours.ts          # Timezone-aware quiet hours engine
│   │   ├── digest-engine.ts        # Micro-event batching & flush aggregation
│   │   ├── dead-letter-queue.ts    # InMemory & durable RedisDeadLetterQueue
│   │   └── traffic-shaper.ts       # Leaky Bucket Provider Traffic Shaper
│   ├── providers/
│   │   ├── base.provider.ts        # Abstract BaseProvider with circuit breaker & shaper
│   │   ├── push.provider.ts        # Push adapter (FCM / APNs) with multicast
│   │   ├── socket.provider.ts      # WebSocket / Socket.io adapter
│   │   ├── email.provider.ts       # Email adapter
│   │   ├── sms.provider.ts         # SMS adapter
│   │   └── mock.provider.ts        # Mock provider for testing
│   ├── services/
│   │   ├── fallback-router.ts      # Multi-channel delivery and fallback router
│   │   └── notifier.service.ts     # Main Orchestrator Facade (Notifier)
│   ├── utils/
│   │   ├── circuit-breaker.ts      # Memory-safe circuit breaker
│   │   ├── min-heap.ts             # Binary Min-Heap data structure
│   │   ├── backoff.ts              # Full Jitter Exponential Backoff algorithm
│   │   └── logger.ts               # Structured logger
│   └── index.ts                    # Public API exports
├── tests/
│   ├── circuit-breaker.test.ts     # Circuit breaker resilience & timer tests
│   ├── dispatch-async.test.ts      # Async dispatch & topic preference tests
│   ├── min-heap.test.ts            # Min-Heap algorithmic tests
│   ├── multicast.test.ts           # Push multicast batching tests
│   ├── notifier.test.ts            # Core integration tests
│   ├── provider-traffic-shaping.test.ts # Provider traffic shaping tests
│   ├── quiet-hours.test.ts         # Quiet hours unit tests
│   ├── rate-limiter.test.ts        # Rate limiter unit tests
│   ├── redis-distributed.test.ts   # Real Redis concurrency & BullMQ integration tests
│   ├── stress-lifecycle.test.ts    # 1,000 notifications heap & shutdown tests
│   ├── template-advanced.test.ts   # Advanced template engine tests
│   └── traffic-shaper.test.ts      # Traffic shaper unit tests
├── package.json
├── tsconfig.json
└── README.md
```

---

## 5. Architecture & Flow Diagrams

### 5.1 High-Level Distributed Architecture

<p align="center">
  <img src="https://raw.githubusercontent.com/rakiburrahman307/distributed-notifier/main/assets/diagrams/01-architecture.svg" alt="High-Level Distributed Architecture" width="100%" />
</p>

<details>
<summary>View Diagram Source</summary>

```mermaid
graph TD
    App["Your Application (Express / NestJS / Fastify)"] -->|"notifier.dispatch(payload)"| Core["Notifier Service Facade"]
    App -->|"notifier.sendSync(payload)"| Core

    subgraph "Validation & Pre-Flight Gate"
        Core --> Val["Zod Schema Validator"]
        Core --> Tpl["Template Engine (Nested Paths / i18n)"]
        Core --> Pref["Topic & Channel Preferences"]
        Core --> Dedupe{"Deduplication Engine"}
        Core --> Rate{"Rate Limiter Engine"}
        Core --> Quiet{"Quiet Hours Engine"}
    end

    subgraph "Distributed Shared State (Redis Mode)"
        Dedupe -->|"SET NX EX"| RedisDedup[("Redis Deduplication")]
        Rate -->|"Lua Token Bucket"| RedisRate[("Redis Rate Limiter")]
    end

    subgraph "Queue & Processing Layer"
        Core -->|"dispatch()"| QueueRouter{"Queue Engine Mode"}
        QueueRouter -->|"mode: 'memory'"| MemQueue["In-Memory Priority Queue (Min-Heap)"]
        QueueRouter -->|"mode: 'redis'"| BullQueue["BullMQ Distributed Cluster"]
    end

    subgraph "Delivery & Provider Layer"
        MemQueue --> Router["Fallback Router"]
        BullQueue --> Router
        Core -->|"sendSync()"| Router
        Router -->|"Shaped & Breaker Protected"| Push["Push Provider (FCM)"]
        Router -->|"Shaped & Breaker Protected"| Mail["Email Provider (Resend)"]
        Router -->|"Fallback Channel"| Sms["SMS Provider (Twilio)"]
    end

    subgraph "Durable Dead Letter Queue"
        Router -->|"Failed Retries"| DLQ["Dead Letter Queue"]
        DLQ -->|Redis Mode| RedisDLQ[("Redis DLQ Sorted Set")]
        DLQ -->|Memory Mode| MemDLQ["In-Memory Map DLQ"]
    end
```
</details>

---

### 5.2 Asynchronous Dispatch vs Synchronous Delivery

<p align="center">
  <img src="https://raw.githubusercontent.com/rakiburrahman307/distributed-notifier/main/assets/diagrams/02-dispatch-flow.svg" alt="Asynchronous Dispatch vs Synchronous Delivery" width="100%" />
</p>

<details>
<summary>View Diagram Source</summary>

```mermaid
sequenceDiagram
    autonumber
    actor Client as API Endpoint
    participant N as Notifier
    participant Q as Queue Engine
    participant W as Background Worker
    participant P as Providers

    Note over Client,N: Asynchronous Dispatch (Non-Blocking)
    Client->>N: dispatch(payload)
    N->>N: Preflight (Validate, Dedupe, RateLimit, Topics)
    N->>Q: enqueue(payload)
    Q-->>N: jobId
    N-->>Client: DispatchResult { status: "QUEUED", jobId: "job_123" } [~2ms response]

    Note over Q,P: Asynchronous Background Processing
    Q->>W: Process job
    W->>P: Deliver via Fallback Router
    P-->>W: DeliveryResult { success: true }

    Note over Client,N: Synchronous Delivery (Awaiting Providers)
    Client->>N: sendSync(payload)
    N->>N: Preflight checks
    N->>P: Deliver through providers & wait
    P-->>N: DeliveryResult
    N-->>Client: NotificationResult { status: "SENT", deliveredAt: Date }
```
</details>

---

### 5.3 Notification Lifecycle Flow

<p align="center">
  <img src="https://raw.githubusercontent.com/rakiburrahman307/distributed-notifier/main/assets/diagrams/03-lifecycle.svg" alt="Notification Lifecycle Flow" width="100%" />
</p>

<details>
<summary>View Diagram Source</summary>

```mermaid
flowchart TD
    Start(["Notification Request"]) --> Validate{"Zod Validation Pass?"}
    Validate -->|No| ThrowErr["Throw ValidationError"]
    Validate -->|Yes| CheckTopic{"Recipient Subscribed to Topic?"}

    CheckTopic -->|Opted Out & Not Critical| SuppressTopic["Return status: UNSUBSCRIBED"]
    CheckTopic -->|Subscribed or High Priority| CheckDedupe{"Idempotency Key Exists?"}

    CheckDedupe -->|Duplicate| RetDupe["Return status: SKIPPED_DUPLICATE"]
    CheckDedupe -->|New Key| CheckRate{"Rate Limit Tokens Available?"}

    CheckRate -->|Exceeded| RetRate["Return status: RATE_LIMITED"]
    CheckRate -->|Allowed| CheckQuiet{"Recipient Quiet Hours Active?"}

    CheckQuiet -->|Yes & Not High Priority| SchedDelay["Schedule at wake-up time"]
    CheckQuiet -->|No or Urgent OTP| CheckAsync{"Async Dispatch or Sync Send?"}

    CheckAsync -->|dispatch()| PushQueue["Enqueue in Queue & Return QUEUED"]
    CheckAsync -->|sendSync()| ExecDelivery["Execute Fallback Router"]

    ExecDelivery --> DeliverySuccess{"Delivery Succeeded?"}
    DeliverySuccess -->|Yes| DoneSent["Return status: SENT"]
    DeliverySuccess -->|Primary Failed| CheckFallback{"Fallback Channels Configured?"}
    CheckFallback -->|Yes| ExecFallback["Attempt Fallback Provider"]
    CheckFallback -->|No| PushDLQ["Push to Dead Letter Queue (DLQ)"]
    ExecFallback --> FallbackSuccess{"Fallback Succeeded?"}
    FallbackSuccess -->|Yes| DonePartial["Return status: PARTIALLY_DELIVERED"]
    FallbackSuccess -->|No| PushDLQ
```
</details>

---

### 5.4 Circuit Breaker State Machine

<p align="center">
  <img src="https://raw.githubusercontent.com/rakiburrahman307/distributed-notifier/main/assets/diagrams/04-circuit-breaker.svg" alt="Circuit Breaker State Machine" width="100%" />
</p>

<details>
<summary>View Diagram Source</summary>

```mermaid
stateDiagram-v2
    [*] --> CLOSED

    CLOSED --> OPEN : Consecutive failures >= failureThreshold
    note right of CLOSED
      Normal operation.
      Calls execute downstream.
    end note

    OPEN --> HALF_OPEN : cooldownPeriodMs elapsed
    note right of OPEN
      Fast-fails all requests.
      Protects degraded providers.
    end note

    HALF_OPEN --> CLOSED : Probe request succeeds
    HALF_OPEN --> OPEN : Probe request fails or times out
    note right of HALF_OPEN
      Limits concurrent probes to 1.
      Prevents downstream stampede.
    end note
```
</details>

---

### 5.5 Retry, Fallback & Dead Letter Queue (DLQ)

<p align="center">
  <img src="https://raw.githubusercontent.com/rakiburrahman307/distributed-notifier/main/assets/diagrams/05-retry-dlq.svg" alt="Retry, Fallback & Dead Letter Queue" width="100%" />
</p>

<details>
<summary>View Diagram Source</summary>

```mermaid
sequenceDiagram
    autonumber
    participant Router as Fallback Router
    participant Push as Primary Provider
    participant Sms as Fallback Provider
    participant DLQ as Dead Letter Queue
    participant Alert as DLQ Listeners (Sentry / Slack)

    Router->>Push: send(payload)
    alt Primary Provider Times Out
        Push-->>Router: DeliveryResult { success: false, error: "APNs timeout" }
        Router->>Sms: send(payload) [Fallback Triggered]
        alt Fallback Provider Succeeds
            Sms-->>Router: DeliveryResult { success: true, messageId: "sms_99" }
            Router-->>Router: NotificationResult { status: "PARTIALLY_DELIVERED", isFallbackUsed: true }
        else Fallback Provider Also Fails
            Sms-->>Router: DeliveryResult { success: false, error: "Twilio 500" }
            Router->>DLQ: push(job, error)
            DLQ->>Alert: onDeadLetter(entry)
        end
    end
```
</details>

---

### 5.6 Graceful Shutdown & Resource Lifecycle

<p align="center">
  <img src="https://raw.githubusercontent.com/rakiburrahman307/distributed-notifier/main/assets/diagrams/06-shutdown.svg" alt="Graceful Shutdown & Resource Lifecycle" width="100%" />
</p>

<details>
<summary>View Diagram Source</summary>

```mermaid
flowchart TD
    ShutdownStart(["notifier.close() Called"]) --> CheckClosed{"Already Closed?"}
    CheckClosed -->|Yes| ReturnImmediate["Return Immediately (Idempotent)"]
    CheckClosed -->|No| SetFlag["Mark isClosed = true"]

    SetFlag --> StopDigest["Destroy Digest Timers"]
    StopDigest --> CloseQueue["Close Queue & Disconnect Sockets"]
    CloseQueue --> CloseDedupe["Disconnect Redis Deduplication"]
    CloseDedupe --> CloseRate["Disconnect Redis Rate Limiter"]
    CloseRate --> CloseDLQ["Disconnect Redis Dead Letter Queue"]
    CloseDLQ --> CloseProviders["Close Provider Traffic Shapers & Breakers"]
    CloseProviders --> ShutdownComplete(["All Handles Cleaned Up Cleanly"])
```
</details>

---

## 6. Modes of Operation

### In-Memory Mode (Zero-Infrastructure)
* **Zero External Dependencies**: Runs entirely inside the Node.js process.
* **$O(\log N)$ Priority Min-Heap**: Sub-millisecond queue scheduling without array sorting bottlenecks.
* **Bounded Maps**: Internal caches feature maximum capacity bounds with LRU/FIFO eviction to eliminate unbounded memory growth.
* **Best For**: Development, local testing, continuous integration (CI), and single-instance backend servers.

### Distributed Mode (True Redis Shared State)
* **BullMQ Distributed Queue**: Multiple instances process jobs from shared Redis queues.
* **Atomic Cross-Instance Deduplication**: Prevents duplicate deliveries even when parallel requests hit separate servers.
* **Atomic Distributed Rate Limiting**: Shared token bucket via Redis Lua scripts protects against distributed spam.
* **Durable Dead Letter Queue**: Failed notifications persist across server restarts and deployments.
* **Best For**: Multi-node Kubernetes clusters, serverless environments, and high-throughput production microservices.

---

## 7. Installation & Configuration

```bash
# Core package (Zero dependencies)
npm install distributed-notifier zod

# Optional: For distributed Redis mode
npm install bullmq ioredis
```

---

## 8. Complete Usage Examples

### 8.1 Asynchronous Background Dispatch (`dispatch`)

Use `dispatch()` in web handlers (Express/NestJS) for instant responses:

```typescript
import { Notifier, PushProvider } from 'distributed-notifier';

const notifier = new Notifier({
  mode: 'redis',
  redis: process.env.REDIS_URL || 'redis://localhost:6379',
  providers: [
    new PushProvider({
      name: 'fcm-main',
      sender: async (tokens, notification) => {
        // Integrate with firebase-admin
        return { success: true, messageId: 'fcm_msg_101' };
      },
    }),
  ],
});

// Fast non-blocking enqueue (~2ms)
const ack = await notifier.dispatch({
  id: 'order_assigned_1001',
  idempotencyKey: 'idemp_order_1001',
  recipient: {
    id: 'user_42',
    pushTokens: ['fcm_token_xyz'],
  },
  channels: ['push'],
  title: 'Driver Assigned',
  body: 'Your driver is on the way!',
});

console.log(ack.status); // "QUEUED"
console.log(ack.jobId);  // "order_assigned_1001"
```

---

### 8.2 Direct Synchronous Delivery (`sendSync`)

Use `sendSync()` when the caller requires immediate delivery results:

```typescript
const result = await notifier.sendSync({
  id: 'otp_verification',
  recipient: {
    id: 'user_99',
    phone: '+8801700000000',
    pushTokens: ['token_abc'],
  },
  channels: ['push'],
  fallbackChannels: ['sms'], // Automatically falls back to SMS if Push fails
  priority: 'HIGH',          // Bypasses quiet hours
  title: 'Verification Code',
  body: 'Your OTP is 849201',
});

console.log(result.status);         // "SENT" or "PARTIALLY_DELIVERED"
console.log(result.isFallbackUsed); // true if delivered via SMS
```

---

### 8.3 Category & Topic Preferences (Opt-Out with Security Override)

```typescript
// Recipient opted out of 'marketing', but kept 'security'
const payload = {
  id: 'promo_spring_sale',
  topic: 'marketing',
  recipient: {
    id: 'user_77',
    preferences: {
      topics: {
        marketing: false, // User unsubscribed
        security: true,
      },
    },
  },
  channels: ['push', 'email'],
  title: 'Spring Sale 50% Off',
};

const result = await notifier.sendSync(payload);
console.log(result.status); // "UNSUBSCRIBED" (Suppressed safely)

// Critical notifications with priority: 'HIGH' bypass opt-out:
const criticalAlert = {
  id: 'sec_alert_1',
  topic: 'security',
  priority: 'HIGH',
  recipient: payload.recipient,
  channels: ['email'],
  title: 'Unauthorized Login Attempt',
};

const alertResult = await notifier.sendSync(criticalAlert);
console.log(alertResult.status); // "SENT"
```

---

### 8.4 Reusable Multi-Channel Template Guide (HTML Email, SMS, Push, In-App, Webhook)

Templates decouple message layout and copywriting from your backend business logic. Register templates once with dynamic placeholders (`{{variable}}`, `{{{rawUrl}}}`, `{{#if condition}}`), and trigger them from anywhere using `templateId`.

---

#### 8.4.1 Rich HTML Email Templates (Resend / SendGrid / Nodemailer)

Supports styled HTML layouts, buttons, conditionals (`{{#if isVip}}`), and raw unescaped URLs (`{{{activationUrl}}}`):

```typescript
import { Notifier, EmailProvider } from 'distributed-notifier';

const notifier = new Notifier();

// Step 1: Register Reusable HTML Template
notifier.registerTemplate({
  id: 'welcome_email',
  defaultChannels: ['email'],
  title: 'Welcome aboard, {{user.name}}!',
  body: `
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 24px; background: #f8fafc; border-radius: 8px;">
      <h2 style="color: #0f172a;">Hello, {{user.name}}!</h2>
      <p style="color: #475569; font-size: 16px;">Thank you for signing up with us.</p>
      
      {{#if isVip}}
        <p style="color: #d97706; font-weight: bold; background: #fef3c7; padding: 8px 12px; border-radius: 4px;">
          VIP Membership Activated: Priority Access Unlocked!
        </p>
      {{/if}}

      <div style="margin-top: 24px;">
        <a href="{{{activationUrl}}}" style="background: #0284c7; color: #ffffff; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">
          Verify Email Address
        </a>
      </div>
      <p style="color: #94a3b8; font-size: 12px; margin-top: 24px;">Button not working? Paste this link: {{{activationUrl}}}</p>
    </div>
  `,
});

// Step 2: Register Email Provider
notifier.registerProvider(
  new EmailProvider({
    name: 'resend-mailer',
    from: 'Acme Team <no-reply@acme.com>',
    sender: async (to, subject, htmlBody) => {
      // htmlBody receives the fully rendered HTML string
      const res = await resend.emails.send({
        from: 'Acme Team <no-reply@acme.com>',
        to,
        subject,
        html: htmlBody,
      });
      return { success: !res.error, messageId: res.data?.id };
    },
  })
);

// Step 3: Dispatch in Background (~2ms)
await notifier.dispatch({
  id: 'welcome_user_1',
  recipient: { id: 'user_1', email: 'alex@example.com' },
  channels: ['email'],
  templateId: 'welcome_email',
  variables: {
    user: { name: 'Alex Johnson' },
    isVip: true,
    activationUrl: 'https://acme.com/verify?token=xyz123abc&email=alex@example.com',
  },
});
```

---

#### 8.4.2 SMS OTP & Transaction Alert Templates (Twilio / Infobip)

SMS messages require compact, character-efficient formatting with security codes and expiration timers:

```typescript
import { Notifier, SmsProvider } from 'distributed-notifier';

const notifier = new Notifier();

// Step 1: Register SMS Template
notifier.registerTemplate({
  id: 'sms_otp_verification',
  defaultChannels: ['sms'],
  title: 'Security Verification',
  body: 'Your {{app.name}} verification code is {{otp.code}}. Valid for {{otp.expiresMinutes}} minutes. Do not share this code with anyone.',
});

// Step 2: Register SMS Provider
notifier.registerProvider(
  new SmsProvider({
    name: 'twilio-sms',
    from: '+18005550199',
    sender: async (to, body) => {
      // Send via Twilio or any SMS gateway
      const message = await twilioClient.messages.create({
        to,
        from: '+18005550199',
        body,
      });
      return { success: true, messageId: message.sid };
    },
  })
);

// Step 3: Send Directly (Synchronous OTP Verification)
const result = await notifier.sendSync({
  id: 'otp_req_99',
  priority: 'HIGH', // High priority bypasses quiet hours
  recipient: { id: 'usr_88', phone: '+1234567890' },
  channels: ['sms'],
  templateId: 'sms_otp_verification',
  variables: {
    app: { name: 'DistributedApp' },
    otp: { code: '849201', expiresMinutes: 5 },
  },
});

console.log(result.status); // "SENT"
```

---

#### 8.4.3 Mobile Push Notification Templates (Firebase FCM / Apple APNs)

Push templates format titles, bodies, and custom deep-linking metadata for mobile devices:

```typescript
import { Notifier, PushProvider } from 'distributed-notifier';

const notifier = new Notifier();

// Step 1: Register Push Template with deep-link metadata
notifier.registerTemplate({
  id: 'order_status_push',
  defaultChannels: ['push'],
  title: 'Order #{{order.id}} {{order.status}}!',
  body: 'Hi {{user.name}}, your driver {{driver.name}} is {{driver.etaMinutes}} minutes away.',
  data: {
    screen: 'order_tracking',
    trackingUrl: 'myapp://orders/track',
  },
});

// Step 2: Register FCM Push Provider with Multicast support
notifier.registerProvider(
  new PushProvider({
    name: 'firebase-fcm',
    sender: async (tokens, notification) => {
      // Automatically batches up to 500 device tokens per network roundtrip
      const response = await admin.messaging().sendEachForMulticast({
        tokens,
        notification: {
          title: notification.title,
          body: notification.body,
        },
        data: {
          screen: String(notification.data?.screen || ''),
        },
      });
      return { success: response.failureCount === 0, messageId: `fcm_${Date.now()}` };
    },
  })
);

// Step 3: Dispatch Push Notification
await notifier.dispatch({
  id: 'push_order_status',
  recipient: {
    id: 'customer_77',
    pushTokens: ['fcm_device_token_abc', 'apns_device_token_xyz'],
  },
  channels: ['push'],
  templateId: 'order_status_push',
  variables: {
    user: { name: 'Sarah' },
    order: { id: 'ORD-5481', status: 'Dispatched' },
    driver: { name: 'David', etaMinutes: 12 },
  },
});
```

---

#### 8.4.4 Real-Time In-App & WebSocket Templates (Socket.io / ws)

For real-time dashboard notifications, bell badges, and live banner alerts:

```typescript
import { Notifier, SocketProvider } from 'distributed-notifier';

const notifier = new Notifier();

// Step 1: Register In-App Template
notifier.registerTemplate({
  id: 'in_app_badge_unlocked',
  defaultChannels: ['in_app'],
  title: 'Achievement Unlocked!',
  body: 'Congratulations {{user.name}}! You earned the "{{badge.title}}" badge.',
  data: {
    type: 'toast',
    iconUrl: 'https://cdn.example.com/icons/trophy.png',
  },
});

// Step 2: Register Socket Provider connected to your WebSocket Server
notifier.registerProvider(
  new SocketProvider({
    name: 'socket-io',
    emitter: async (userId, event, payload) => {
      // Emit to recipient's dedicated Socket.io room
      io.to(`user:${userId}`).emit('notification:received', {
        title: payload.title,
        body: payload.body,
        data: payload.data,
        timestamp: Date.now(),
      });
      return { success: true, messageId: `sock_${Date.now()}` };
    },
  })
);

// Step 3: Dispatch In-App Event
await notifier.dispatch({
  id: 'achieve_88',
  recipient: { id: 'user_42' },
  channels: ['in_app'],
  templateId: 'in_app_badge_unlocked',
  variables: {
    user: { name: 'Michael' },
    badge: { title: 'Early Adopter' },
  },
});
```

---

#### 8.4.5 Webhook Notification Templates (Slack, Discord & Custom APIs)

Deliver structured notification payloads to external team channels or microservice endpoints:

```typescript
import { Notifier, WebhookProvider } from 'distributed-notifier';

const notifier = new Notifier();

// Step 1: Register Webhook Template
notifier.registerTemplate({
  id: 'slack_incident_alert',
  defaultChannels: ['webhook'],
  title: 'Critical Incident: {{service.name}}',
  body: 'Service {{service.name}} reported error: {{incident.message}} in region {{service.region}}.',
});

// Step 2: Register Webhook Provider
notifier.registerProvider(
  new WebhookProvider({
    name: 'slack-incidents',
    headers: { Authorization: 'Bearer xoxb-slack-bot-token' },
    timeoutMs: 5000,
  })
);

// Step 3: Dispatch to dynamic Slack or Discord URL
await notifier.dispatch({
  id: 'incident_911',
  priority: 'HIGH',
  recipient: { id: 'devops_oncall' },
  channels: ['webhook'],
  templateId: 'slack_incident_alert',
  metadata: {
    webhookUrl: 'https://hooks.slack.com/services/T00/B00/XXXX',
  },
  variables: {
    service: { name: 'Payment Gateway', region: 'us-east-1' },
    incident: { message: 'Database connection pool exhausted' },
  },
});
```

---

#### 8.4.6 Multi-Language (i18n) Localization & Fallbacks

Provide language translations with automatic fallback to `defaultLocale` if a key or language is omitted:

```typescript
notifier.registerTemplate({
  id: 'trip_receipt',
  defaultLocale: 'en',
  title: 'Trip #{{trip.id}} Receipt',
  body: 'Hello {{user.name}}, your fare was ${{trip.fare.total}}. {{#if isVip}}Thank you for being a VIP!{{else}}Join VIP for 10% off!{{/if}}',
  locales: {
    bn: {
      title: 'ট্রিপ #{{trip.id}} রসিদ',
      body: 'হ্যালো {{user.name}}, আপনার ভাড়া ${{trip.fare.total}}। {{#if isVip}}ভিআইপি হওয়ার জন্য ধন্যবাদ!{{else}}১০% ছাড়ের জন্য ভিআইপিতে যোগ দিন!{{/if}}',
    },
    es: {
      title: 'Recibo del viaje #{{trip.id}}',
      body: 'Hola {{user.name}}, tu tarifa fue de ${{trip.fare.total}}.',
    },
  },
});

// Automatically renders in Bengali based on recipient locale:
const res = await notifier.sendSync({
  id: 'receipt_bn_1',
  templateId: 'trip_receipt',
  recipient: {
    id: 'u_1',
    locale: 'bn', // Triggers Bengali template
    pushTokens: ['device_token_123'],
  },
  channels: ['push'],
  variables: {
    user: { name: 'রাকিবুর' },
    trip: { id: 'TRIP-99', fare: { total: 250 } },
    isVip: true,
  },
});

console.log(res.status); // "SENT"
```

---

### 8.5 Provider Traffic Shaping & Backpressure

Protect third-party providers (SendGrid, Twilio, FCM) from rate-limit throttling:

```typescript
import { PushProvider } from 'distributed-notifier';

const rateLimitedPush = new PushProvider({
  name: 'fcm-metered',
  trafficShaperOptions: {
    maxRequestsPerSecond: 25, // Leaky Bucket pacing: 1 request every 40ms
    maxQueueSize: 500,        // Backpressure limit
  },
  circuitBreakerOptions: {
    failureThreshold: 5,      // Trip OPEN after 5 consecutive failures
    cooldownPeriodMs: 15000,  // Test recovery after 15s
    timeoutMs: 5000,          // 5s request timeout with automatic timer cleanup
  },
});

notifier.registerProvider(rateLimitedPush);
```

---

### 8.6 Durable Dead Letter Queue (DLQ) Inspection

```typescript
const dlq = notifier.getDeadLetterQueue();

// Register alert webhook (e.g. Slack / Sentry)
dlq.onDeadLetter(async (entry) => {
  console.error(`Alert: Job ${entry.job.id} failed permanently:`, entry.error.message);
});

// Query failed entries
const totalFailed = await dlq.count();
const recentFailures = await dlq.getAll(10);
```

---

### 8.7 Bounded Bulk Delivery (`sendBulk`)

```typescript
const bulkPayloads = users.map((u) => ({
  id: `blast_${u.id}`,
  recipient: { id: u.id, email: u.email },
  channels: ['email'],
  title: 'System Maintenance Notice',
}));

// Dispatches with controlled parallel pool of 10 workers
const results = await notifier.sendBulk(bulkPayloads, { concurrency: 10, async: true });
```

---

### 8.8 Built-in Webhook Delivery (Slack, Discord, Custom APIs)

The built-in `WebhookProvider` delivers structured JSON payloads directly over HTTP/HTTPS with automatic timeout safety and custom headers:

```typescript
import { Notifier, WebhookProvider } from 'distributed-notifier';

const notifier = new Notifier();

// Register generic or channel-specific webhook provider
const slackWebhook = new WebhookProvider({
  name: 'slack-ops',
  headers: { Authorization: 'Bearer xoxb-...' },
  timeoutMs: 5000,
});

notifier.registerProvider(slackWebhook);

// Dispatch notification with dynamic target URL
await notifier.dispatch({
  id: 'deploy_notice_1',
  recipient: { id: 'devops-team' },
  channels: ['webhook'],
  title: 'Production Deployment Succeeded',
  body: 'Release v1.2.0 is live across all cluster nodes',
  metadata: {
    webhookUrl: 'https://hooks.slack.com/services/T00/B00/XXXX',
  },
});
```

---

### 8.9 Observability & Lifecycle Event Hooks (`EventEmitter`)

`Notifier` extends Node.js `EventEmitter` with fully-typed lifecycle hooks. Easily stream events to Datadog, Prometheus, Slack alerts, or database audit trails without modifying core business logic:

```typescript
import { Notifier } from 'distributed-notifier';

const notifier = new Notifier();

// 1. Audit trail for every queued or suppressed job
notifier.on('dispatched', (result, payload) => {
  console.log(`[DISPATCHED] Job ${result.jobId} -> Status: ${result.status}`);
});

// 2. Metrics for successful third-party provider delivery
notifier.on('delivered', (result, payload) => {
  statsd.increment('notifications.delivered', { channels: result.channelsAttempted });
});

// 3. Automated alerting on delivery failure
notifier.on('failed', (result, payload) => {
  console.error(`[ALERT] Notification ${payload.id} delivery failed:`, result.channelResults);
});

// 4. Opt-out and suppression telemetry
notifier.on('suppressed', ({ reason, payload }) => {
  console.warn(`[SUPPRESSED] Notification ${payload.id} skipped due to ${reason}`);
});

// 5. Permanent dead-letter tracking
notifier.on('dead_letter', (entry) => {
  pagerDuty.trigger({ message: entry.error.message, payload: entry.job.payload });
});
```

---

## 9. Adding Custom Providers

Implement custom channels by subclassing `BaseProvider`:

```typescript
import { BaseProvider, ChannelDeliveryResult, NotificationPayload } from 'distributed-notifier';

export class DiscordWebhookProvider extends BaseProvider {
  public readonly name = 'discord-notifier';
  public readonly channel = 'webhook' as const;

  constructor(private readonly webhookUrl: string) {
    super({
      trafficShaperOptions: { maxRequestsPerSecond: 5 },
      circuitBreakerOptions: { failureThreshold: 3, timeoutMs: 4000 },
    });
  }

  protected async deliver(payload: NotificationPayload): Promise<ChannelDeliveryResult> {
    const res = await fetch(this.webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content: `**${payload.title}**: ${payload.body}` }),
    });

    if (!res.ok) {
      return {
        success: false,
        channel: 'webhook',
        provider: this.name,
        error: `Discord responded with status ${res.status}`,
        timestamp: Date.now(),
      };
    }

    return {
      success: true,
      channel: 'webhook',
      provider: this.name,
      messageId: `discord_${Date.now()}`,
      timestamp: Date.now(),
    };
  }
}

notifier.registerProvider(new DiscordWebhookProvider(process.env.DISCORD_URL!));
```

---

## 10. Resource Safety & Memory Guarantees

* **Zero Circuit Breaker Timer Leaks**: Timeout promises explicitly clear their scheduled timer via a `finally` block on every resolution, rejection, or timeout event.
* **Deterministic HALF_OPEN Probe Concurrency**: Exactly one probe is permitted to test downstream health in `HALF_OPEN` state, preventing concurrent test storms.
* **Bounded In-Memory Stores**: All internal maps (`Deduplication`, `RateLimiter`, `DeadLetterQueue`, `Queue`) enforce hard item caps with automatic eviction.
* **Graceful & Idempotent Shutdown**: Calling `notifier.close()` safely disposes all timers, drains queue workers, cleanly disconnects Redis sockets, and releases provider resources.

---

## 11. Testing & Verification

Run the complete test suite:

```bash
# Run unit, integration, concurrency, and stress tests
npm test

# Run TypeScript type check
npm run typecheck

# Build production bundle
npm run build
```

---

## 12. Frequently Asked Questions (FAQ)

#### Can I use distributed-notifier without Redis?
Yes. By default, `distributed-notifier` runs in Zero-Dependency In-Memory mode. It uses an internal $O(\log N)$ Binary Min-Heap for priority scheduling, sliding-window maps for deduplication, and local token buckets for rate limiting. No Redis, Docker, or external daemon is required for local development or single-server monoliths.

#### How does distributed-notifier eliminate duplicate notifications across clusters?
In distributed mode, `distributed-notifier` leverages atomic Redis `SET key 1 NX EX <ttl>` commands. Even if multiple API servers receive duplicate webhook events or retry identical payloads concurrently, only the first request acquires the lock and is dispatched. All subsequent requests are safely suppressed as `SKIPPED_DUPLICATE`.

#### How does the Fallback Router handle provider outages?
When you define `channels: ['push']` and `fallbackChannels: ['sms', 'email']`, the engine attempts delivery via the primary provider first. If the primary provider times out or trips its Circuit Breaker, the router automatically fails over to the fallback channel in real time without dropping the message.

#### How does Provider Traffic Shaping prevent HTTP 429 errors?
External APIs like Twilio, SendGrid, and Firebase enforce strict per-second egress limits. `distributed-notifier` embeds an asynchronous Leaky Bucket traffic shaper directly into `BaseProvider`. Rapid bursts are queued in memory and dispatched at a smooth, constant rate (e.g. 25 req/sec), eliminating downstream 429 Too Many Requests errors.

#### Is distributed-notifier safe against memory leaks?
Yes. Every timer handle created by timeout promises and circuit breakers is captured and explicitly cleared in `Promise.race()` `finally` blocks. All internal in-memory stores have hard capacity limits with automated eviction, and `.close()` provides an idempotent, leak-free shutdown sequence.

---

## 13. Author & Maintainer

**Md Rakibur Rahman**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Md_Rakibur_Rahman-0A66C2?style=flat-square&logo=linkedin)](https://www.linkedin.com/in/iamrakiburrahman/)
[![GitHub](https://img.shields.io/badge/GitHub-rakiburrahman307-181717?style=flat-square&logo=github)](https://github.com/rakiburrahman307)
[![Email](https://img.shields.io/badge/Email-rakiburrahman.dev@gmail.com-D14836?style=flat-square&logo=gmail&logoColor=white)](mailto:rakiburrahman.dev@gmail.com)

* **LinkedIn**: [linkedin.com/in/iamrakiburrahman](https://www.linkedin.com/in/iamrakiburrahman/)
* **GitHub**: [@rakiburrahman307](https://github.com/rakiburrahman307)
* **Email**: [rakiburrahman.dev@gmail.com](mailto:rakiburrahman.dev@gmail.com)

---

## 14. License

Distributed under the **MIT License**. See [LICENSE](LICENSE) for details.
