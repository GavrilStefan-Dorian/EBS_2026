# Evaluation Report

## Setup
- 3 Brokers forming a ring
- 3 Subscribers registering a total of 10000 subscriptions
- 2 Publishers sending messages for 15 seconds

## Scenario 1: 100% Equality on 'company'
- Publications Sent: 294
- Delivered Notifications: 72600
- Average Latency: 7476.57 ms
- Match Rate: 2.469388%

## Scenario 2: 25% Equality on 'company'
- Publications Sent: 288
- Delivered Notifications: 147700
- Average Latency: 6082.13 ms
- Match Rate: 5.128472%
