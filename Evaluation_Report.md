# Evaluation Report

## Setup
- 3 Brokers forming a line/tree Simple Routing overlay: b1 -- b2 -- b3
- 3 Subscribers registering a total of 10000 subscriptions
- 2 Publishers sending messages for 180 seconds

## Scenario 1: 100% Equality on 'company'
- Publications Sent: 3534
- Delivered Notifications: 1052862
- Average Latency: 169.90 ms
- Match Rate: 2.979236%

## Scenario 2: 25% Equality on 'company'
- Publications Sent: 3527
- Delivered Notifications: 4173233
- Average Latency: 86460.55 ms
- Match Rate: 11.832246%
