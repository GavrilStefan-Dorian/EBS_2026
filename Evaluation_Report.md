# Evaluation Report

## Setup
- 3 Brokers forming a fully connected Simple Routing overlay
- 3 Subscribers registering a total of 10000 subscriptions
- 2 Publishers sending messages for 180 seconds

## Scenario 1: 100% Equality on 'company'
- Publications Sent: 3491
- Delivered Notifications: 1015300
- Average Latency: 71.11 ms
- Match Rate: 2.908336%

## Scenario 2: 25% Equality on 'company'
- Publications Sent: 3378
- Delivered Notifications: 2420100
- Average Latency: 41210.81 ms
- Match Rate: 7.164298%
