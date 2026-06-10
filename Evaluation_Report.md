# Evaluation Report

## Setup
- 3 Brokers forming a line/tree Simple Routing overlay: b1 -- b2 -- b3
- 3 Subscribers registering a total of 10000 subscriptions
- 2 Publishers sending messages for 30 seconds

## Scenario 1: 100% Equality on 'company'
- Publications Sent: 408
- Delivered Notifications: 121707
- Average Latency: 72.73 ms
- Match Rate: 2.983015%

## Scenario 2: 25% Equality on 'company'
- Publications Sent: 402
- Delivered Notifications: 372408
- Average Latency: 4943.09 ms
- Match Rate: 9.263881%
