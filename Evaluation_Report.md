# Evaluation Report

## Setup
- 3 Brokers forming a ring
- 3 Subscribers registering a total of 10000 subscriptions
- 2 Publishers sending messages for 15 seconds

## Scenario 1: 100% Equality on 'company'
- Publications Sent: 284
- Delivered Notifications: 82200
- Average Latency: 7193.04 ms
- Match Rate: 2.894366%

## Scenario 2: 25% Equality on 'company'
- Publications Sent: 284
- Delivered Notifications: 171900
- Average Latency: 9203.45 ms
- Match Rate: 6.052817%
