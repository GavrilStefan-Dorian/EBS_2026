import subprocess
import time
import json
import os
import signal
import sys

def start_python(*args):
    if os.name == "nt":
        return subprocess.Popen(
            [sys.executable, *args],
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
        )

    return subprocess.Popen([sys.executable, *args])


def stop_subscriber(process):
    try:
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.send_signal(signal.SIGINT)
    except Exception:
        process.terminate()
        
        
def wait_for_queues_to_drain(brokers_count=3, subs_count=3, ignored_brokers=None):
    import pika
    ignored_brokers = ignored_brokers or set()
    
    print("Waiting for all queues to drain completely...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    
    queues_to_check = []
    for i in range(1, brokers_count + 1):
        broker_id = f"b{i}"

        if broker_id in ignored_brokers:
            continue

        queues_to_check.extend([
            f'broker.{broker_id}.subs',
            f'broker.{broker_id}.routing',
            f'broker.{broker_id}.pubs',
            f'broker.{broker_id}.forwarded_pubs',
        ])
    for i in range(1, subs_count + 1):
        queues_to_check.append(f'subscriber.sub{i}.notifications')
        
    idle_count = 0
    while idle_count < 5:
        total_messages = 0
        for q in queues_to_check:
            try:
                res = channel.queue_declare(queue=q, passive=True)
                total_messages += res.method.message_count
            except pika.exceptions.ChannelClosedByBroker:
                channel = connection.channel()
                total_messages += 1
            except Exception:
                pass
                
        if total_messages == 0:
            idle_count += 1
        else:
            idle_count = 0
        time.sleep(1)
        
    connection.close()
    print("All queues empty, proceeding.")

def check_processes_alive(processes, name, allowed_dead=None):
    allowed_dead = allowed_dead or set()

    for i, p in enumerate(processes, start=1):
        process_name = f"{name}{i}"

        if p.poll() is not None and process_name not in allowed_dead:
            raise RuntimeError(f"{process_name} died during evaluation")
        
def run_scenario(
    scenario_name,
    generated_dir,
    duration=180,
    num_subs=10000,
    num_pubs=2,
    simulate_failure=False,
    failure_after=60,
    failed_broker="b2"
):
    print(f"\n--- Running Scenario: {scenario_name} ---")
    for fname in [f"pub{i}_metrics.json" for i in range(1, num_pubs + 1)] + [f"sub{i}_metrics.json" for i in range(1, 4)]:
        if os.path.exists(fname):
            os.remove(fname)
            
    subs_file = os.path.join(generated_dir, "subscriptions.txt")
    pubs_file = os.path.join(generated_dir, "publications.txt")
    
    if not os.path.exists(subs_file) or not os.path.exists(pubs_file):
        raise FileNotFoundError(f"Generated files not found in {generated_dir}. Please run pubsub/evaluation/generate_data.py first.")
    
    # 1. Start Coordinator
    coord = start_python("pubsub/coordinator/coordinator.py")
    time.sleep(2)
    
    # 2. Start 3 Brokers
    brokers = []
    for i in range(1, 4):
        b = start_python("pubsub/broker/broker.py", "--id", f"b{i}")
        brokers.append(b)
    time.sleep(3)
    
    # 3. Start 3 Subscribers (approx 3333 each)
    subs = []
    offsets = [0, 3333, 6666]
    counts = [3333, 3333, 3334]

    for i in range(1, 4):
        count = counts[i - 1]
        offset = offsets[i - 1]
        subscriber_args = [
            "pubsub/subscriber/subscriber.py",
            "--id", f"sub{i}",
            "--file", subs_file,
            "--count", str(count),
            "--offset", str(offset)
        ]

        if simulate_failure:
            subscriber_args.extend(["--replication-factor", "2"])
        else:
            subscriber_args.extend(["--replication-factor", "1"])

        s = start_python(*subscriber_args)

        
        subs.append(s)
    
    print(f"Waiting for subscriptions to register...")
    time.sleep(2)
    wait_for_queues_to_drain()
    check_processes_alive(brokers, "broker b")
    check_processes_alive(subs, "subscriber sub")
    time.sleep(5)
    check_processes_alive(brokers, "broker b")
    check_processes_alive(subs, "subscriber sub")
    
    # 4. Start 2 Publishers
    pubs = []
    for i in range(1, 3):
        publisher_args = [
            "pubsub/publisher/publisher.py",
            "--id", f"pub{i}",
            "--file", pubs_file,
            "--duration", str(duration),
            "--rate", "10"
        ]

        if simulate_failure:
            publisher_args.append("--fault-tolerance")

        p = start_python(*publisher_args)
        pubs.append(p)

    if simulate_failure:
        import threading

        def kill_broker_later():
            time.sleep(failure_after)
            idx = int(failed_broker[1:]) - 1

            print(f"Simulating broker failure: terminating {failed_broker}")
            brokers[idx].terminate()
            try:
                brokers[idx].wait(timeout=5)
            except subprocess.TimeoutExpired:
                brokers[idx].kill()

            time.sleep(5)

            print(f"Restarting failed broker {failed_broker}")
            brokers[idx] = start_python("pubsub/broker/broker.py", "--id", failed_broker)

        threading.Thread(target=kill_broker_later, daemon=True).start()
        
    print(f"Publishing for {duration} seconds...")
    
    
    for p in pubs:
        p.wait()
        
    print("Publishing finished.")

    ignored_brokers = {failed_broker} if simulate_failure else set()
    allowed_dead = {f"broker {failed_broker}"} if simulate_failure else set()

    wait_for_queues_to_drain(ignored_brokers=ignored_brokers)
    check_processes_alive(brokers, "broker b", allowed_dead=allowed_dead)
    check_processes_alive(subs, "subscriber sub")
    
    time.sleep(20)
    
    for s in subs:
        stop_subscriber(s)

    for s in subs:
        try:
            s.wait(timeout=5)
        except subprocess.TimeoutExpired:
            s.terminate()

    for b in brokers:
        b.terminate()

    coord.terminate()
    
    time.sleep(2)
    
    total_pubs_sent = 0
    for i in range(1, num_pubs + 1):
        metric_file = f"pub{i}_metrics.json"
        if os.path.exists(metric_file):
            try:
                with open(metric_file, "r") as f:
                    data = json.load(f)
                    total_pubs_sent += data['sent']
            except json.JSONDecodeError:
                pass
            os.remove(metric_file)
        else:
            print(f"Metric file {metric_file} not found")

    total_received = 0
    total_latency = 0    
    for i in range(1, 4):
        metric_file = f"sub{i}_metrics.json"
        print(f"Checking {metric_file}...")
        if os.path.exists(metric_file):
            with open(metric_file, "r") as f:
                data = json.load(f)
                total_received += data['received']
                total_latency += data['total_latency_ms']
                print(f"Loaded {metric_file}: {data}")
            os.remove(metric_file)
        else:
            print(f"Metric file {metric_file} NOT FOUND!")
            
    avg_latency = total_latency / total_received if total_received > 0 else 0
    
    match_rate = total_received / (total_pubs_sent * num_subs) if total_pubs_sent > 0 else 0
    
    print(f"Scenario Results:")
    print(f"Total pubs sent: {total_pubs_sent}")
    print(f"Total delivered: {total_received}")
    print(f"Average latency: {avg_latency:.2f} ms")
    print(f"Match rate: {match_rate*100:.6f}%")
    
    return total_received, avg_latency, match_rate, total_pubs_sent

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--duration', type=int, default=180, help='Duration in seconds to publish')
    parser.add_argument('--simulate-failure', action='store_true')
    parser.add_argument('--failure-after', type=int, default=60)
    parser.add_argument('--failed-broker', default='b2')
    
    args = parser.parse_args()
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_100_dir = os.path.join(base_dir, "data_100")
    data_25_dir = os.path.join(base_dir, "data_25")
    
    duration = args.duration
    num_pubs = 2
    
    res_100 = run_scenario(
        "100% Equality",
        data_100_dir,
        duration=duration,
        num_pubs=num_pubs,
        simulate_failure=args.simulate_failure,
        failure_after=args.failure_after,
        failed_broker=args.failed_broker
    )

    res_25 = run_scenario(
        "25% Equality",
        data_25_dir,
        duration=duration,
        num_pubs=num_pubs,
        simulate_failure=args.simulate_failure,
        failure_after=args.failure_after,
        failed_broker=args.failed_broker
    )
    
    report = f"""# Evaluation Report

## Setup
- 3 Brokers forming a line/tree Simple Routing overlay: b1 -- b2 -- b3
- 3 Subscribers registering a total of 10000 subscriptions
- {num_pubs} Publishers sending messages for {duration} seconds

## Scenario 1: 100% Equality on 'company'
- Publications Sent: {res_100[3]}
- Delivered Notifications: {res_100[0]}
- Average Latency: {res_100[1]:.2f} ms
- Match Rate: {res_100[2]*100:.6f}%

## Scenario 2: 25% Equality on 'company'
- Publications Sent: {res_25[3]}
- Delivered Notifications: {res_25[0]}
- Average Latency: {res_25[1]:.2f} ms
- Match Rate: {res_25[2]*100:.6f}%
"""
    # Write report to project root
    report_path = os.path.join(os.path.dirname(os.path.dirname(base_dir)), "Evaluation_Report.md")
    with open(report_path, "w") as f:
        f.write(report)
        
    print(f"\nEvaluation Report generated in '{report_path}'")
