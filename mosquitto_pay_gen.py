import json
import time
import random
import paho.mqtt.client as mqtt

mqtt_broker = "127.0.0.1"  
mqtt_port = 1883
topic_prefix = "vehicle/data"

client = mqtt.Client()

def on_message(client, userdata, message):
    print(f"📥 Received: {message.topic} -> {message.payload.decode()}")

client.on_message = on_message  
client.connect(mqtt_broker, mqtt_port, 60)
client.subscribe(f"{topic_prefix}/#")  

def generate_random_data():
    return {
        "id": random.randint(1000, 9999),
        "speed": random.randint(0, 120),  
        "latitude": round(random.uniform(-90, 90), 6),
        "longitude": round(random.uniform(-180, 180), 6),
    }

def process_data(data):
    if data["speed"] == 0:
        data["status"] = "stopped"
    elif data["speed"] > 80:
        data["status"] = "over-speeding"
    else:
        data["status"] = "normal"
    return data

def publish_loop():
    while True:
        data = generate_random_data()
        processed_data = process_data(data)

        topic = f"{topic_prefix}/{processed_data['id']}"
        client.publish(topic, json.dumps(processed_data))
        print(f"📤 Sent: {processed_data} to topic {topic}")

        time.sleep(3)

client.loop_start()
publish_loop()  