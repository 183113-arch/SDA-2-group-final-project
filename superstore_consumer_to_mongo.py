from kafka import KafkaConsumer, errors
from pymongo import MongoClient
import json
import sys

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'stock-topic'
GROUP_ID = 'stock-consumer-group'

# MongoDB configuration
MONGO_URI = 'mongodb+srv://mongoadmin:mongoadmin@cluster0.ceikf.mongodb.net/' # Atlas Connection URL
DB_NAME = 'superstore_data_consumer'
COLLECTION_NAME = 'superstore_collection'
ALERTS_COLLECTION = 'alerts'

# Step 1: Connect to MongoDB
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    alerts_collection = db[ALERTS_COLLECTION]
    print("✅ Connected to MongoDB at", MONGO_URI)
except Exception as e:
    print("❌ Error connecting to MongoDB:", str(e))
    sys.exit(1)

# Step 2: Create Kafka consumer
try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    print("✅ Connected to Kafka broker at", KAFKA_BROKER)
except errors.NoBrokersAvailable:
    print("❌ Kafka broker not available at", KAFKA_BROKER)
    sys.exit(1)
except Exception as e:
    print("❌ Error connecting to Kafka:", str(e))
    sys.exit(1)

# Step 3: Consume and insert into MongoDB
print(f"📡 Listening to topic: {TOPIC}")
try:
    for message in consumer:
        stock_data = message.value
        collection.insert_one(stock_data)
        print(f"✅ Inserted into MongoDB: {stock_data}")

        # Convert safely
        try:
            profit = float(stock_data.get('Profit', 0))
        except (ValueError, TypeError):
            profit = 0.0

        try:
            sales = float(stock_data.get('Sales', 0))
        except (ValueError, TypeError):
            sales = 0.0

        try:
            quantity = float(stock_data.get('Quantity', 0))
        except (ValueError, TypeError):
            quantity = 0.0

        alert_message = None
        if sales > 0 and (profit / sales) < 0.05:
            alert_message = "low margin"
        if profit < 0:
            alert_message = "negative profit"
        if sales > 20000 or quantity > 50:
            alert_message = "big order"

        if alert_message:
            order_id = stock_data.get('Order ID')
            customer_id = stock_data.get('Customer ID')
            customer_name = stock_data.get('Customer Name')
            product_id = stock_data.get('Product ID')

            alert_msg = (
                f"ALERT 🚨: Order {order_id} (product_id={product_id}) "
                f"by {customer_name} has {alert_message}."
            )
            print(alert_msg)

            alert_doc = {
                "order_id": order_id,
                "customer_id": customer_id,
                "customer_name": customer_name,
                "product_id": product_id,
                "message": alert_message
            }
            alerts_collection.insert_one(alert_doc)
            print("✅ Alert inserted into alerts collection.")

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped manually.")
except Exception as e:
    print("❌ Error while consuming/inserting:", str(e))
finally:
    consumer.close()
    mongo_client.close()
