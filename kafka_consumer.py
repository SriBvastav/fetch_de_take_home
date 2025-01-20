import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta
import logging
import ipaddress
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaStreamProcessor:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'user-login',
            bootstrap_servers=['localhost:29092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='user_login_processor',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        self.processed_topic = 'processed-user-login'
        self.device_stats = {'iOS': 0, 'android': 0}
        self.version_stats = {}
        self.message_count = 0
        self.user_sessions = {}
        self.location_stats = defaultdict(int)
        self.hourly_activity = defaultdict(int)

    def get_ip_category(self, ip):
        """Categorize IP address into internal/external and rough geolocation."""
        try:
            ip_obj = ipaddress.ip_address(ip)
            if ip_obj.is_private:
                return "internal"
            # Simple region categorization based on first octet
            first_octet = int(ip.split('.')[0])
            if first_octet < 100:
                return "NA"
            elif first_octet < 150:
                return "EU"
            elif first_octet < 200:
                return "ASIA"
            else:
                return "OTHER"
        except:
            return "unknown"

    def process_message(self, message):
        """Process individual messages with error handling."""
        try:
            # Convert Unix timestamp to datetime
            timestamp = datetime.fromtimestamp(int(message.get('timestamp', 0)))
            
            # Get user session data
            user_id = message.get('user_id')
            current_time = datetime.now()
            
            # Update session tracking
            if user_id in self.user_sessions:
                last_seen = self.user_sessions[user_id]['last_seen']
                if (current_time - last_seen) > timedelta(minutes=30):
                    self.user_sessions[user_id]['session_count'] += 1
            else:
                self.user_sessions[user_id] = {
                    'last_seen': current_time,
                    'session_count': 1
                }
            
            self.user_sessions[user_id]['last_seen'] = current_time
            
            # Get IP category
            ip_category = self.get_ip_category(message.get('ip', ''))
            self.location_stats[ip_category] += 1
            
            # Update hourly activity
            self.hourly_activity[timestamp.hour] += 1
            
            # Add processed fields
            processed_message = {
                'user_id': user_id,
                'app_version': message.get('app_version'),
                'device_type': message.get('device_type'),
                'ip': message.get('ip'),
                'ip_category': ip_category,
                'locale': message.get('locale'),
                'device_id': message.get('device_id'),
                'original_timestamp': message.get('timestamp'),
                'processed_timestamp': current_time.isoformat(),
                'hour_of_day': timestamp.hour,
                'day_of_week': timestamp.strftime('%A'),
                'session_count': self.user_sessions[user_id]['session_count']
            }
            
            # Update statistics
            device_type = message.get('device_type')
            app_version = message.get('app_version')
            
            if device_type in self.device_stats:
                self.device_stats[device_type] += 1
            
            if app_version:
                self.version_stats[app_version] = self.version_stats.get(app_version, 0) + 1
            
            return processed_message
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            logger.error(f"Problem message: {message}")
            return None

    def send_to_topic(self, processed_message):
        """Send processed message to new topic."""
        try:
            self.producer.send(self.processed_topic, value=processed_message)
        except Exception as e:
            logger.error(f"Error sending message to topic: {e}")

    def log_statistics(self):
        """Log current statistics."""
        logger.info("Current Statistics:")
        logger.info(f"Device Types: {self.device_stats}")
        logger.info(f"App Versions: {self.version_stats}")
        logger.info(f"Geographic Distribution: {dict(self.location_stats)}")
        logger.info(f"Total Messages Processed: {self.message_count}")
        logger.info(f"Unique Users: {len(self.user_sessions)}")
        logger.info(f"Hourly Activity: {dict(self.hourly_activity)}")

    def run(self):
        """Main processing loop."""
        logger.info("Starting Kafka Stream Processor...")
        
        try:
            for message in self.consumer:
                try:
                    raw_message = message.value
                    processed_message = self.process_message(raw_message)
                    
                    if processed_message:
                        self.send_to_topic(processed_message)
                        self.message_count += 1
                        
                        # Log statistics every 100 messages
                        if self.message_count % 100 == 0:
                            self.log_statistics()
                            
                except Exception as e:
                    logger.error(f"Error in message processing loop: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Critical error in consumer: {e}")
        finally:
            logger.info("Closing consumer and producer...")
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    processor = KafkaStreamProcessor()
    processor.run()