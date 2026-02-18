import logging
import time
import random
import uuid
import os
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
HISTORICAL_DOCUMENTS = 5000  # 5K historical records
DOCUMENTS_PER_MINUTE = 3      # Real-time rate

class Producer:
    """Generates documents and updates their status"""

    STATUS_FLOW=['draft', 'sent', 'viewed', 'signed', 'completed']
    TEMPLATES=[
        {'name': 'Sales Contract', 'category': 'Sales'},
        {'name': 'NDA Agreement', 'category': 'Legal'},
        {'name': 'Employment Contract', 'category': 'HR'},
        {'name': 'Service Agreement', 'category': 'Sales'},
    ]

    def __init__(self):
        self.db_config={
            "host": os.getenv('POSTGRES_HOST', 'postgres'),
            "port": os.getenv('POSTGRES_PORT', 5432),
            "database": os.getenv('POSTGRES_DB', 'docstream'),
            "user": os.getenv('POSTGRES_USER', 'docuser'),
            "password":os.getenv('POSTGRES_PASSWORD', 'root')
        }
        self.rate=DOCUMENTS_PER_MINUTE
        self.conn=None
        self._connect()
    
    def _connect(self):
        """Connect to database"""
        logger.info("Connecting to database...")
        max_retries=30
        for i in range(max_retries):
            try:
                self.conn=psycopg2.connect(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password']
                )
                self.conn.autocommit=False
                logger.info("Connected to database")
                return
            except Exception as e:
                logger.warning(f"Database connection failed (attempt {i+1}/{max_retries}): {e}")
                time.sleep(5)
        raise Exception("Could not connect to database after multiple attempts")

    def load_historical_data(self):
        """Load historical documents (past 6 months)"""
        logger.info("=" * 70)
        logger.info("HISTORICAL DATA LOAD")
        logger.info("=" * 70)

        # Check if historical data already exists
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM documents")
            existing_count=cur.fetchone()[0]

            if existing_count>=HISTORICAL_DOCUMENTS:
                logger.info(f" Historical data already loaded ({existing_count} documents)")
                logger.info("=" * 70)
                return

        logger.info(f"Loading {HISTORICAL_DOCUMENTS:,} historical documents...")
        logger.info("This simulates 6 months of past activity")
        logger.info("")

        batch_size=-1000
        total_batches = HISTORICAL_DOCUMENTS // batch_size
        start_time=time.time()

        # Generate documents spread over 6 months
        end_date=datetime.now()
        start_date=end_date - timedelta(days=180)

        for batch_num in range(total_batches):
            documents=[]
            for i in range(batch_size):
            # Distribute documents over 6 months
                days_ago=random.randint(0,180)
                created_at=end_date-timedelta(days=days_ago)




def run(self):
    """Run producer with historical load first"""
    logger.info("")
    logger.info("="*70)
    logger.info("DOCSTREAM PRODUCER")
    logger.info("=" * 70)
    logger.info("")

    #  Load historical data
    self.load_historical_data()




if __name__ == "__main__":
    producer=Producer()
    producer.run()