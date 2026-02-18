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
            existing_count = cur.fetchone()[0]
            
            if existing_count >= HISTORICAL_DOCUMENTS:
                logger.info(f"✓ Historical data already loaded ({existing_count} documents)")
                logger.info("=" * 70)
                return
        
        logger.info(f"Loading {HISTORICAL_DOCUMENTS:,} historical documents...")
        logger.info("This simulates 6 months of past activity")
        logger.info("")
        
        batch_size = 1000
        total_batches = HISTORICAL_DOCUMENTS // batch_size
        start_time = time.time()
        
        # Generate documents spread over 6 months
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)
        
        for batch_num in range(total_batches):
            documents = []
            
            for i in range(batch_size):
                # Distribute documents over 6 months
                days_ago = random.randint(0, 180)
                created_at = end_date - timedelta(days=days_ago)
                
                template = random.choice(self.TEMPLATES)
                status, timestamps = self._generate_historical_status(created_at)
                
                doc = (
                    str(uuid.uuid4()),
                    template['name'],
                    status,
                    template['category'],
                    created_at,
                    *timestamps
                )
                documents.append(doc)
            
            # Insert batch
            with self.conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO documents 
                    (document_id, title, status, template_category, created_at,
                     sent_at, viewed_at, signed_at, completed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, documents, page_size=batch_size)
                self.conn.commit()
            
            # Progress update
            if (batch_num + 1) % 10 == 0:
                elapsed = time.time() - start_time
                progress = (batch_num + 1) / total_batches * 100
                rate = (batch_num + 1) * batch_size / elapsed
                logger.info(
                    f"Progress: {progress:5.1f}% | "
                    f"Loaded: {(batch_num + 1) * batch_size:,} | "
                    f"Rate: {rate:,.0f} docs/sec"
                )
        
        elapsed = time.time() - start_time
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"✓ Historical load complete!")
        logger.info(f"  Documents: {HISTORICAL_DOCUMENTS:,}")
        logger.info(f"  Duration: {elapsed:.1f} seconds")
        logger.info(f"  Rate: {HISTORICAL_DOCUMENTS/elapsed:,.0f} docs/sec")
        logger.info("=" * 70)
        logger.info("")
        
        self._show_statistics()
        
    def _generate_historical_status(self, created_at):
        """Generate realistic historical document with complete lifecycle"""
        now=datetime.now()

        # 80% of old documents should be completed
        age_days=(now-created_at).days

        completion_probability=min(0.8,age_days/30) # 80% after 30 days
        if random.random() < completion_probability:

            # Completed document
            sent_at=created_at+timedelta(hours=random.randint(1, 24))
            viewed_at = sent_at + timedelta(minutes=random.randint(10, 120))
            signed_at = viewed_at + timedelta(hours=random.randint(1, 48))
            completed_at = signed_at + timedelta(hours=random.randint(1, 24))
            return 'completed',(sent_at,viewed_at,signed_at,completed_at)
        else:

            status_choice=random.choices(['sent', 'viewed', 'signed'])
            if status_choice=="sent":
                sent_at = created_at + timedelta(hours=random.randint(1, 24))
                return "sent",(sent_at,None,None,None)
            elif status_choice=="viewed":
                sent_at=created_at+timedelta(hours=random.randint(1,24))
                viewed_at=sent_at+timedelta(minutes=random.randint(10, 120))
                return "viewed",(sent_at,viewed_at,None,None)
            else:  # signed
                sent_at=created_at+timedelta(hours=random.randint(1, 24))
                viewed_at=sent_at+timedelta(minutes=random.randint(10,120))
                signed_at=viewed_at+timedelta(hours=random.randint(1,48))
                return "signed",(sent_at,viewed_at,signed_at,None)
    def _show_statistics(self):
        """Show database statistics"""
        with self.conn.cursor() as cur:
            # Status distribution
            cur.execute("""
                SELECT status, COUNT(*) as count
                FROM documents
                GROUP BY status
                ORDER BY count DESC             
            """)
            logger.info("Status Distribution:")
            for status,count in cur.fetchall():
                percentage=count/HISTORICAL_DOCUMENTS*100
                logger.info(f"{status:12} {count:6,} ({percentage:5.1f}%)")
            
            # Category distribution
            cur.execute(
                """
                SELECT template_category, COUNT(*) as count
                FROM documents
                GROUP BY template_category
                ORDER BY count DESC
                """
            )

            logger.info("")
            logger.info("Category Distribution:")
            for category,count in cur.fetchall():
                percentage=count/HISTORICAL_DOCUMENTS*100
                logger.info(f"  {category:12} {count:6,} ({percentage:5.1f}%)")
            logger.info("")
    
    def create_documents(self,count):
        """Create new documents (real-time)"""
        documents=[]
        now=datetime.now()

        for _ in range(count):
            template=random.choice(self.TEMPLATES)

            doc=(
                str(uuid.uuid4()),
                template['name'],
                'draft',
                template['category'],
                now,
                None,None,None,None
            )

            documents.append(doc)
        with self.conn.cursor() as cur:
            execute_batch(cur,"""
                INSERT INTO documents 
                (document_id, title, status, template_category, created_at,
                 sent_at, viewed_at, signed_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, documents)
            self.conn.commit()

        logger.info(f"Created {count} new documents")
        return count







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