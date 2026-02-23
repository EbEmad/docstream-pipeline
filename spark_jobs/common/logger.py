import logging
import sys

def setup_logging(app_name: str, log_level: str = "INFO") -> logging.Logger:
    """Setup logging for a Spark job"""

    root_logger=logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    handler=logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    
    return logging.getLogger(app_name)