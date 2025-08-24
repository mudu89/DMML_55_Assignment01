import logging
import os

def get_logger(name="pipeline", log_file="logs/pipeline.log"):
    """
    Returns a logger instance that writes logs to logs/pipeline.log.
    Reusable across ingestion, validation, transformation, modeling.
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if function is called multiple times
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger