import logging
import os
import sys
import time as totalTime
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time, timedelta

import mysql.connector
import pandas as pd
import pytz


db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'pool_name': 'custom_connection_pool',
    'pool_size': 10
}

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True
)
