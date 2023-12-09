import logging, os
log_file = os.path.dirname(os.path.abspath(__file__)) + "/profiling.log"
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(log_file, mode="w")
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)