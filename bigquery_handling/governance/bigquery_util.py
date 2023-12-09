from google.cloud import bigquery
from time import time
from base_logger import logger


def run_query(query, log="", attach_query=True):
    
    client = bigquery.Client()
    query_config = {
    "labels": {'country': 'corp', 'domain': 'data_governance_automation',
               'query_name': 'quality_rules', 'business': 'corp',
               'portfolio-id': 'automation'}
    
    }
    logger.info(log)
    job_config = bigquery.QueryJobConfig(
    **query_config
    )
    t1 = time()
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    results = results.to_dataframe()

    if attach_query:

        results['query'] = query
    
    byte_processed = query_job.total_bytes_processed

    if byte_processed is not None:

        print(f"This query will process {round(byte_processed / (1024 ** 2), 3)} MB.")
    
    logger.info("Query Execution Ends")
    t2 = time()
    logger.info(f"Total Time Taken to execute query is {round(t2 - t1, 3)} sec")
    return results