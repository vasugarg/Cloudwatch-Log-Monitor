import logging
import time
import boto3
import os
import smtplib
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from html_generate import html_generate
from sendMail import sendMail
from queries_data import queries

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('logs')

def format_data(data):
    all_logs = []

    for lst in data['results']:
        temp = {d['field'] : d['value'] for d in lst}
        all_logs.append(temp)
        
    return all_logs

def get_log_data(logGroup, logGroupNames):
    
    query = queries[logGroup]
    logger.info("Query : {0}".format(query))
    start_query_response = client.start_query(
            logGroupNames= logGroupNames,
            startTime=int((datetime.today() - timedelta(minutes=15)).timestamp())*1000,
            endTime=int(datetime.now().timestamp())*1000,
            queryString=query,
            )

    query_id = start_query_response['queryId']
    response = None

    while response == None or response['status'] == 'Running':
            logger.info('Waiting for query to complete ...')
            time.sleep(1)
            response = client.get_query_results(
            queryId=query_id
            )
    logger.info("Response: {}".format(response))
    return response

def lambda_handler(event, context):

    logger.info("Event: {0}".format(event)) 
    
    for group in event['logGroupName']:
        
        logGroupNames = os.environ[group].split(' ')
        logger.info("Log Group Category: {0}". format(group))
        data = get_log_data(group, logGroupNames)

        if (data['statistics']['recordsMatched'] == 0 or len(data['results']) ==0):
            logger.info("No errors encountered")
        
        elif (data['statistics']['recordsMatched'] > 0 and len(data['results']) !=0):
            logger.info("Records matched")
            formed_data = format_data(data)
            body = html_generate(group,formed_data)
            logger.info("Entering mail function")
            sendMail(body, group)
            logger.info("Email Sent")

        else:
            logger.warn("Oops, something went wrong. Data: {0}". format(data))
