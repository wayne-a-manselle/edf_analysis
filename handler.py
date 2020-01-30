"""
AWS Lambda EDF Signal Analysis Report
An AWS Lambda Example
Author: Wayne Manselle
Version: 1.0

Copyright Glue Architectures, 2019 All Rights Reserved 
"""

import boto3
import uuid
from urllib.parse import unquote_plus
#EDF Reader Library
#https://pyedflib.readthedocs.io/en/latest/
import pyedflib
import json
import numpy as np
from scipy.signal import welch

#The s3 client is what you'll be using to retrieve data from s3
s3_client = boto3.client('s3')

def edf_analysis(reqID, edf_path, report_path):
  """
  Function to determine the strongest frequency component of each
  signal in the passed EDF file and assemble the results into the
  report a user may retrieve.

  :param: reqID - a unique identifier for the request
  :param: edf_path - the S3 path the EDF is located at.
  :param: report_path - the S3 path the report will be written to.
  """
  finalReport = {}
  with pyedflib.EdfReader(edf_path) as edf_data:
    numSigs = edf_data.signals_in_file()
    sigNames = edf_data.getSignalLabels()
    # For the purposes of this tutorial, we assume that all signals
    # were sampled at the same frequency
    sampFreq = edf_data.getSampleFrequencies()
    for sig in np.arange(numSigs):
      curSignal = edf_data.readSignal(sig)
      f, Pxx = welch(curSignal, sampFreq, nperseg=1024)
      """
      Sort the list of frequencies from the signal by
      the inverse of their power spectrum, grab first
      item.
      """
      strongest = f[Pxx.argsort()[::-1][:1]]
      #Add Signal Label: Strongest Frequency Component to the report
      finalReport[sigNames[sig]] = strongest
    #Write Report to Report_Path
    reportName = '-'.join(reqID,'report.json')
    with open(reportName, 'w') as fp:
      json.dump(finalReport, fp)


def handler(event, context):
  """
  Standard S3 Handler Method for AWS Lambda
  with some modifications for the Glue Architectures
  AWS Lambda Tutorial

  Source: https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-deployment-pkg.html#with-s3-example-deployment-pkg-python
  """
  for record in event['Records']:
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
    edf_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
    report_path = '/tmp/{}-report'.format(key)
    s3_client.download_file(bucket, key, edf_path)
    edf_analysis(key, edf_path, report_path)
    s3_client.upload_file(report_path, '{}_final_report'.format(bucket), key)

