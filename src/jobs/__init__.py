#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import importlib
import os
import subprocess
import sys
import time
import traceback

from shared.Log4j import Log4j
from shared.SparkUtil import SparkUtil

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name',
                        help="The name of the job module you want to run. (ex: poc will run job on jobs.poc package)")
    parser.add_argument('--job-args', nargs='*',
                        help="Extra arguments to send to the PySpark job (--job-args template=manual-email1 foo=bar")

    args = parser.parse_args()

    print("Called with arguments: %s" % args)

    environment = {
        'PYSPARK_JOB_ARGS': ' '.join(args.job_args) if args.job_args else ''
    }

    job_args = dict()
    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        print('job_args_tuples: %s' % job_args_tuples)
        job_args = {a[0]: a[1] for a in job_args_tuples}

    print('\nRunning job %s ...\n environment is %s\n' % (args.job_name, environment))

    os.environ.update(environment)

    spark = SparkUtil.spark_session(args.job_name)

    sc = spark.sparkContext
    cores = sc.getConf().get("spark.executor.cores")
    instances = sc.getConf().get("spark.executor.instances")
    log4j = Log4j(spark)

    log4j.info("cores:" + str(cores) + "* instances:" + str(instances))

    log4j.info("=========Loading jobs " + args.job_name + "  Profile:" + job_args['profile'] + "==========")

    job_module = importlib.import_module('jobs.%s' % args.job_name)
    try:
        start = time.time()
        job_module.run(spark, job_args)
        end = time.time()
        log4j.info("\nExecution of job " + args.job_name + " took " + str(end - start) + " seconds")
    except Exception as e:
        log4j.error(
            "\nExecution of job " + args.job_name + " fail! , because:" + str(e) + "\n" + traceback.format_exc())

    if job_args['profile'] == 'gcp':
        log4j.info("Try to delete cluster " + job_args['cluster_name'] + "......")
        result = subprocess.call(['gcloud',
                                  'dataproc',
                                  'clusters',
                                  'delete',
                                  job_args['cluster_name'],
                                  '--quiet',
                                  '--async'])
        log4j.info("Sent command result:" + str(result))
