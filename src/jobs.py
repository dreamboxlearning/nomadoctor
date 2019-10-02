import requests
import json
import os
import logging
import base64
import boto3
from botocore.exceptions import ClientError

class Jobs:
    JOBS_URI = '/v1/jobs'

    def __init__(self, endpoint, token):
        self._endpoint = endpoint
        self._headers = { 'X-Nomad-Token': token } if token is not None else {}

    # Call /v1/jobs to fetch all jobs
    def __list_jobs(self):
        request_url = f'{self._endpoint}{self.JOBS_URI}'
        r = requests.Response()
        jobs = []
        try:
            r = requests.get(request_url, headers=self._headers)
            r.raise_for_status()
            jobs = json.loads(r.text)
        except:
            logging.critical("Failed to list nomad jobs")
            raise
        non_periodic_jobs = []
        # Don't save periodic jobs
        for job in jobs:
            if job.get('ParentID') == "":
                non_periodic_jobs.append(job)
        return non_periodic_jobs

    def __extract_job_names(self, jobs):
        job_names = [ job['Name'] for job in jobs ]
        return job_names

    def __fetch_job_definitions(self, job_names):
        job_definitions = []
        for job_name in job_names:
            request_url = f'{self._endpoint}/v1/job/{job_name}'
            r = requests.Response()
            try:
                r = requests.get(request_url, headers=self._headers)
                r.raise_for_status()
                job_def = base64.b64encode(bytes(r.text, 'utf-8')).decode('utf-8')
                job_definitions.append(job_def)
            except:
                logging.critical(f'Failed to fetch job definition for {job_name}', exc_info=True)
        return job_definitions

    def __output_jobs(self, job_definitions, backup_to_s3):
        
        if backup_to_s3 is None:
            for job_def in job_definitions:
                print(job_def)
        else:
            # create file and write backup contents
            f = open('nomadoctor_backup', 'w')
            for job_def in job_definitions:
                f.write(job_def+"\n")
            f.close()
            # split up the backup_to_s3 ARN into bucket and key
            s3_location = backup_to_s3[5:]
            s3_location = s3_location.split('/', 1)
            s3 = boto3.client('s3')
            s3.upload_file('./nomadoctor_backup', s3_location[0], s3_location[1])
             
    def backup_jobs(self, backup_to_s3):
        # fetch all jobs
        jobs = self.__list_jobs()
        logging.info("Listed jobs.")
        # extract job names
        job_names = self.__extract_job_names(jobs)
        logging.info("Extracted jobs.")
        # fetch job definitions
        job_definitions = self.__fetch_job_definitions(job_names)
        logging.info("Fetched job definitions.")
        # print out jobs
        self.__output_jobs(job_definitions, backup_to_s3)

    def restore_jobs(self, jobs_file):
        if jobs_file.startswith('s3://'):
            jobs_file = jobs_file[5:]
            s3_location = jobs_file.split('/', 1)
            s3 = boto3.client('s3')
            s3.download_file(s3_location[0], s3_location[1], './nomadoctor_backup')
            with open(f'./nomadoctor_backup', 'r') as job_definitions:
                for job in job_definitions:
                    self.__deploy_job(job)
            job_definitions.close()
            os.remove('nomadoctor_backup')
        else:
            with open(f'./{jobs_file}', 'r') as job_definitions:
                for job in job_definitions:
                    self.__deploy_job(job)
            job_definitions.close()
    
    def __deploy_job(self, job):
        r = requests.Response()
        job_def = base64.b64decode(job).decode('utf-8')
        
        job_name = json.loads(job_def)['Name']

        data = json.loads(job_def)
        job_def = { 'Job': data }

        request_url = f'{self._endpoint}{self.JOBS_URI}'
        try:
            r = requests.post(request_url, data=json.dumps(job_def), headers=self._headers)
            r.raise_for_status()
            logging.info(f'Deploying {job_name}: {r.text}')
        except:
            logging.critical(f'Failed to deploy {job_name}', exc_info=True)
    