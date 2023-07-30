#!/usr/bin/python3
import base64
import datetime
import json
import os
import os.path
import tarfile
import time

import schedule
from bson.json_util import dumps
from pymongo import MongoClient

from fileuploader import get_service, upload_file

working_dir = '/usr/src/app'


def create_folder_backup(dbname):
    dt = datetime.datetime.now()
    directory = '%s/bk_%s' % (working_dir, dbname)
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def make_tarfile(output_filename, source_dir):
    tar = tarfile.open(output_filename, "w:gz")
    for filename in source_dir:
        tar.add(filename)
    tar.close()


def run_backup(mongoUri):
    client = MongoClient(mongoUri)
    databases = client.list_database_names()
    print('backing up databases: %s' % databases)
    databases.remove('admin')
    databases.remove('config')
    folders_to_compress = []
    for database_name in databases:
        db = client[database_name]
        collections = db.list_collection_names()
        files_to_compress = []
        directory = create_folder_backup(database_name)
        folders_to_compress.append(directory)
        for collection in collections:
            db_collection = db[collection]
            cursor = db_collection.find({})
            filename = ('%s/%s.json' % (directory, collection))
            files_to_compress.append(filename)
            with open(filename, 'w') as file:
                file.write('[')
                for document in cursor:
                    file.write(dumps(document))
                    file.write(',')
                file.write(']')
    print('folders_to_compress: %s' % folders_to_compress)
    dt = datetime.datetime.now()
    tar_file_path = ('%s/bk_complete_%s-%s-%s__%s_%s.tar.gz' %
                     (working_dir, dt.month, dt.day, dt.year, dt.hour, dt.minute))
    tar_file_name = ('bk_complete_%s-%s-%s__%s_%s.tar.gz' %
                     (dt.month, dt.day, dt.year, dt.hour, dt.minute))
    make_tarfile(tar_file_path, folders_to_compress)
    print('tar_file_name: %s' % tar_file_name)
    print('tar_file_path: %s' % tar_file_path)
    return tar_file_name, tar_file_path


def backup_databases():
    USERNAME = os.environ.get('MONGO_INITDB_ROOT_USERNAME')
    PASSWORD = os.environ.get('MONGO_INITDB_ROOT_PASSWORD')
    HOST = os.environ.get('MONGO_INITDB_DATABASE', 'libredb')
    DATABASE = os.environ.get('MONGO_ADMIN_DATABASE', 'admindb')
    PORT = os.environ.get('DB_PORT', '27017')

    print(USERNAME, PASSWORD, HOST, DATABASE, PORT)

    mongoUri = ('mongodb://%s:%s@%s:%s/%s?authSource=admin' %
                (USERNAME, PASSWORD, HOST, PORT, DATABASE))
    print('mongoUri: %s' % mongoUri)
    tar_file_name, tar_file_path = run_backup(mongoUri=mongoUri)
    upload_backup_file(tar_file_name)


def upload_backup_file(backup_file_name: str):
    scope = 'https://www.googleapis.com/auth/drive'
    try:
        service_account_key = json.loads(
            base64.b64decode(os.environ.get('GDRIVE_SA_KEY')))
        print('service_account_key: %s' % service_account_key)
        service = get_service(
            api_name='drive',
            api_version='v3',
            scopes=[scope],
            service_account_key=service_account_key)

        upload_file(folder=working_dir,
                    file_name=backup_file_name, service=service)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    schedule.every(2).minutes.do(backup_databases)
    service_account_key = json.loads(
            base64.b64decode(os.environ.get('GDRIVE_SA_KEY')))
    print('service_account_key: %s' % service_account_key)
    # schedule.every().day.at("00:00").do(backup_databases)

    while True:
        schedule.run_pending()
        time.sleep(1)
