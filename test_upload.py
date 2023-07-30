import base64
import json
import os

from fileuploader import get_service, upload_file


def upload_backup_file(backup_file_name: str):
    scope = 'https://www.googleapis.com/auth/drive'
    try:
        json_service_account = base64.b64decode(os.environ.get('GDRIVE_SA_KEY'))
        print(json_service_account)
        service_account_key = json.loads(json_service_account)
        service = get_service(
            api_name='drive',
            api_version='v3',
            scopes=[scope],
            service_account_key='/usr/src/app/key.json')

        upload_file(file_name=backup_file_name, 
                    service=service, 
                    parent_folder_id=os.environ.get('GDRIVE_BACKUP_FOLDER_ID'))
    except Exception as e:
        print(e)


upload_backup_file('/usr/src/app/text.txt')