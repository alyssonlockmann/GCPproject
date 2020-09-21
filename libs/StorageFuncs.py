from google.cloud import storage

class StorageFuncs():
    # Atributos padrões:
    __CREDENTIALS_FILE = 'credentials/cloud-bigquery.json'
    __BUCKET_NAME = 'cloud_project-1'

    @classmethod
    def uploadObjectIntoBucket(self, input_file, storage_file_name):
        # Criando cliente do storage:
        storage_client = storage.Client.from_service_account_json(self.__CREDENTIALS_FILE)

        # Definindo o bucket e o arquivo de destino:
        bucket = storage_client.bucket(self.__BUCKET_NAME)
        blob = bucket.blob(storage_file_name)

        # Fazendo upload do arquivo:
        try:
            blob.upload_from_filename(input_file)
            print(' -> Arquivo carrregado com sucesso!') 
            print(' -> Origem: {} \n -> Destino {}'.format(input_file, storage_file_name))
        except Exception as inst:
            print('*** Erro no upload do arquivo: {}'.format(input_file))
            raise inst

        # Listando conteudo do bucket:
        print(' -> Bucket: {}'.format(self.__BUCKET_NAME))
        print('  -> Objetos do bucket:')
        bucket_blobs = storage_client.list_blobs(self.__BUCKET_NAME)
        for obj in bucket_blobs:
                print('     * {}'.format(obj.name))
