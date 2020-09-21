# GCPproject
	Projeto com o objetico de consumir arquivos JSON e carregar os dados apra o Google BgQuery 

# Instalar as bibliotecas do cloud-storage:
	pip install --upgrade google-cloud-storage
	pip install --upgrade google-cloud-bigquery
	pip install --upgrade pandas

# Criar conta de serviço e baixar o JSON com as credenciais:
	1. https://cloud.google.com/docs/authentication/production?hl=pt-br#create_service_account
	2. Criar a conta com 2 papeis, 'Administrador do BigQuery' e 'Administrador do Storage'
	3. Caso já tenha o arquivo JSON de autenticação, pode apenas adiconar os papeis via UI: 		 
		- https://cloud.google.com/iam/docs/granting-roles-to-service-accounts?hl=pt-br

# Instruções
Utilize o comando python "InsertData.py" para rodar o script do pipeline.