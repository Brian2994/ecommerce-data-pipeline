from google.cloud import bigquery

client = bigquery.Client(project="projetos-de-teste-445920")
print("Conectado ao projeto:", client.project)
