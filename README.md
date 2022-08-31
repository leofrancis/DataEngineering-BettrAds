<br> 
# Processo de Execução

1) É necessário instalar os requeriments (requirements.txt) com: python3 -m pip install -r requirements.txt 
2) Depois de setar as informações em CONFIGS de URL_API e os dados do DB; criar uma conta [gratis] elephantsql, criar instância, acessar Details e pegar as informações do banco e informar em CONFIGS.
3) Utilizar flask run para abrir o Serviço API e acessar http://127.0.0.1:5000/
4) Clicar em Populate ou http://127.0.0.1:5000/populate para buscar os dados e que irá salvá-los na pasta como data.parquet
5) Clicando em Report ou http://127.0.0.1:5000/report para buscar os dados já salvos e exportá-los para pasta report utilizando apanas o pandas para ajuste de dados
6) Acessando o endpoint http://127.0.0.1:5000/report/sql realiza envio dos dados para postgres definido em CONFIGS e no final exportá-los para report através da consulta SQL