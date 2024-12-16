import requests
import json
import pyodbc
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Configuração do SQL Server
db_server = os.getenv('db_server')
db = os.getenv('db')
db_username = os.getenv('db_username')
db_password = os.getenv('db_password')
table_name = os.getenv('table_name')

print(f"server: {db_server}")
print(f"database: {db}")
print(f"username: {db_username}")
print(f"password: {db_password}")
print(f"table_name: {table_name}")

# Conexão com o SQL Server
conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server};DATABASE={db};UID={db_username};PWD={db_password}'
)
cursor = conn.cursor()

# Endpoint da API
url = "https://api.casadosdados.com.br/v5/cnpj/pesquisa?tipo_resultado=completo"
pagina = 1
total_registros_baixados = 0
limite = 100  # Defina o limite por página

# Payload ajustado
payload_template = {
    "situacao_cadastral": ["ATIVA"],
    "uf": ["PE"],  # Inclua outras UFs conforme necessário
    "data_abertura": {
        "inicio": "2024-12-15",  # Ajuste o intervalo de datas conforme necessário
        "fim": "2024-12-15"  # Ajuste o intervalo de datas conforme necessário
    },
    "limite": limite,
    "pagina": pagina
}

headers = {
    'Content-Type': 'application/json',
    'api-key': os.getenv('api_key')
}

# Função para converter valores booleanos em 'True' ou 'False'
def boolean_to_text(value):
    return 'True' if value else 'False'

# Função para converter valores numéricos em texto
def number_to_text(value):
    return str(value)

# Função para truncar valores caso ultrapassem o comprimento máximo da coluna
def truncate_value(value, max_length):
    if value and len(value) > max_length:
        return value[:max_length]
    return value

# Loop para consultar a API e inserir no banco
while True:
    print(f"\nConsultando a página {pagina}...")
    payload_template["pagina"] = pagina
    response = requests.post(url, headers=headers, json=payload_template)
    
    print(f"Status da resposta: {response.status_code}")
    if response.status_code != 200:
        print(f"Erro ao consultar API. Código de status: {response.status_code}")
        print("Resposta completa:")
        print(response.text)
        break

    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Erro ao decodificar JSON:")
        print(response.text)
        break

    # Pega o total de registros e os resultados da página
    total_registros_api = data.get("total", 0)
    resultados = data.get("cnpjs")
    
    if not resultados:
        print(f"Nenhum dado encontrado na página {pagina}. Finalizando...")
        break

    print(f"Dados encontrados na página {pagina}: {len(resultados)} registros.")

    # Adiciona o número de registros encontrados na página atual ao total
    total_registros_baixados += len(resultados)

    for registro in resultados:
        # Converter os valores booleanos para 'True' ou 'False'
        email_valido = boolean_to_text(registro["contato_email"][0].get("valido"))
        simples_optante = boolean_to_text(registro["simples"].get("optante"))
        mei_optante = boolean_to_text(registro["mei"].get("optante"))
        bloqueado = boolean_to_text(registro.get("bloqueado"))
        filial_numero = number_to_text(registro.get("filial_numero"))
        capital_social = number_to_text(registro.get("capital_social"))

        ibge_municipio = number_to_text(registro["endereco"].get("ibge").get("codigo_municipio"))
        ibge_uf = number_to_text(registro["endereco"].get("ibge").get("codigo_uf"))
        ibge_latitude = number_to_text(registro["endereco"].get("ibge").get("latitude"))
        ibge_longitude = number_to_text(registro["endereco"].get("ibge").get("longitude"))

        # Preparar os valores para inserção, truncando onde necessário
        telefone_completo = None
        telefone_tipo = None

        if isinstance(registro.get("contato_telefonico"), list) and len(registro["contato_telefonico"]) > 0:
            telefone_completo = truncate_value(registro["contato_telefonico"][0].get("completo"), 50)
            telefone_tipo = truncate_value(registro["contato_telefonico"][0].get("tipo"), 50)

        # Remover os hífens do telefone_completo
        telefone_completo = telefone_completo.replace("-", "") if telefone_completo else None

        values = (
            truncate_value(registro.get("cnpj"), 14),
            truncate_value(registro.get("cnpj_raiz"), 8),
            filial_numero,
            truncate_value(registro.get("razao_social"), 255),
            truncate_value(registro["qualificacao_responsavel"].get("codigo"), 50),
            truncate_value(registro["qualificacao_responsavel"].get("descricao"), 100),
            truncate_value(registro["porte_empresa"].get("codigo"), 50),
            truncate_value(registro["porte_empresa"].get("descricao"), 100),
            truncate_value(registro.get("matriz_filial"), 50),
            truncate_value(registro.get("codigo_natureza_juridica"), 10),
            truncate_value(registro.get("descricao_natureza_juridica"), 100),
            truncate_value(registro.get("nome_fantasia"), 100),
            truncate_value(registro["situacao_cadastral"].get("situacao_atual"), 50),
            truncate_value(registro["situacao_cadastral"].get("motivo"), 255),
            truncate_value(registro["situacao_cadastral"].get("data"), 50),
            truncate_value(registro["endereco"].get("cep"), 10),
            truncate_value(registro["endereco"].get("tipo_logradouro"), 50),
            truncate_value(registro["endereco"].get("numero"), 10),
            truncate_value(registro["endereco"].get("complemento"), 100),
            truncate_value(registro["endereco"].get("bairro"), 100),
            truncate_value(registro["endereco"].get("uf"), 2),
            truncate_value(registro["endereco"].get("municipio"), 100),
            truncate_value(registro.get("data_abertura"), 10),
            truncate_value(registro.get("ente_federativo"), 50),
            capital_social,
            truncate_value(registro["situacao_especial"].get("descricao"), 100),
            truncate_value(registro["situacao_especial"].get("data"), 50),
            truncate_value(registro["atividade_principal"].get("codigo"), 50),
            truncate_value(registro["atividade_principal"].get("descricao"), 100),
            truncate_value(registro.get("data_consulta"), 50),
            bloqueado,
            mei_optante,
            truncate_value(registro["mei"].get("data_opcao_mei"), 10),
            truncate_value(registro["mei"].get("data_exclusao_mei"), 10),
            truncate_value(registro["mei"].get("cpf"), 14),
            simples_optante,
            truncate_value(registro["simples"].get("data_opcao_simples"), 10),
            truncate_value(registro["simples"].get("data_exclusao_simples"), 10),
            telefone_completo,
            telefone_tipo,
            truncate_value(registro["contato_email"][0].get("email"), 255),
            email_valido,
            truncate_value(registro["contato_email"][0].get("dominio"), 100),
            ibge_municipio,
            ibge_uf,
            ibge_latitude,
            ibge_longitude,
        )

        # Tentar inserir os valores no SQL Server com tratamento de erro
        try:
            cursor.execute(f"""
            INSERT INTO {table_name} (
                cnpj,
                cnpj_raiz,
                filial_numero,
                razao_social,
                qualificacao_responsavel_codigo,
                qualificacao_responsavel_descricao,
                porte_empresa_codigo,
                porte_empresa_descricao,
                matriz_filial,
                codigo_natureza_juridica,            
                descricao_natureza_juridica,
                nome_fantasia,
                situacao_cadastral_atual,
                situacao_cadastral_motivo,
                situacao_cadastral_data,
                cep,
                tipo_logradouro,
                numero,
                complemento,
                bairro,
                uf,            
                municipio,
                data_abertura,
                ente_federativo,
                capital_social,
                situacao_especial_descricao,
                situacao_especial_data,
                atividade_principal_codigo,
                atividade_principal_descricao,            
                data_consulta,
                bloqueado,            
                mei_optante,
                data_opcao_mei,
                data_exclusao_mei,
                mei_cpf,
                simples_optante,
                data_opcao_simples,
                data_exclusao_simples,
                telefone_completo,
                telefone_tipo,
                email,            
                email_valido,
                email_dominio,
                ibge_municipio,
                ibge_uf,
                ibge_latitude,
                ibge_longitude
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, values)
            conn.commit()
        except pyodbc.DataError as e:
            print(f"\nErro ao tentar inserir os dados na tabela '{table_name}'")
            print(f"Dados: {values}")
            print(f"Detalhes do erro: {e}")

    # Se o número total de registros baixados for igual ao total informado pela API, interrompa
    if total_registros_baixados >= total_registros_api:
        print(f"Todos os {total_registros_api} registros foram baixados.")
        break

    pagina += 1

print("Processamento concluído.")
conn.close()
