import pyodbc

server = "adms3.costafaria.ind.br"
database = "erp"
username = "acessoext_mineirinho"
password = "eBtyfGpEMNtRjJt4mq84"
driver = "ODBC Driver 17 for SQL Server"

try:
    print("Testando conexão com o banco de dados...")
    conn_str = f"DRIVER={{{driver}}};SERVER={server},1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes;"
    conn = pyodbc.connect(conn_str, timeout=5)
    print("✅ Conexão bem-sucedida!")
    conn.close()
except Exception as e:
    print("❌ Falha na conexão:")
    print(e)
