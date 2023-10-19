import yfinance as yf
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import time

# Conecta ao banco de dados MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="your_username",
    password="your_password",
    database="your_database"
)

mycursor = mydb.cursor()

# Cria uma tabela no banco de dados se ela não existir
mycursor.execute("""
CREATE TABLE IF NOT EXISTS stock_data (
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INT
)
""")

# Define o símbolo da ação e o período de interesse
symbol = "PSSA3.SA"
start_date = "2023-01-01"
yesterday = datetime.now() - timedelta(days=1)
end_date = yesterday.strftime('%Y-%m-%d')  # Define end_date como o dia anterior

# Usa a biblioteca yfinance para baixar os dados da ação
data = yf.download(symbol, start=start_date, end=end_date)

# Insere os dados na tabela MySQL
for index, row in data.iterrows():
    sql = "INSERT INTO stock_data (date, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (index.date(), row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
    mycursor.execute(sql, val)

mydb.commit()

# Cria uma rotina diária para puxar dados novos e inserir no banco de dados
while True:
    # Baixa os dados do dia anterior
    yesterday = datetime.now() - timedelta(days=1)
    data = yf.download(symbol, start=yesterday.strftime('%Y-%m-%d'), end=datetime.now().strftime('%Y-%m-%d'))
    
    # Insere os dados na tabela MySQL
    for index, row in data.iterrows():
        sql = "INSERT INTO stock_data (date, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (index.date(), row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
        mycursor.execute(sql, val)
    
    mydb.commit()
    
    # Aguarda 24 horas antes de puxar novos dados
    time.sleep(86400)
