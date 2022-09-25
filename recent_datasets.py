import pandas as pd
import logging
import pymysql
import sshtunnel
from sshtunnel import SSHTunnelForwarder
from credentials import *

    
# opens ssh tunnel to connect
def open_ssh_tunnel(verbose = False):
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
    global tunnel
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username = ssh_username,
        ssh_password = ssh_password,
        remote_bind_address = ("127.0.0.1", 3306)
    )
    
    tunnel.start()

def mysql_connect():
    """Connect to a MySQL server using the SSH tunnel connection
    
    :return connection: Global MySQL database connection
    """
    
    global connection
    
    connection = pymysql.connect(
        host = localhost,
        user = database_username,
        passwd = database_password,
        db = database_name,
        port = tunnel.local_bind_port
    )
    
def run_query(sql):   
    return pd.read_sql_query(sql, connection)

def mysql_disconnect():
    connection.close()
    
def close_ssh_tunnel():
    tunnel.close


open_ssh_tunnel()
mysql_connect()
df = run_query("SELECT * FROM dataset_eml_records")

mysql_disconnect()
close_ssh_tunnel()

most_recent = df.groupby(by="dataset_id")[['revision', 'doi', 'date_published']].max()

pd.set_option('display.max_colwidth', 1000)
pd.set_option('display.max_rows', 1000)
print(most_recent['doi'])
