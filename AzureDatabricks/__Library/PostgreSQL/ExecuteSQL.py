# Databricks notebook source
# Install PyPi library 'Psycopg' to cluster
import psycopg

try:
    __SQL = dbutils.widgets.get("SQL")
except:
    raise Exception("Required parameter(s) missing")

database_host = dbutils.secrets.get(scope = 'KeyVault', key = 'postgre-host')
database_port = dbutils.secrets.get(scope = 'KeyVault', key = 'postgre-port')
database_name = dbutils.secrets.get(scope = 'KeyVault', key = 'postgre-database')
database_user = dbutils.secrets.get(scope = 'KeyVault', key = 'postgre-citus-user')
database_password = dbutils.secrets.get(scope = 'KeyVault', key = 'postgre-citus-password')
database_query = __SQL

try:
    with psycopg.connect(dbname=database_name, user=database_user, password=database_password, host=database_host, port=database_port) as connection:
        with connection.cursor() as cursor:
            cursor.execute(database_query)
            connection.commit()

except (Exception, psycopg.Error) as error:
    raise Exception(error)

finally:
    if connection:
        cursor.close()
        connection.close()
