# DOCUMENTACION PARA EL MODULO sqlbeam
Este modulo se importa desde los **archivos base de codigo** desde donde se realizan las operaciones con apache beam.

* **__init__.py**
    * archivo que identifica un directorio como módulo importable
* **exceptions.py**
    * archivo que contiene el manejo de excepciones
* **sql.py**
    * archivo que contiene la base del modulo, maneja el proceso en general
* **wrapper.py**
    * archivo que contiene la implementacion para cada sistema (AS400,ORACLE,MSSQL, etc), maneja el proceso en particular

# CONFIGURACION DEL ENTORNO PARA BASES DE DATOS AS400, ORACLE Y MSSQL
Estas mismas instrucciones vienen configuradas en el archivo **setup.py** del pipeline, para el uso en los workers
    
```
pip install apache-beam[gcp] google-cloud-storage google-cloud-bigquery pandas fsspec gcsfs

pip install pyodbc oracledb pymssql

wget https://public.dhe.ibm.com/software/ibmi/products/odbc/debs/dists/1.1.0/ibmi-acs-1.1.0.list
wget https://download.oracle.com/otn_software/linux/instantclient/1920000/oracle-instantclient19.20-basic-19.20.0.0.0-1.x86_64.rpm

mv -v ibmi-acs-1.1.0.list /etc/apt/sources.list.d/ibmi-acs-1.1.0.list

apt-get update

apt-get install --assume-yes unixodbc unixodbc-dev ibm-iaccess alien libaio1

alien -i --scripts oracle-instantclient19.20-basic-19.20.0.0.0-1.x86_64.rpm
```
