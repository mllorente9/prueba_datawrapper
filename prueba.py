import os, sys, re
import logging
from logging.handlers import RotatingFileHandler

# Función para establecer el comportamiento de los logs durante la ejecución
def set_logs(logLevelParam = 'INFO'):
        logLevelParamCorr = logLevelParam.upper()
        logLevelDict = {
                        'INFO':logging.INFO,
                        'NOTSET':logging.NOTSET,
                        'DEBUG':logging.DEBUG,
                        'WARNING':logging.WARNING,
                        'ERROR':logging.ERROR,
                        'CRITICAL':logging.CRITICAL}
        if logLevelParamCorr in logLevelDict.keys():
                logLevel = logLevelDict[logLevelParamCorr]
        else:
                logLevel = 'INFO'

        logname = '/home/admin/logs/log.log'
        log_file_formatter = logging.Formatter('%(filename)s %(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
        log_stream_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')

        file_handler = RotatingFileHandler(logname, mode='a', maxBytes=5*1024*1024, backupCount=2, encoding=None, delay=0)
        file_handler.setFormatter(log_file_formatter)
        file_handler.setLevel(logLevel)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(log_stream_formatter)
        stream_handler.setLevel(logLevel)

        app_log = logging.getLogger('root')
        app_log.setLevel(logging.DEBUG)
        app_log.addHandler(file_handler)
        app_log.addHandler(stream_handler)
        return app_log

# Función que carga las variables globales del script, las cuales están almacenadas en un 
# fichero externo de propiedades.
def properties(path):
	prop = {}
	with open(path, 'r') as f:
		for line in f:
			key, val = line.split('=')
			key = key.strip()
			val = str(val).strip()
			prop[key] = val
	return prop

# Función que lista todos los ficheros excel dentro de un directorio.
def list_files(path):
	files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
	return files


# Función que transfiere el archivo a HDFS
def transfer_data(origin_path, dest_path):
	if dest_path[-1] != '/':
		dest_path = dest_path

	put = f'hdfs dfs -put -f {origin_path} {dest_path}'
	os.system(put)

	rm = f'rm {origin_path}'
	os.system(rm) 
	

def main():

    if len(sys.argv)>1:
        app_log = set_logs(sys.argv[1])
    else:
        app_log = set_logs()
	
    PROP_FILE = 'prueba.properties'

    try:
        PROPERTIES = properties(PROP_FILE)
    except Exception as ex:
        app_log.error("Error durante la carga del fichero de properties. {}".format(str(ex)))
        raise
	
    DATA_DIR = PROPERTIES['DATA_DIR']

    try:
        files = list_files(DATA_DIR)
        app_log.info('Listado de los ficheros dentro del directorio.')
    except Exception as ex:
        app_log.error(f'Error al listar los ficheros dentro del directorio. {ex}')
        raise

    for file in files:
        transfer_data(DATA_DIR + file, '.\\' + 'prueba_' + file)