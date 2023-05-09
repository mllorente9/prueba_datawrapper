import os
import subprocess

# Ruta al archivo que deseas descargar de HDFS
hdfs_path = 'C:\\Users\\miguel.llorente\\Documents\\Nastat\\nastat\\hive\\distribution\\Industria_construccion_y_servicios\\040106_produccion_turismos.sh'

# Ruta local donde deseas almacenar el archivo descargado
local_path = 'C:\\Users\\miguel.llorente\\Desktop\\'

# Descarga el archivo de HDFS a tu sistema local
subprocess.call(['hadoop', 'fs', '-copyToLocal', hdfs_path, local_path])

# Agrega el archivo al repositorio Git y realiza un commit
git_path = 'C:\\Users\\miguel.llorente\\Documents\\Nastat\\prueba_datawrapper\\prueba_datawrapper'
git_file = os.path.join(git_path, 'nombre_del_archivo')

subprocess.call(['git', '-C', git_path, 'add', git_file])
subprocess.call(['git', '-C', git_path, 'commit', '-m', 'Subiendo archivo desde HDFS'])

# Carga el archivo en el repositorio Git remoto
subprocess.call(['git', '-C', git_path, 'push'])