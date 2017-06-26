__author__ = 'cmgiler'

from app import vel_data_import as vdi
from app import COLLECTIONS as cln

# vdi.vel_data_convert()
# vdi.vel_folder_to_sql()
# vdi.vel_db_to_csvs()
vdi.encrypt_sql_database_values(cln.DATABASE_NAME,
                                cln.DATABASE_USER,
                                cln.DATABASE_PASSWORD,
                                'VELdb',
                                'cmgiler',
                                'epiphone')