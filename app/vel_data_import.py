__author__ = 'cmgiler'
import vel_structure as vs

import Tkinter
import tkFileDialog
import os
import COLLECTIONS as cln

def vel_data_convert():
    """
    Reads in input file name (.xlsx format) and output file name (.json format) and imports data and writes to new JSON
    format file.
    :param data_file:
    :return:
    """
    root = Tkinter.Tk()
    root.withdraw()

    currdir = os.getcwd()
    file_name = tkFileDialog.askopenfilename(parent=root, initialdir=currdir, title='Choose a file',
                                             filetypes=[('Excel Spreadsheet', '*.xlsx')])

    if len(file_name) > 0:
        # Set up and run import / conversion
        VEL_Dict = vs.VEL_Struct(file_name)
        VEL_Dict.load_test_info()
        VEL_Dict.load_bag_data()
        VEL_Dict.load_pm_data()

        output_file_name = file_name.replace('.xlsx','.json')
        # output_file_name = tkFileDialog.asksaveasfilename(filetypes=[('JSON File', '*.json')],
        #                                                   title='Choose JSON file name',
        #                                                   defaultextension='.json')
        if len(output_file_name) > 0:
            VEL_Dict.to_json(outfile_name=output_file_name)


def find_files(folder, extension_list):
    import os
    from fnmatch import fnmatch

    file_list = []
    for path, subdirs, files in os.walk(folder):
        for name in files:
            for pattern in extension_list:
                if fnmatch(name, pattern):
                    if ("modal" not in name.lower()) and (("HATCIAA" in name) or ("HATCICA" in name)):
                        file_list.append(os.path.join(path, name))
    return file_list


def vel_folder_to_sql():
    import os
    root = Tkinter.Tk()
    root.withdraw()
    currdir = os.getcwd()
    folder_name = tkFileDialog.askdirectory()
    if folder_name == '':
        return

    files = find_files(folder_name, ['*.xlsx', '*.xls'])

    # Look up current id list in database
    import psycopg2
    conn = psycopg2.connect("dbname=" + cln.DATABASE_NAME +
                            " user=" + cln.DATABASE_USER +
                            " password=" + cln.DATABASE_PASSWORD)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT _id FROM general")
    available_ids = [i[0] for i in cur.fetchall()]
    cur.close()
    conn.close()

    ctr = 0
    print
    print ('{0} possible files found in selected folder. Beginning conversion and database update.'.format(len(files)))
    for file_name in files:
        ctr += 1
        try:
            VEL_Dict = vs.VEL_Struct(file_name)
        except:
            continue

        # Check if already in database:
        if VEL_Dict.test_id in available_ids:
            print('({0}/{1}) Test Report with ID "{2}" is already in database.'.format(ctr,
                                                                                len(files),
                                                                                VEL_Dict.test_id))
            continue

        # Load all data
        try:
            VEL_Dict.load_test_info()
        except:
            print('({0}/{1}) Cannot read Test Report with ID "{2}".'.format(ctr,
                                                                           len(files),
                                                                           VEL_Dict.test_id))
            continue
            # pass
        VEL_Dict.load_bag_data()
        if VEL_Dict.data['Emissions'].keys() == []:
            print('({0}/{1}) Cannot add Test Report with ID "{2}".'.format(ctr,
                                                                            len(files),
                                                                            VEL_Dict.test_id))
            continue

        VEL_Dict.load_pm_data()

        if len(file_name) > 0:
            try:
                VEL_Dict.to_sql()
                print('({0}/{1}) SUCCESS: Test Report ID "{2}" added to database.'.format(ctr,
                                                                                              len(files),
                                                                                              VEL_Dict.data['_id']))
                available_ids.append(VEL_Dict.data['_id'])
            except:
                print('({0}/{1}) ERROR: Could not add ID "{2}" to database.'.format(ctr,
                                                                                    len(files),
                                                                                    VEL_Dict.data['_id']))

def vel_db_to_csvs(use_aws=False):
    import COLLECTIONS as cln
    import psycopg2
    import os
    from datetime import datetime
    import pandas as pd

    # Get folder name
    root = Tkinter.Tk()
    root.withdraw()
    currdir = os.getcwd()
    folder_name = tkFileDialog.askdirectory()
    if folder_name == '':
        return

    # Start SQL Instance
    if use_aws:
        conn = psycopg2.connect(host=cln.AWS_HOST,
                                port=cln.AWS_PORT,
                                dbname=cln.AWS_DATABASE_NAME,
                                user=cln.AWS_DATABASE_USER,
                                password=cln.AWS_DATABASE_PASSWORD)
    else:
        conn = psycopg2.connect("dbname=" + cln.DATABASE_NAME +
                                " user=" + cln.DATABASE_USER +
                                " password=" + cln.DATABASE_PASSWORD)
    cur = conn.cursor()

    # Get set of table names in database
    cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    AND table_type='BASE TABLE';
                """)
    table_names = [i[0] for i in cur.fetchall()]

    for tb_name in table_names:
        # Get column names
        cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name=
                    """ + "'" + tb_name + "';")
        column_names = [i[0] for i in cur.fetchall()]

        # Get data
        cur.execute("SELECT * FROM " + tb_name)
        cur_data = pd.DataFrame(cur.fetchall(), columns=column_names)

        # Create file name (csv)
        today_date = str(datetime.utcnow().date())
        csv_name = "VEL_Data_Export_" + tb_name + "_" + today_date + ".csv"

        # Write to csv
        cur_data.to_csv(os.path.join(folder_name, csv_name))

    # Close Instance
    cur.close()
    conn.close()

def id_generator(size=10):
    import string
    import random
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))

def encrypt_sql_database_values(base_db_name='', base_db_user='', base_db_password='',
                                enc_db_name='', enc_db_user='', enc_db_password=''):
    """
    Encryption of database to allow posting and using on AWS server for personal portfolio.
    This database protects Hyundai's private emission report data, while still allowing demo to 
    visualize similar trends in data.
    Two types of encryption:
        - Encrypt text fields by creating a dictionary to randomly shuffle letters/numbers for each value.
        - Encrypt numbers by selecting some factor based vehicle name.
    :param base_db_name: name of database to be encrypted
    :param base_db_user: name of user for database
    :param base_db_password: password for database user
    :param enc_db_name: Name of new database containing encrypted values
    :param enc_db_user: 
    :param enc_db_password: 
    :return: 
    """

    # Step 0: Import required libraries
    import psycopg2
    import sqlalchemy
    import numpy as np
    import pandas as pd
    import pandas.io.sql as pdsql
    from random import uniform as rand_unf

    # Step 1: Connect to database.
    connection_str = "{0}:{1}@localhost/{2}".format(base_db_user, base_db_password, base_db_name.replace("\\",""))
    engine = sqlalchemy.create_engine("postgresql+psycopg2://"+connection_str)

    # Step 2: Load in data as pandas dataframes to minimize calls to SQL Database
    bag_em = pdsql.read_sql_table('bag_em', engine)
    dynamometer = pdsql.read_sql_table('dynamometer', engine)
    fuel = pdsql.read_sql_table('fuel', engine)
    fuel_economy = pdsql.read_sql_table('fuel_economy', engine)
    general = pdsql.read_sql_table('general', engine)
    pm = pdsql.read_sql_table('pm', engine)
    test_info = pdsql.read_sql_table('test_info', engine)
    time_info = pdsql.read_sql_table('time_info', engine)
    vehicle = pdsql.read_sql_table('vehicle', engine)
    j2951 = pdsql.read_sql_table('j2951', engine)



    # Step 3: Create link of test_id to vehicle
    id_veh_map = {}
    for test, row in vehicle.iterrows():
        id_veh_map[row['_id']] = row['vin']

    # Step 4: Loop through bag emissions and create list of factors for each
    # print bag_em.head()
    veh_em_offset = {}
    for key, veh_name in id_veh_map.iteritems():
        if veh_name not in veh_em_offset.keys():
            veh_em_offset[veh_name] = rand_unf(0.6, 1.4)
    for col_name in bag_em.columns:
        if col_name not in ['_id', 'phase', 'emission_type']:
            bag_em[col_name] = bag_em[col_name] * \
                                         bag_em['_id'].apply(lambda x: veh_em_offset[id_veh_map[x]])
    # print bag_em.head()

    # Step 5: Create new random numbers for dynamometer coefficients
    # (diff random number for inertia and other coefficients)
    # print dynamometer.head()
    veh_dyno_offset = {}
    for key in veh_em_offset.keys():
        veh_dyno_offset[key] = {}
        veh_dyno_offset[key]['inertia'] = rand_unf(0.6, 1.4)
        veh_dyno_offset[key]['coef'] = rand_unf(0.7,1.3)
    for col_name in dynamometer.columns:
        if col_name == 'inertia':
            dynamometer[col_name] = dynamometer[col_name] * \
                dynamometer['_id'].apply(lambda x: veh_dyno_offset[id_veh_map[x]]['inertia'])
        elif col_name not in ['_id', 'inertia']:
            dynamometer[col_name] = dynamometer[col_name] * \
                dynamometer['_id'].apply(lambda x: veh_dyno_offset[id_veh_map[x]]['coef'])
    # print dynamometer.head()


    # Step 6: Randomize Fuel Numbers (multiply entire table by the same random number
    # print fuel.head()
    fuel_name_map = {}
    i = 1
    for fuel_name in fuel['fuel'].unique():
        fuel_name_map[fuel_name] = 'Fuel #'+str(i)
        i+=1
    # print fuel_name_map
    fuel['fuel'] = fuel['fuel'].apply(lambda x: fuel_name_map[x])
    for col_name in fuel.columns:
        if col_name not in ['_id', 'fuel', 'r_f']:
            fuel_factor = rand_unf(0.7, 1.4)
            fuel[col_name] = fuel[col_name] * fuel_factor
    # print fuel.head()


    # Step 7: Randomize general info:
    #   - modify nmog_ratio, hcho_ratio, and driver/operator/requestor names
    #   - remove test_options, shift_list, event_list, and remarks
    # print general.head()

    general['nmog_ratio'] = general['nmog_ratio'] * rand_unf(0.7,1.3)
    general['hcho_ratio'] = general['hcho_ratio'] * rand_unf(0.7,1.3)
    driver_map = {}
    requestor_map = {}
    i = 1
    for driver in sorted(general['driver'].unique()):
        driver_map[driver] = 'Technician #'+str(i)
        i+=1
    for operator in sorted(general['operator'].unique()):
        if operator not in driver_map.keys():
            driver_map[operator] = 'Technician #' + str(i)
            i += 1
    # print driver_map
    i = 1
    for requestor in general['requestor'].unique():
        requestor_map[requestor] = 'Engineer #' + str(i)
        i += 1
    # print requestor_map
    general['driver'] = general['driver'].apply(lambda x: driver_map[x])
    general['operator'] = general['operator'].apply(lambda x: driver_map[x])
    general['requestor'] = general['requestor'].apply(lambda x: requestor_map[x])

    general['test_description'] = ''
    general['event_list'] = ''
    general['remarks_pre'] = ''
    general['shift_list'] = ''
    general['remarks_post'] = ''
    general['test_options'] = ''

    # print general.head()


    # Step 8: Randomize pm data (mass_emission and mass_per_distance - select random factor by vehicle)
    for col_name in pm.columns:
        if col_name not in ['_id', 'phase']:
            pm[col_name] = pm[col_name] * \
                           pm['_id'].apply(lambda x: veh_em_offset[id_veh_map[x]])


    # Step 9: Randomize vehicle info (all of it, except for Odometer and tire pressure)
    vehicle['axle_ratio'] = vehicle['axle_ratio'].apply(lambda x: np.nan)
    vehicle['curb_weight'] = dynamometer['inertia']
    vehicle['engine_catalyst'] = None
    vehicle['engine_code'] = None
    engine_desc_map = {}
    i = 1
    for desc in vehicle['engine_description'].unique():
        engine_desc_map[desc] = "Engine Type #"+str(i)
        i += 1
    vehicle['engine_description'] = vehicle['engine_description'].apply(lambda x: engine_desc_map[x])
    rand_factor = rand_unf(0.7,1.3)
    vehicle['engine_displacement'] = vehicle['engine_displacement'].apply(lambda x: round(x*rand_factor,1))
    vehicle['gross_weight'] = dynamometer['inertia']
    vehicle['shift_point'] = None

    test_group_map = {}
    i = 1
    for group in vehicle['test_group'].unique():
        if group == None:
            test_group_map[group] = None
        else:
            test_group_map[group] = "Test Group #" + str(i)
            i += 1
    vehicle['test_group'] = vehicle['test_group'].apply(lambda x: test_group_map[x])

    transmission_map = {}
    i = 1
    for trans in vehicle['transmission'].unique():
        transmission_map[trans] = "T" + str(i)
        i+=1
    vehicle['transmission'] = vehicle['transmission'].apply(lambda x: transmission_map[x])

    vehicle['trim_level'] = None

    vin_map = {}
    for vin in vehicle['vin'].unique():
        vin_map[vin] = id_generator(size=8)
    vehicle['vin'] = vehicle['vin'].apply(lambda x: vin_map[x])

    veh_map = {}
    i = 1
    for model in vehicle['vehicle_model'].unique():
        veh_map[model] = "Vehicle" + str(i)
        i += 1
    vehicle['vehicle_model'] = vehicle['vehicle_model'].apply(lambda x: veh_map[x])
    vehicle['tire_size'] = None

    # Step 10: Randomize j2951 data
    for col_name in j2951.columns:
        if col_name not in ['_id', 'cycle', 'cycle_id']:
            j2951[col_name] = j2951[col_name] * rand_unf(0.7, 1.3)

    #  Step 11: Randomize _id names among all tables
    for key in id_veh_map.keys():
        id_veh_map[key] = id_generator(size=8)+'_'+id_generator(size=8)+'_'+id_generator(size=3)

    bag_em['_id'] = bag_em['_id'].apply(lambda x: id_veh_map[x])
    dynamometer['_id'] = dynamometer['_id'].apply(lambda x: id_veh_map[x])
    fuel['_id'] = fuel['_id'].apply(lambda x: id_veh_map[x])
    fuel_economy['_id'] = fuel_economy['_id'].apply(lambda x: id_veh_map[x])
    general['_id'] = general['_id'].apply(lambda x: id_veh_map[x])
    pm['_id'] = pm['_id'].apply(lambda x: id_veh_map[x])
    test_info['_id'] = test_info['_id'].apply(lambda x: id_veh_map[x])
    time_info['_id'] = time_info['_id'].apply(lambda x: id_veh_map[x])
    vehicle['_id'] = vehicle['_id'].apply(lambda x: id_veh_map[x])
    j2951['_id'] = j2951['_id'].apply(lambda x: id_veh_map[x])

    # Write new tables to new database (enc_db_name/user/password)
    ## Reset
    table_names = pdsql.read_sql_query("select table_name from information_schema.tables where table_schema='public'",
                                       engine)
    reset_query = ""
    for table in table_names.values:
        reset_query += "DELETE FROM " + table + "; "

    conn = psycopg2.connect(host="postgres-instance.cm2vo1ykiz5s.us-east-2.rds.amazonaws.com",
                            port=5432,
                            dbname=enc_db_name,
                            user=enc_db_user,
                            password=enc_db_password)
    cur = conn.cursor()
    cur.execute(reset_query[0])
    conn.commit()
    cur.close()
    conn.close()

    connection_str = "postgresql://{0}:{1}@postgres-instance.cm2vo1ykiz5s.us-east-2.rds.amazonaws.com:5432/{2}".format(enc_db_user,
                                                    enc_db_password,
                                                    enc_db_name.replace("\\", ""))

    engine_aws = sqlalchemy.create_engine(connection_str)

    print 'general...'
    general.to_sql('general', engine_aws, if_exists='append', index=False)
    print 'dynamometer...'
    dynamometer.to_sql('dynamometer', engine_aws, if_exists='append', index=False)
    print 'fuel...'
    fuel.to_sql('fuel', engine_aws, if_exists='append', index=False)
    print 'fuel_economy...'
    fuel_economy.to_sql('fuel_economy', engine_aws, if_exists='append', index=False)
    print 'pm...'
    pm.to_sql('pm', engine_aws, if_exists='append', index=False)
    print 'test_info...'
    test_info.to_sql('test_info', engine_aws, if_exists='append', index=False)
    print 'time_info...'
    time_info.to_sql('time_info', engine_aws, if_exists='append', index=False)
    print 'vehicle...'
    vehicle.to_sql('vehicle', engine_aws, if_exists='append', index=False)
    print 'j2951...'
    j2951.to_sql('j2951', engine_aws, if_exists='append', index=False)
    print 'bag_em...'
    bag_em.to_sql('bag_em', engine_aws, if_exists='append', index=False)