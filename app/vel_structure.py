# -*- coding: utf-8 -*-
__author__ = 'cmgiler'

import math
import pandas as pd
import numpy as np
import datetime
import json
import xlrd
import COLLECTIONS as cln
import re


def init_vel_dict():
    # Initialize VEL_Dict
    return {'_id': '',
            'Test': '',
            'Date': '',
            'Test Description': {'Test Description': '',
                                 'Shift List': '',
                                 'Event List': '',
                                 'Test Options': '',
                                 'Report Regulation Type': ''},
            'Vehicle Information': {'VIN': '',
                                    'Vehicle Model': '',
                                    'Vehicle Year': '',
                                    'Build Date': '',
                                    'Trim Level': '',
                                    'Gross Weight': {'Value': '',
                                                     'Unit': ''},
                                    'Curb Weight': {'Value': '',
                                                    'Unit': ''},
                                    'Tire Size': '',
                                    'Tire Pressure': '',
                                    'Test Start Odometer': '',
                                    'Test End Odometer': '',
                                    'Test Group': '',
                                    'Engine Code': '',
                                    'Engine Displacement': '',
                                    'Engine Description': '',
                                    'Engine Catalyst': '',
                                    'Transmission': '',
                                    'Shift Point': '',
                                    'Axle Ratio': ''},
            'Test Specifications': {'CVS BulkStream Flow': {'Value': '',
                                                            'Unit': ''},
                                    'NMOG Ratio': '',
                                    'HCHO Ratio': ''},
            'Dynamometer': {'Inertia': {'Value': '',
                                        'Unit': ''},
                            'Dyno Coefficient A': {'Value': '',
                                                   'Unit': ''},
                            'Dyno Coefficient B': {'Value': '',
                                                   'Unit': ''},
                            'Dyno Coefficient C': {'Value': '',
                                                   'Unit': ''},
                            'Target Coefficient A': {'Value': '',
                                                     'Unit': ''},
                            'Target Coefficient B': {'Value': '',
                                                     'Unit': ''},
                            'Target Coefficient C': {'Value': '',
                                                     'Unit': ''}},
            'Fuel Information': {'Fuel': '',
                                 'CWF': '',
                                 'NHV': '',
                                 'Density (kg/l)': '',
                                 'HWF': '',
                                 'R-Factor': '',
                                 'OWF': '',
                                 'R-F': ''},
            'Emissions': {},
            'Remarks': {'Pre Test Remarks': '',
                        'Post Test Remarks': ''},
            'Personnel': {'Operator': '',
                          'Driver': '',
                          'Requestor': ''}}


def remove_nan_from_dict(dictionary):
    for key, value in dictionary.iteritems():
        if type(value) == dict:
            value = remove_nan_from_dict(value)
        else:
            if type(value) == float and math.isnan(value):
                dictionary[key] = ''
    return dictionary


def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    elif isinstance(x, datetime.time):
        return x.isoformat()
    raise x


def fix_keys(mydict):
    for key in mydict.keys():
        try:
            new_key = key.split('(')[0].strip()
        except:
            new_key = key
        mydict[new_key] = mydict.pop(key)
        if type(mydict[new_key]) == dict:
            mydict[new_key] = fix_keys(mydict[new_key])
    return mydict


def insert_query(columns_list, values_list, table_name):
    columns_str = '('
    values_tup = ()
    placeholder_str = '('
    for i in range(len(values_list)):
        if (values_list[i] != '') and (values_list[i] != []) and (str(values_list[i]).lower() != 'nan'):
            if (len(placeholder_str) == 1):
                placeholder_str += '%s'
                columns_str += columns_list[i]
            else:
                placeholder_str += ',%s'
                columns_str += (',' + columns_list[i])
            values_tup += (values_list[i], )
    columns_str += ')'
    placeholder_str += ')'
    query = "INSERT INTO " + table_name + " " + columns_str + " VALUES " + placeholder_str
    return query, values_tup


class VEL_Struct:
    def read_data(self, sheet_name):
        # print self.file_name
        try:
            if sheet_name == 'Test Info':
                df = pd.read_excel(self.file_name, sheet_name).x
            else:
                df = pd.read_excel(self.file_name, sheet_name)
        except:
            from xlrd import open_workbook
            wb = open_workbook(self.file_name)
            try:
                s = wb.sheet_by_index(sheet_name)
            except:
                s = wb.sheet_by_name(sheet_name)
            # print 'Sheet:',s.name
            values = []
            for row in range(s.nrows):
                col_value = []
                for col in range(s.ncols):
                    value = (s.cell(row, col).value)
                    try:
                        value = str(float(value))
                    except:
                        pass
                    if value == '':
                        value = np.nan
                    col_value.append(value)
                values.append(col_value)
            df = pd.DataFrame(values)
        return df

    def __init__(self, file_name):
        self.data = init_vel_dict()
        self.file_name = file_name
        data_tmp = self.read_data('Test Info')
        try:
            self.test_id = data_tmp.index[1][3]
        except:
            self.test_id = data_tmp.values[2,3]

    def remove_nan(self):
        self.data = remove_nan_from_dict(self.data)

    @staticmethod
    def find_bag_data_breakpoints(bag_data):
        num_rows, num_cols = bag_data.shape
        exp = '^HoribaReportSection\_Bagread\_'

        bag_data = bag_data.fillna('')
        row_breakpoints = bag_data[bag_data.columns[0]].str.contains(exp)
        section_startpoints = row_breakpoints[row_breakpoints].index.tolist()
        section_endpoints = [x - 1 for x in section_startpoints]
        section_endpoints = section_endpoints[1:]
        section_endpoints.append(num_rows)
        section_breakpoints = zip(section_startpoints, section_endpoints)
        return section_breakpoints

    def load_bag_data(self):
        # print 'Reading Bag Data Sheet...'
        try:
            df = self.read_data('Bag Results')
        except:
            print 'Bag Data Unavailable...'
            return
        breakpoints = self.find_bag_data_breakpoints(df)
        dict_labels = ['Phase 1',
                       'Phase 2',
                       'Phase 3',
                       'Phase 4',
                       'Phase 5',
                       'Phase 6',
                       '',
                       'Total',
                       '',
                       'Weighted Results']
        for i in range(len(dict_labels)):
            label = dict_labels[i]
            if label == '':
                continue
            # Determine starting and ending rows for current section
            start = breakpoints[i][0]
            end = breakpoints[i][1]
            # Split out test info section
            loc_idx = [idx for idx in range(len(df.columns)) if
                       df.columns[idx] == 'HoribaReportSection_Bagread_FE_Alternate']
            df_split_info = df.iloc[:, loc_idx[0]:].dropna(axis=1, subset=[start + 2])
            if len(df_split_info.columns) == 4 or i > 5:
                try:
                    test_info = df_split_info.iloc[start:end, 0:2]
                    test_info = test_info[~test_info.T.ix[1].isnull()]
                    test_info = test_info.drop_duplicates()
                    test_info = test_info.rename(index=test_info.iloc[:, 0])
                    test_info = test_info.drop(test_info.columns[0], axis=1)
                    test_info.columns = ['Test Info']
                    test_info = test_info.to_dict()['Test Info']
                except:
                    test_info = {}
                # Split out time info section
                try:
                    time_info = df_split_info.iloc[start:end, 2:]
                    time_info = time_info[~time_info.T.ix[1].isnull()]
                    time_info = time_info.drop_duplicates()
                    time_info = time_info.rename(index=time_info.iloc[:, 0])
                    time_info = time_info.drop(time_info.columns[0], axis=1)
                    time_info.columns = ['Time Info']
                    time_info = time_info.to_dict()['Time Info']
                except:
                    time_info = {}
            else:
                test_info = {}
                time_info = {}
            # Split out bag data section
            bag_data = df.ix[start:end, :]
            bag_data = bag_data.dropna(axis=1, subset=[start])
            bag_data = bag_data[~bag_data.T.ix[1].isnull()]
            #     break
            start = min(bag_data.index[:])
            end = max(bag_data.index[:])
            # bag_data_tmp = bag_data.dropna(axis=1, subset=[end])
            bag_data = bag_data.replace('SHOW', np.nan)
            bag_data.dropna(axis=1, subset=bag_data.index[1:], inplace=True, how='all')
            if i != 7:
                # if i < 7:
                #     if len(bag_data_tmp.iloc[0]) < len(bag_data.dropna(axis=1, subset=[end - 1]).iloc[0]):
                #         bag_data.dropna(axis=1, subset=[end - 1], inplace=True)
                #     else:
                #         bag_data = bag_data_tmp
                # else:
                #     bag_data = bag_data_tmp
                bag_data.columns = bag_data.iloc[0]
                bag_data.iloc[1, 0] = 'Unit'
                bag_data = bag_data.drop(bag_data.index[:1])
                bag_data.dropna(axis=1, subset=bag_data.index[1:], inplace=True, how='all')
                if i < 6:
                    bag_data.iloc[3, 0] = 'Sample Std'
                    bag_data.iloc[5, 0] = 'Ambient Std'
                bag_data = bag_data.rename(index=bag_data.iloc[:, 0])
                # bag_data = bag_data.drop_duplicates()
                bag_data = bag_data.drop(bag_data.columns[0], axis=1)
            else:
                # bag_data = bag_data_tmp
                bag_data.columns = bag_data.iloc[0]
                bag_data = bag_data.drop(bag_data.index[:1])
                bag_data.dropna(axis=1, subset=bag_data.index[1:], inplace=True, how='all')
                bag_data = bag_data.rename(index=bag_data.iloc[:, 0])
                # bag_data = bag_data.drop_duplicates()
                bag_data = bag_data.drop(bag_data.columns[0], axis=1)
            try:
                bag_data['NMOG+NOx'] = bag_data['NMOG'] + bag_data['NOx']
            except:
                continue
            bag_data = bag_data.to_dict()

            self.data['Emissions'][label] = {'Bag Data': bag_data,
                                            'Test Info': test_info,
                                            'Time Info': time_info}

        # For US06 Split Bag (Phase 1/3 results are saved to Phase 3 only
        if ('Phase 3' in self.data['Emissions'].keys()) and ('Phase 1' not in self.data['Emissions'].keys()):
            self.data['Emissions']['Phase 1'] = self.data['Emissions']['Phase 3']
            del self.data['Emissions']['Phase 3']


    def load_test_info(self):
        # print 'Reading Test Info Sheet...'
        try:
            df = self.read_data('Test Info')
        except:
            print 'Test Info Unavailable'
            return
        try:
            df.index = df.index.droplevel([4, 5, 6, 9, 10])
        except:
            pass
        df = df.reset_index()
        search_test_info_names = ['Test Description', 'Vehicle Information',
                                  'Test Specifications', 'Dynamometer',
                                  'Fuel Information', 'Personnel', 'Remarks']
        for search_name in search_test_info_names:
            key_names = self.data[search_name].keys()
            for idx, row in df.iterrows():
                row = row.dropna()
                for idx, elem in row.iteritems():
                    if type(elem) == unicode:
                        elem = elem.replace(':', '')
                        elem = elem.strip()

                        # For older versions:
                        elem = elem.replace('Target Road Load', 'Target Coefficient')
                        elem = elem.replace('Road Load', 'Dyno Coefficient')
                        if elem in key_names:
                            loc = row.keys().get_loc(idx)
                            if type(self.data[search_name][elem]) != dict:
                                if loc < len(row) - 1:
                                    if row.iloc[loc + 1] not in key_names:
                                        # 'Saving:', row.iloc[loc+1], ' -> ', search_name, '/', elem, 'in VEL_Dict'
                                        self.data[search_name][elem] = row.iloc[loc + 1]
                            else:
                                if loc < len(row) - 1:
                                    try:
                                        value = float(row.iloc[loc + 1])
                                        unit = row.iloc[loc + 2]
                                        self.data[search_name][elem]['Value'] = value
                                        self.data[search_name][elem]['Unit'] = unit
                                    except:
                                        self.data[search_name][elem]['Unit'] = row.iloc[loc + 1]

        base_level_info_names = ['ID', 'Test Date', 'Test']
        for idx, row in df.iterrows():
            row = row.dropna()
            for idx, elem in row.iteritems():
                if type(elem) == unicode:
                    elem = elem.replace(':', '')
                    elem = elem.strip()
                    if elem in base_level_info_names:
                        loc = row.keys().get_loc(idx)
                        if elem == 'ID':
                            search_name = '_id'
                        elif elem == 'Test Date':
                            search_name = 'Date'
                        elif elem == 'Test':
                            search_name = 'Test'
                        if type(self.data[search_name]) != dict:
                            if loc < len(row) - 1:
                                if row.iloc[loc + 1] not in base_level_info_names:
                                    if search_name == 'Date':
                                        try:
                                            cur_date = xlrd.xldate.xldate_as_datetime(float(row.iloc[loc+1]),0)
                                            self.data[search_name] = cur_date
                                        except:
                                            self.data[search_name] = row.iloc[loc + 1]
                                    else:
                                        self.data[search_name] = row.iloc[loc + 1]
        for idx, row in df.iterrows():
            row = row.dropna()
            for idx2, elem in row.iteritems():
                if elem == 'J2951':
                    df_j2951 = df.ix[idx:].ix[:, 3:].dropna(axis=1, how='all').dropna(how='all', axis=0)
                    df_j2951.iloc[0][0:2] = df_j2951.iloc[1][0:2]
                    df_j2951 = df_j2951.drop(df_j2951.index[1])
                    df_j2951.columns = df_j2951.iloc[0]
                    df_j2951 = df_j2951.drop(df_j2951.index[0])
                    df_j2951.index = df_j2951.ix[:,0]
                    self.data['J2951'] = df_j2951.T.to_dict()
                    return



    def load_pm_data(self):
        # print 'Reading Particulates Sheet...'
        try:
            df = self.read_data('Particulates')
            df.index = df.index.get_level_values(2)
            df = df.drop_duplicates()
            df = df.dropna(axis=1, subset=['Total:'])
            df = df.dropna(axis=0, subset=[df.columns[-1]])
            df.columns = df.iloc[0] + ' ' + df.iloc[1] + ' ' + df.iloc[2]
            df = df.ix[3:]
            df_tmp = df.reset_index()
            df_tmp = df_tmp.replace('Total:', 'Total')
            df_tmp = df_tmp.replace('Weighted g/mi', 'Weighted Results')
            df.index = df_tmp.iloc[:, 0]
            pm_data = df.T.to_dict()
            for key, value in pm_data.iteritems():
                if key in self.data['Emissions'].keys():
                    self.data['Emissions'][key]['PM'] = value
            # self.data['Emissions'][key]['PM']
        except:
            print 'PM Data Unavailable'
            return


    def to_json(self, outfile_name, to_file=True):
        if to_file:
            print 'Writing to', outfile_name
            self.remove_nan()
            with open(outfile_name, 'w') as outfile:
                json.dump(self.data, outfile, sort_keys=True, default=datetime_handler)
        else:
            print 'Converting to json format'
            self.remove_nan()
            return json.dump(self.data, sort_keys=True, default=datetime_handler)


    def to_mongo(self, db):
        pass


    def to_sql(self):
        import psycopg2
        # Connect to SQL database
        self.data = fix_keys(self.data)

        conn = psycopg2.connect("dbname=" + cln.DATABASE_NAME +
                                " user=" + cln.DATABASE_USER +
                                " password=" + cln.DATABASE_PASSWORD)
        cur = conn.cursor()


        ## GENERAL ##
        # Insert into 'general'
        # _id, nmog_ratio, test_date, driver, operator, test_name, test_description, hcho_ratio, event_list,
        # report_regulation_type, requestor, remarks_pre, shift_list, remarks_post, test_options
        table_name = 'general'
        column_names = ['_id', 'test_date', 'test_name', 'test_description', 'event_list',
                        'report_regulation_type', 'shift_list', 'hcho_ratio', 'nmog_ratio',
                        'driver', 'operator', 'requestor', 'remarks_pre', 'remarks_post', 'test_options']
        dict_values = [self.data['_id'], self.data['Date'],
                       self.data['Test'], self.data['Test Description']['Test Description'],
                       self.data['Test Description']['Event List'],
                       self.data['Test Description']['Report Regulation Type'],
                       self.data['Test Description']['Shift List'], self.data['Test Specifications']['HCHO Ratio'],
                       self.data['Test Specifications']['NMOG Ratio'], self.data['Personnel']['Driver'],
                       self.data['Personnel']['Operator'], self.data['Personnel']['Requestor'],
                       self.data['Remarks']['Pre Test Remarks'], self.data['Remarks']['Post Test Remarks'],
                       self.data['Test Description']['Test Options']]

        query, apply_values = insert_query(columns_list=column_names, values_list=dict_values, table_name=table_name)
        cur.execute(query, apply_values)


        ## FUEL_ECONOMY ##
        # Insert into 'fuel_economy'
        # _id, phase, grams_per_mi, unit
        table_name = 'fuel_economy'
        column_names = ['_id', 'phase', 'grams_per_mi', 'unit']

        for key, value in self.data['Emissions'].iteritems():
            if (key == 'Total'):
                dict_values = [self.data['_id'], key, value['Bag Data']['FE']['Total g/mi'], 'mpg']
            elif (key == 'Weighted Results'):
                # dict_values = [self.data['_id'], key, value['Bag Data']['FE']['Rounded g/mi'], 'mpg']
                continue
            else:
                if np.isnan(value['Bag Data']['FE']['Grams/mi']):
                    value['Bag Data']['FE']['Grams/mi'] = value['Bag Data']['FE']['Grams/ph']
                dict_values = [self.data['_id'], key, value['Bag Data']['FE']['Grams/mi'], 'mpg']
            query, apply_values = insert_query(column_names, dict_values, table_name)
            cur.execute(query, apply_values)


        ## VEHICLE ##
        # Insert into 'vehicle'
        # _id, axle_ratio, build_date, curb_weight, curb_weight_unit, engine_catalyst, engine_code, engine_description,
        # engine_displacement, gross_weight, gross_weight_units, shift_point, odo_test_start, odo_test_end, test_group,
        # tire_pressure, tire_size, transmission, trim_level, vin, vehicle_model, vehicle_year
        table_name = 'vehicle'
        column_names = ['_id', 'axle_ratio', 'build_date', 'curb_weight', 'curb_weight_unit', 'engine_catalyst',
                        'engine_code', 'engine_description', 'engine_displacement', 'gross_weight',
                        'gross_weight_units', 'shift_point', 'odo_test_start', 'odo_test_end', 'test_group',
                        'tire_pressure', 'tire_size', 'transmission', 'trim_level', 'vin', 'vehicle_model', 'vehicle_year']
        self.data['Vehicle Information']['Engine Displacement'] = \
            re.split('[a-zA-Z]', str(self.data['Vehicle Information']['Engine Displacement']))[0]
        dict_values = [self.data['_id'], self.data['Vehicle Information']['Axle Ratio'],
                         self.data['Vehicle Information']['Build Date'],
                         self.data['Vehicle Information']['Curb Weight']['Value'],
                         self.data['Vehicle Information']['Curb Weight']['Unit'],
                         self.data['Vehicle Information']['Engine Catalyst'],
                         self.data['Vehicle Information']['Engine Code'],
                         self.data['Vehicle Information']['Engine Description'],
                         self.data['Vehicle Information']['Engine Displacement'],
                         self.data['Vehicle Information']['Gross Weight']['Value'],
                         self.data['Vehicle Information']['Gross Weight']['Unit'],
                         self.data['Vehicle Information']['Shift Point'],
                         self.data['Vehicle Information']['Test Start Odometer'],
                         self.data['Vehicle Information']['Test End Odometer'],
                         self.data['Vehicle Information']['Test Group'],
                         int(float(self.data['Vehicle Information']['Tire Pressure'])),
                         self.data['Vehicle Information']['Tire Size'],
                         self.data['Vehicle Information']['Transmission'],
                         self.data['Vehicle Information']['Trim Level'],
                         self.data['Vehicle Information']['VIN'].upper(),
                         self.data['Vehicle Information']['Vehicle Model'],
                         int(float(self.data['Vehicle Information']['Vehicle Year']))]
        query, apply_values = insert_query(column_names, dict_values, table_name)
        cur.execute(query, apply_values)


        ## FUEL ##
        # Insert into 'fuel'
        # _id, cwf, density, fuel, hwf, nhv, owf, r_f, r_factor
        table_name = 'fuel'
        column_names = ['_id', 'cwf', 'density', 'fuel', 'hwf', 'nhv', 'owf', 'r_f', 'r_factor']
        dict_values = [self.data['_id'], self.data['Fuel Information']['CWF'],
                       self.data['Fuel Information']['Density'], self.data['Fuel Information']['Fuel'],
                       self.data['Fuel Information']['HWF'], self.data['Fuel Information']['NHV'],
                       self.data['Fuel Information']['OWF'], self.data['Fuel Information']['R-F'],
                       self.data['Fuel Information']['R-Factor']]
        query, apply_values = insert_query(column_names, dict_values, table_name)
        cur.execute(query, apply_values)

        ## DYNAMOMETER ##
        # Insert into 'dynamometer'
        # _id, inertia, dyno_coef_a, dyno_coef_b, dyno_coef_c, target_coef_a, target_coef_b, target_coef_c
        table_name = 'dynamometer'
        column_names= ['_id', 'inertia', 'dyno_coef_a', 'dyno_coef_b', 'dyno_coef_c',
                       'target_coef_a', 'target_coef_b', 'target_coef_c']
        dict_values = [self.data['_id'], self.data['Dynamometer']['Inertia']['Value'],
                       self.data['Dynamometer']['Dyno Coefficient A']['Value'],
                       self.data['Dynamometer']['Dyno Coefficient B']['Value'],
                       self.data['Dynamometer']['Dyno Coefficient C']['Value'],
                       self.data['Dynamometer']['Target Coefficient A']['Value'],
                       self.data['Dynamometer']['Target Coefficient B']['Value'],
                       self.data['Dynamometer']['Target Coefficient C']['Value']]
        query, apply_values = insert_query(column_names, dict_values, table_name)
        cur.execute(query, apply_values)

        # Insert into 'test_info'
        # _id, phase, ahum, baro, dil_factor, dist, nox_factor, rhum, sao_p, sao_t, saov, temp_f, vmix
        table_name = 'test_info'
        column_names = ['_id', 'phase', 'ahum', 'baro', 'dil_factor', 'dist', 'nox_factor',
                        'rhum', 'sao_p', 'sao_t', 'saov', 'temp_f', 'vmix']
        for key, value in self.data['Emissions'].iteritems():
            for key2, val2 in value['Test Info'].iteritems():
                value['Test Info'][key2] = str(val2)
            if (key == 'Total'):
                dict_values = [self.data['_id'], key, value['Test Info']['Ahum'],
                               value['Test Info']['Baro'], '', '', value['Test Info']['NOx Factor'],
                               value['Test Info']['Rhum'], '', '', '', value['Test Info']['Temp'],
                               '']

            elif (key == 'Weighted Results'):
                continue

            else:

                dict_values = [self.data['_id'], key, value['Test Info']['Ahum'],
                               value['Test Info']['Baro'], value['Test Info']['Dil.Factor'],
                               value['Test Info']['Dist'], value['Test Info']['NOx Factor'],
                               value['Test Info']['Rhum'],
                               value['Test Info']['Temp'], value['Test Info']['Vmix']]
            query, apply_values = insert_query(column_names, dict_values, table_name)
            cur.execute(query, apply_values)


        # Insert into 'time_info'
        # _id, phase, analysis_end, driver_errors, elapsed, end_shutdown_time, max_temp_f, min_temp_f, phase_finish, phase_start
        table_name = 'time_info'
        column_names = ['_id', 'phase', 'analysis_end', 'driver_errors', 'elapsed', 'end_shutdown_time',
                        'max_temp_f', 'min_temp_f', 'phase_finish', 'phase_start']
        for key, value in self.data['Emissions'].iteritems():
            if (key == 'Total') or (key=='Weighted Results'):
                continue
            else:
                dict_values = [self.data['_id'], key, value['Time Info']['Analysis End'],
                               value['Time Info']['Driver Errors'], '',
                               '', value['Time Info']['MaxTemp'], value['Time Info']['MinTemp'],
                               value['Time Info']['Phase finish'], value['Time Info']['Phase start']]
            query, apply_values = insert_query(column_names, dict_values, table_name)
            cur.execute(query, apply_values)
        # cur.execute("")

        # Insert into 'pm'
        # _id, phase, mass_emission, mass_per_distance
        table_name = 'pm'
        column_names = ['_id', 'phase', 'mass_emissions', 'mass_per_distance']
        for key, value in self.data['Emissions'].iteritems():
            if 'PM' in value.keys():
                pass
        # cur.execute("")

        # Insert into 'bag_em'
        # _id, phase, emission_type, ambient, ambient_std, grams_per_mi, grams_per_ph, modal_corr, net_conc, range,
        # sample, sample_std, unit
        table_name = 'bag_em'
        column_names = ['_id', 'phase', 'emission_type', 'ambient', 'ambient_std', 'grams_per_mi', 'grams_per_ph',
                        'modal_corr', 'net_conc', 'range', 'sample', 'sample_std', 'unit']
        for key, value in self.data['Emissions'].iteritems():
            for em_key, em_value in value['Bag Data'].iteritems():
                if em_key == 'Meas. THC' or em_key == 'CH3OH':
                    continue
                if em_key == 'FE':
                    continue
                if (key == 'Total'):
                    column_names = ['_id', 'phase', 'emission_type', 'grams_per_mi']
                    dict_values = [self.data['_id'], key, em_key, em_value['Rounded g/mi']]
                elif (key == 'Weighted Results'):
                    continue
                else:
                    # dict_values = [self.data['_id'], key, em_key, em_value['Ambient'], em_value['Ambient Std'],
                    #                em_value['Grams/mi'], em_value['Grams/ph'], em_value['Modal Corr'],
                    #                em_value['Net conc'], em_value['Range'], em_value['Sample'],
                    #                em_value['Sample Std'], em_value['Unit']]
                    column_names = ['_id', 'phase', 'emission_type', 'grams_per_mi',
                                    'grams_per_ph', 'ambient', 'range',
                                    'sample', 'modal_corr', 'net_conc']
                    dict_values = [self.data['_id'], key, em_key, em_value['Grams/mi'],
                                   em_value['Grams/ph'], em_value['Ambient'], em_value['Range'],
                                   em_value['Sample'], em_value['Modal Corr'], em_value['Net conc']]
                query, apply_values = insert_query(column_names, dict_values, table_name)
                cur.execute(query, apply_values)

        try:
            # Insert into 'j2951'
            # _id, cycle, cycle_id, ascr, eer, iwr
            table_name = 'j2951'
            column_names = ['_id', 'cycle', 'cycle_id', 'ascr', 'eer', 'iwr']
            for key, value in self.data['J2951'].iteritems():
                dict_values = [self.data['_id'], value['Cycle'], value['Cycle ID'],
                               value['ASCR'], value['EER'], value['IWR']]
                query, apply_values = insert_query(column_names, dict_values, table_name)
                cur.execute(query, apply_values)
        except:
            print 'J2951 Data not available for Test ID ' + self.data['_id']


        conn.commit()
        cur.close()
        conn.close()
        return