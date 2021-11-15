import numpy
from driver.common import CommonMethods as cmn
import traceback
from configs import AppConfig
from utils import get_logger

log = get_logger('Driver_Classification')


class DriverFileClassification:

    def __init__(self, fips):
        # AIN
        self.app_con = AppConfig()
        self.parcel = [data for data in self.app_con.get_parcel_col().split(',')]
        self.owner_name = [data for data in self.app_con.get_owner_name_col().split(',')]
        self.loc_add = [data for data in self.app_con.get_loc_add_col().split(',')]
        self.mail_add = [data for data in self.app_con.get_mail_add_col().split(',')]
        self.legal = [data for data in self.app_con.get_legal_col().split(',')]
        self.not_col = [data for data in self.app_con.get_not_col().split(',')]
        self.rpt_driver_file_name = self.app_con.get_rpt_driver_file_name()
        self.rpt_diablo_file_name = self.app_con.get_rpt_diablo_driver_file_name()
        self.exclude_file_names = [data for data in self.app_con.get_exclude_file_names().split(',')]
        self.report_lst = []
        self.diablo_report = []
        self.report_lst.append([data for data in self.app_con.get_driver_col().split(',')])
        self.diablo_report.append([data for data in self.app_con.get_diablo_driver_col().split(',')])
        self.fips = fips
        self.cmn = cmn()
        self.db_pcl_info = []
        self.db_pcl_len = []

    def short_list_driver_file(self, files_lst: list, path: str, orig_pcl_id: list, iris_sec_key: list):
        file_name = None
        short_listed_file_col = {}
        short_listed_files = {}
        filtered_files = {}

        try:
            for data in files_lst:
                col_lst = []
                file_name = data[0]
                desc, legal_col, loc, loc_col, mail, mail_col, owner, owner_col, parcel, pcl_col = self.cmn.column_filter(data, file_name, self.parcel, self.owner_name, self.loc_add, self.mail_add, self.legal, self.not_col)
                per_equip_inx = []
                valid_file_name = self.cmn.validate_file_name(file_name, self.exclude_file_names)
                if parcel is True and valid_file_name is True and desc is True:
                    col_lst = self.cmn.read_cols(path, file_name, legal_col)
                    for col_data in col_lst:
                        per_equip_inx = [i for i, e in enumerate(col_data) if 'personal' in e.lower() or 'equip' in e.lower()]
                    final_data = []
                    for col_data in col_lst:
                        shortlist_col = [i for j, i in enumerate(col_data) if j not in per_equip_inx]
                        final_data.append(shortlist_col)
                    final_data = [col_data for col_data in final_data if col_data[0] in pcl_col]
                    short_listed_files[file_name] = final_data
                    short_listed_file_col[file_name] = [loc_col, mail_col, owner_col, pcl_col]

                elif parcel is True and valid_file_name is True:
                    col_lst = self.cmn.read_cols(path, file_name, pcl_col)
                    short_listed_files[file_name] = col_lst
                    short_listed_file_col[file_name] = [loc_col, mail_col, owner_col, pcl_col]

                rpt_parcel = 'Yes' if parcel is True else 'No'
                rpt_loc = 'Yes' if loc is True else 'No'
                rpt_owner = 'Yes' if owner is True else 'No'
                rpt_mail = 'Yes' if mail is True else 'No'
                rpt_desc = 'Yes' if desc is True else 'No'

                self.report_lst.append([file_name, 'No', rpt_parcel, rpt_loc, rpt_owner, rpt_mail, rpt_desc, len(per_equip_inx), 'No', '', '', ''])
            if len(short_listed_files) > 0:
                for key1, val1 in short_listed_files.items():
                    filtered_col = []
                    for data in val1:
                        col_len = []
                        for lin_num, data1 in enumerate(data):
                            if lin_num == 0:
                                col_len.append(data1)
                            else:
                                col_len.append(data1)
                        if len(col_len) > 1:
                            filtered_col.append(col_len)
                    if len(filtered_col) > 0:
                        longest_list = self.cmn.get_max_len_list(filtered_col)
                        unique_parcel = list(dict.fromkeys(longest_list))
                        unique_percentage = 100 * float(len(unique_parcel)) / float(len(longest_list))
                        if unique_percentage >= 60.0:
                            col_names = short_listed_file_col[key1]
                            if (len(col_names[0]) > 0 or (len(col_names[1]) > 0 and len(col_names[2]) > 0)) or ('parcel' in str(key1).lower() or 'main' in str(key1).lower()):
                                filtered_files[key1] = unique_parcel
                        log.info(f'{key1} Total values: {len(longest_list)} --> Unique Parcel Percentage: {unique_percentage}')
                    else:
                        log.info(f'{key1} does not have any filtered columns')
            else:
                for data in files_lst:
                    col_lst = []
                    file_name = data[0]
                    col_names = data[2]
                    for cnam in col_names:
                        col_lst = self.cmn.read_cols(path, file_name, cnam)

            for data in self.report_lst:
                for key, val in filtered_files.items():
                    if data[0] == key:
                        data[8] = 'Yes'
                        data[9] = len(val)
                        break
        except Exception as ex:
            log.info(ex.args)
            log.info(traceback.format_exc)
        log.info(f'Short Listed Driver Files: {filtered_files.keys()}')
        log.info(f'Short Listed File columns: {short_listed_file_col}')
        return filtered_files, short_listed_file_col

    def filter_driver_files(self, short_listed_files, short_listed_file_col):
        log.info('---------------------------------------------------------------------------------------------------')
        log.info('--------------------------------------------Filtering Files----------------------------------------')
        not_a_driver = []
        invalid_data = []
        dis_joint_columns = {}
        driver_files = {}
        filter_driver_files = list(short_listed_files.keys())
        try:
            for key1, value1 in short_listed_files.items():
                result1 = [self.cmn.clean_up_parcel(data) for data in value1 if not ''.join([*filter(str.isalnum, data)]).isalpha() and ''.join([*filter(str.isalnum, data)]).strip() != '' and ''.join([*filter(str.isalnum, data)]).strip() is not None]
                driver_files[key1] = [result1, value1[0]]
                if len(short_listed_files.items()) == 1:
                    dis_joint_columns[key1] = len(result1)
                    log.info(f'Only One file has shortlisted {key1}')
                elif key1 not in not_a_driver:
                    driver_col_data = [val for key, val in short_listed_files.items() if key != key1 and key not in not_a_driver]
                    result2 = [self.cmn.clean_up_parcel(data) for l in driver_col_data for data in l if not ''.join([*filter(str.isalnum, data)]).isalpha() and ''.join([*filter(str.isalnum, data)]).strip() != '' and ''.join([*filter(str.isalnum, data)]).strip() is not None]
                    log.info(f'File: {key1} - column: {value1[0]}  values are comparing with other shortlisted files')
                    a1 = numpy.array(result1)
                    a2 = numpy.array(result2)
                    diff1 = numpy.setdiff1d(a1, a2)
                    log.info(f'File: {key1} - has {len(diff1)} disjoint parcels')

                    if len(filter_driver_files) == 2 and len(diff1) == 0:
                        diff2 = numpy.setdiff1d(a2, a1)
                        next_item = [key for key in filter_driver_files if key != key1 and key not in not_a_driver]
                        driver_files[next_item[0]] = [result2, driver_col_data[0][0]]
                        log.info(f'File: {next_item[0]} - has {len(diff2)} disjoint parcels')
                        if len(diff1) == 0 and len(diff2) == 0:
                            found = False
                            for key2, value2 in short_listed_file_col.items():
                                if key2 == key1:
                                    for col in value2:
                                        if any('loc' in s.lower() or 'situs' in s.lower() or 'property' in s.lower() for s in col):
                                            log.info(f'File: {key1} - has situs or property or loc columns')
                                            found = True
                                            not_a_driver.append(next_item[0])
                                            dis_joint_columns[next_item[0]] = len(diff2)
                                            dis_joint_columns[key1] = len(diff1)
                                            break
                                    if found:
                                        break
                                if key2 in next_item[0]:
                                    for col in value2:
                                        if any('loc' in s.lower() or 'situs' in s.lower() or 'property' in s.lower() for s in col):
                                            log.info(f'File: {next_item[0]} - has situs or property or loc columns')
                                            found = True
                                            not_a_driver.append(key1)
                                            dis_joint_columns[key1] = len(diff1)
                                            dis_joint_columns[next_item[0]] = len(diff2)
                                            break
                            if found is False:
                                not_a_driver.append(key1)
                        else:
                            not_a_driver.append(key1)
                            dis_joint_columns[key1] = len(diff1)
                            dis_joint_columns[next_item[0]] = len(diff2)
                        continue
                    if len(diff1) <= 4:
                        not_a_driver.append(key1)
                        log.info(f'File: {key1} - has {len(diff1)} <= 4 disjoint parcels and {key1} not a driver file')
                        if key1 not in dis_joint_columns.keys():
                            dis_joint_columns[key1] = len(diff1)
                        next_item = list(short_listed_files.items())[1]
                        if len(short_listed_files.items()) == 2 and next_item[0] not in dis_joint_columns.keys():
                            driver_files[next_item[0]] = [result2, driver_col_data[0][0]]
                            diff2 = numpy.setdiff1d(a2, a1)
                            log.info(f'File: {next_item[0]} - has {len(diff2)} disjoint parcels')
                            dis_joint_columns[next_item[0]] = len(diff2)
                    elif len(driver_col_data) > 0:
                        dis_joint_columns[key1] = len(diff1)

                for key in not_a_driver:
                    if key in filter_driver_files:
                        filter_driver_files.remove(key)
        except Exception as ex:
            log.info(ex.args)
            log.info(traceback.format_exc)
        driver_col_data = [val[0] for key, val in driver_files.items() if key not in not_a_driver]
        result = [data for l in driver_col_data for data in l]
        result = list(dict.fromkeys(result))
        log.info(f'--> Non Driver Files: {not_a_driver}')
        return driver_files, result, dis_joint_columns, not_a_driver, invalid_data

    def driver_runner(self, path: str, out_path: str):
        try:
            tab_files = self.cmn.get_tab_files(path)
            tab_files_col = []
            orig_pcl_id, iris_sec_key = self.cmn.get_diablo_parcel_info(self.fips)
            for file in tab_files:
                col_len, col_names = self.cmn.read_col_nam_col_len(path, file)
                tab_files_col.append([file, col_len, col_names])
            short_listed_file, short_listed_file_col = self.short_list_driver_file(tab_files_col, path, orig_pcl_id, iris_sec_key)
            driver_files, result, dis_joint_columns, not_a_driver, invalid_data = self.filter_driver_files(short_listed_file, short_listed_file_col)
            for data in self.report_lst:
                if data[0] in driver_files.keys() and data[0] in dis_joint_columns.keys() and data[0] not in not_a_driver:
                    log.info(f'--> Driver File: {data[0]}')
                    data[1] = 'Yes'
                    data[9] = len(driver_files[data[0]][0])
                    data[10] = dis_joint_columns[data[0]]
                    data[11] = driver_files[data[0]][1]
                if data[0] in not_a_driver and data[0] in dis_joint_columns.keys():
                    data[9] = len(driver_files[data[0]][0])
                    data[10] = dis_joint_columns[data[0]]
                    data[11] = driver_files[data[0]][1]

            self.cmn.create_reports(out_path, self.report_lst, self.rpt_driver_file_name)
            # Diablo code ---------------------------
            log.info('Driver report has been generated')
            log.info('---------------------------------------------------------------------------------------------------')
            log.info('--------------------------------------------Diablo comparison----------------------------------------')
            # row_count = self.cmn.get_diablo_row_count(self.fips)
            log.info(f'Diablo  Parcel Count: {len(orig_pcl_id)}')
            unique_pcl_cnt = len(result) - int(len(orig_pcl_id))
            log.info(f'Parcel Count Difference: {unique_pcl_cnt}')
            diff1, diff2 = self.cmn.list_comparison(result, orig_pcl_id)

            iris_key_val = list(dict.fromkeys(iris_sec_key))

            if (len(diff2) == len(orig_pcl_id)) and len(iris_key_val) > 1:
                log.info('Diablo Parcel count and Number of parcels dropped are same then using IrisSecKey')
                diff1, diff2 = self.cmn.list_comparison(result, iris_sec_key)
                log.info('IrisSecKey column is used to compare the Data')
            elif (len(diff2) == len(orig_pcl_id)) and len(iris_key_val) <= 1:
                log.info('Diablo Parcel count and Number of parcels dropped are same and IrisSecKey has NULL values')
            else:
                log.info('OrigPclId column is used to compare the Data')
            self.diablo_report.append([str(len(result)), len(orig_pcl_id), unique_pcl_cnt, len(diff1), len(diff2)])
            self.cmn.create_reports(out_path, self.diablo_report, self.rpt_diablo_file_name)

            # End of Diablo code
        except Exception as ex:
            log.info(ex.args)
            log.info(traceback.format_exc)


if __name__ == '__main__':
    op_path = r'C:\mvpreports'
    in_path = r'C:\mvpdata\nm\MORA'
    obj = DriverFileClassification('35033')
    obj.driver_runner(in_path, op_path)
