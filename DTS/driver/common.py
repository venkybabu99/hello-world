import os
import fnmatch
import itertools
import re
from fuzzywuzzy import fuzz
from typing import List
import traceback
from itertools import repeat
import csv
from database import Database
from configs import AppConfig
import copy
import numpy
from utils import get_logger
log = get_logger('Driver_Utility')


class CommonMethods:

    def __init__(self):
        self.app_obj = AppConfig()

    @staticmethod
    def get_tab_files(path: str):
        file_lst = []
        try:
            files = fnmatch.filter(os.listdir(path), "*.tab")
            for file_name_ext in files:
                file_lst.append(file_name_ext)
        except Exception as ex:
            print(ex.args)
            print(traceback.format_exc)
        return file_lst

    @staticmethod
    def read_col_nam_col_len(path: str, file_name: str):
        column_lst = []
        try:
            with open(os.path.join(path, file_name)) as tab_file:
                header_col = tab_file.readline()
                column_lst = header_col.split('\t')
                data_col = tab_file.readline()
                data_lst = data_col.split('\t')
                if len(column_lst) < len(data_lst):
                    remaining_col = len(data_lst) - len(column_lst)
                    for num in range(remaining_col):
                        column_lst.append('Filler' + str(num))
                tab_file.close()

        except Exception as ex:
            print(ex.args)
            print(traceback.format_exc)
        return len(column_lst), column_lst

    @staticmethod
    def clean_up_non_printable(input_to_clean: str):
        regex_expression = "[\x20-\x7E]"
        input_to_clean = str(input_to_clean).replace('\r\n', "")
        input_to_clean = str(input_to_clean).replace('\n\r', "")
        input_to_clean = str(input_to_clean).replace('\r', "")
        input_to_clean = str(input_to_clean).replace('\n', "")
        input_to_clean = str(input_to_clean).replace('None', "")
        input_to_clean = str(input_to_clean).replace('\t', "")
        input_to_clean = str(input_to_clean).replace('"', "")
        input_to_clean = str(input_to_clean).replace(regex_expression, "")
        return input_to_clean

    @staticmethod
    def clean_up_parcel(input_to_clean: str):
        try:
            input_to_clean = str(input_to_clean).replace('-', "")
            input_to_clean = str(input_to_clean).replace('.', "")
        # input_to_clean = str(input_to_clean).replace(' ', "")
            return input_to_clean
        except Exception as ex:
            print(ex.args)

    def read_file(self, path: str, file_name: str):
        try:
            with open(os.path.join(path, file_name)) as data_file:
                for line_number, line in enumerate(data_file, 1):
                    if line_number <= 1:
                        header = line.split('\t')
                        col_lst = [[] for x in repeat(None, len(header))]

                    line_split = line.split('\t')
                    inc = 0
                    for data in line_split:
                        col_lst[inc].append(self.clean_up_non_printable(data))
                        inc += 1
            col_lst = [empty_lst for empty_lst in col_lst if empty_lst != []]
            return col_lst
        except Exception as ex:
            print(ex.args)

    def read_cols(self, path: str, file_name: str, col_values: List):
        try:
            col_lst = self.read_specific_cols_data(col_values, file_name, path)
            col_lst = [empty_lst for empty_lst in col_lst if self.is_list_empty(empty_lst)]
            return col_lst
        except Exception as ex:
            print(ex.args)

    @staticmethod
    def is_list_empty(col_lst: list):
        if len(col_lst) > 0:
            new_list = col_lst.copy()
            del new_list[0]
            if any(new_list):
                return True

    def read_unique_cols(self, path: str, file_name: str, col_values: List):
        try:
            col_lst = self.read_specific_cols_data(col_values, file_name, path)
            col_lst = [list(dict.fromkeys(empty_lst)) for empty_lst in col_lst if self.is_list_empty(empty_lst)]
            return col_lst
        except Exception as ex:
            print(ex.args)

    def read_specific_cols_data(self, col_values, file_name, path):
        col_pick_list = []
        col_lst = [[] for x in repeat(None, len(col_values))]
        with open(os.path.join(path, file_name)) as data_file:
            for line_number, line in enumerate(data_file, 1):
                if line_number <= 1:
                    header = line.split('\t')
                    inc = 1
                    for data in header:
                        for col_num in col_values:
                            if data == col_num:
                                col_pick_list.append(inc)
                        inc += 1
                    # continue
                split_line = line.split('\t')
                inc = 1
                index = 0
                for data in split_line:
                    if inc in col_pick_list:
                        col_lst[index].append(self.clean_up_non_printable(data))
                        index += 1
                    inc += 1
        return col_lst

    @staticmethod
    def check_zeros(lst: List):
        return [i for i, e in enumerate(lst) if e == '0' or e == 0]

    @staticmethod
    def check_empty_strings(lst: List):
        return [i for i, e in enumerate(lst) if str(e).strip() == '' or str(e).strip() is None]

    @staticmethod
    def contains_digit(lst: List):
        return [(i, x) for i, x in enumerate(lst, 1) if any(map(str.isdigit, x))]

    @staticmethod
    def max_unique_length_list(col_lst: List):
        col_lst = [list(dict.fromkeys(empty_lst)) for empty_lst in col_lst if any(empty_lst)]
        longest_list = max(col_lst, key=len)
        return longest_list

    @staticmethod
    def get_max_len_list(col_lst: list):
        new_list = copy.deepcopy(col_lst)
        unique_list = [list(dict.fromkeys(empty_lst)) for empty_lst in new_list if any(empty_lst)]
        longest_list = max(unique_list, key=len)
        final_list = [data for data in col_lst if data[0] == longest_list[0]]
        return final_list[0]

    @staticmethod
    def unique_col_list(col_lst: List):
        col_lst = [list(dict.fromkeys(empty_lst)) for empty_lst in col_lst if any(empty_lst)]
        return col_lst

    @staticmethod
    def remove_empty_list(col_lst: List):
        col_lst = [list(empty_lst) for empty_lst in col_lst if any(empty_lst)]
        return col_lst

    @staticmethod
    def create_reports(path: str, driver_lst: list, file_name: str):
        try:
            with open(os.path.join(path, file_name), 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(driver_lst)

        except Exception as ex:
            print(ex.args)

    def get_diablo_row_count(self, fips: str):
        edg_instance = Database.connect_diablo()
        row_count = edg_instance.fetch(self.app_obj.get_row_count().format(fips))
        return row_count

    def get_diablo_parcel_info(self, fips: str):
        edg_instance = Database.connect_diablo()
        parcel_info = edg_instance.fetch(self.app_obj.get_parcel_info().format(fips))
        orig_pclid = [self.clean_up_parcel(i[0]) for i in parcel_info]
        iris_seckey = [self.clean_up_parcel(i[1]) for i in parcel_info]
        return orig_pclid, iris_seckey

    def get_diablo_parcel_length(self, fips: str):
        edg_instance = Database.connect_diablo()
        parcel_length = edg_instance.fetch(self.app_obj.get_parcel_len().format(fips))
        diablo_pcl_len = [i[0] for i in parcel_length]
        return diablo_pcl_len

    @staticmethod
    def validate_file_name(file_name, comp_list):
        flag = True
        for exe_value in comp_list:
            if exe_value.lower() in file_name.lower():
                flag = False
        return flag

    @staticmethod
    def compare_diablo_data(diablo_pcl_info: list, diablo_pcl_len: list, col_lst: list):
        pass

    @staticmethod
    def list_comparison(result1, result2):
        a1 = numpy.array(result1)
        a2 = numpy.array(result2)
        diff1 = numpy.setdiff1d(a1, a2)
        log.info(f'Number of parcels added: {len(diff1)}')
        diff2 = numpy.setdiff1d(a2, a1)
        log.info(f'Number of parcels dropped: {len(diff2)}')
        return diff1, diff2

    @staticmethod
    def column_filter(data, file_name, parcel_lst, owner_name_lst, loc_add_lst, mail_add_lst, legal_lst, not_col):
        try:
            pcl_col = []
            legal_col = []
            loc_col = []
            mail_col = []
            owner_col = []
            parcel = False
            owner = False
            loc = False
            desc = False
            mail = False
            for col_name in data[2]:
                col = re.sub(r'[^A-Za-z]+', '', col_name.strip().lower())
                for (pcl, own, loc_add, mail_add, legal) in itertools.zip_longest(parcel_lst, owner_name_lst, loc_add_lst, mail_add_lst, legal_lst):
                    if pcl is not None and pcl in col and fuzz.token_sort_ratio(pcl, col) >= 75 and col not in not_col:
                        log.info(f'{file_name} -> Parcel: The matching score between {pcl} and {col}: {fuzz.token_sort_ratio(pcl, col)}')
                        parcel = True
                        pcl_col.append(col_name)
                        legal_col.append(col_name)
                    elif pcl is not None and pcl in col and fuzz.token_sort_ratio(pcl, col) < 75 and col not in not_col:
                        log.info(f'{file_name} -> Parcel: The matching score between {pcl} and {col}: {fuzz.token_sort_ratio(pcl, col)}')

                    if own is not None and own in col and fuzz.token_sort_ratio(own, col) > 40 and 'seller' not in col and 'buyer' not in col:
                        log.info(f'{file_name} -> Owner: The matching score between {own} and {col}: {fuzz.token_sort_ratio(own, col)}')
                        owner = True
                        owner_col.append(col_name)
                    elif own is not None and own in col and fuzz.token_sort_ratio(own, col) < 40 and 'seller' not in col and 'buyer' not in col:
                        log.info(f'{file_name} -> Owner: The matching score between {own} and {col}: {fuzz.token_sort_ratio(own, col)}')

                    if loc_add is not None and loc_add in col and fuzz.token_sort_ratio(loc_add, col) > 50 and 'seller' not in col and 'buyer' not in col:
                        log.info(f'{file_name} -> Location: The matching score between {loc_add} and {col}: {fuzz.token_sort_ratio(loc_add, col)}')
                        loc = True
                        loc_col.append(col_name)
                    elif loc_add is not None and loc_add in col and fuzz.token_sort_ratio(loc_add, col) < 50 and 'seller' not in col and 'buyer' not in col:
                        log.info(f'{file_name} -> Location: The matching score between {loc_add} and {col}: {fuzz.token_sort_ratio(loc_add, col)}')

                    if mail_add is not None and mail_add in col and fuzz.token_sort_ratio(mail_add, col) > 50 and 'seller' not in col and 'buyer' not in col:
                        log.info(f'{file_name} -> Mailing Address: The matching score between {mail_add} and {col}: {fuzz.token_sort_ratio(mail_add, col)}')
                        mail = True
                        mail_col.append(col_name)
                    elif mail_add is not None and mail_add in col and fuzz.token_sort_ratio(mail_add, col) > 50 and 'seller' not in col and 'buyer' not in col:
                        log.info(f'{file_name} -> Mailing Address: The matching score between {mail_add} and {col}: {fuzz.token_sort_ratio(mail_add, col)}')

                    if legal is not None and legal in col and fuzz.token_sort_ratio(legal, col) > 50 and 'sqft' not in col:
                        log.info(f'{file_name} -> Legal: The matching score between {legal} and {col}: {fuzz.token_sort_ratio(legal, col)}')
                        desc = True
                        legal_col.append(col_name)

            return desc, legal_col, loc, loc_col, mail, mail_col, owner, owner_col, parcel, pcl_col
        except Exception as ex:
            log.info(ex.args)
            log.info(traceback.format_exc)