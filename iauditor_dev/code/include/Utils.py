import os
from pathlib import Path
import config
from zipfile import ZipFile
import shutil
from datetime import datetime
import csv
import logging
from typing import Optional, List, Dict
import time
import psutil
import re
import polars as pl
import numpy as np


class Utils:
    LOG_FILE = '/tmp/my_utils.log'
    LOG_TO_FILE = False

    LOG_LEVEL_KEY = 'log_level'

    # log types
    LOG_TYPE_FATAL = 'fatal'
    LOG_TYPE_ERROR = 'error'
    LOG_TYPE_WARN = 'warn'
    LOG_TYPE_INFO = 'info'
    LOG_TYPE_DEBUG = 'debug'

    TEMP_FILE_EXT = '.tmp_transformed.dat'

    DEFAULT_DIGITS_FOR_NUMBER_FROM_TEXT_FUNCTION = 4
    NEW_COLUMN_NAME_KEY = 'new_column_name'
    TRANSFORM_COLUMN_VALUE_TO_NUMBER_KEY = 'transform_value_to_number'  # signal to call getNumberFromText()
    TRANSFORM_COLUMN_VALUE_TO_NUMBER_EXTRA_PARAM_KEY = 'transform_value_to_number_extra_param'  # param to pass to getNumberFromText()

    ORIGINAL_LINES_KEY = 'original_lines'
    NEW_LINES_KEY = 'new_lines'
    SKIPPED_LINES_KEY = 'skipped_lines'
    ORIGINAL_FILE_KEY = 'original_file'
    TRANSFORMED_FILE_KEY = 'tmp_file'
    SUCCESS_KEY = 'success'

    DATE_KEY = 'date'
    TABLE_NAME_KEY = 'table_name'
    DATA_FILE_KEY = 'data_file'
    RUN_TIME_KEY = '__RUN_TIME__'
    BAD_FILE_KEY = 'bad_file'
    LOG_FILE_KEY = 'log_file'
    CTL_FILE_KEY = 'ctl_file'
    BASE_FILE_NO_EXT_KEY = 'base_file_no_ext'
    INTERMEDIATE_DELIMITER = '^^__^^__$$'

    ZIP_EXT = '.zip'
    DAT_EXT = '.dat'
    CSV_EXT = '.csv'

    mAryFilesColumnsCount = {}
    mArySelectDataDetail = []
    mIntCurrentLineCount = 0

    @staticmethod
    def save_file(l_str_full_file_path, l_str_data, l_bln_to_file_append=False):
        l_str_dest_dir = os.path.dirname(l_str_full_file_path)
        if Utils.create_dir(l_str_dest_dir):
            mode = 'a' if l_bln_to_file_append else 'w'
            try:
                with open(l_str_full_file_path, mode, encoding='utf-8') as file:
                    file.write(l_str_data)
                return True
            except Exception as e:
                print(f"Error: {e}")
                return False
        return False

    @staticmethod
    def delete_file(l_str_full_file_path):
        if not os.path.exists(l_str_full_file_path) or (
                os.path.exists(l_str_full_file_path) and not os.path.isfile(l_str_full_file_path)):
            return False
        os.remove(l_str_full_file_path)
        return True

    @staticmethod
    def is_file_valid(l_str_full_file_path):
        return os.path.exists(l_str_full_file_path) and os.path.isfile(l_str_full_file_path)

    @staticmethod
    def create_dir(directory):
        exists = False
        if os.path.exists(directory):
            exists = True
        else:
            os.makedirs(directory, exist_ok=True)
            exists = True
        return exists

    @staticmethod
    def unzip_file(l_str_zip_file_full_path, l_str_unzip_dir_full_path):
        if not l_str_zip_file_full_path or not os.path.exists(l_str_zip_file_full_path):
            raise Exception('Invalid zip file: ' + l_str_zip_file_full_path)

        if not Utils.create_dir(l_str_unzip_dir_full_path):
            raise Exception('Could not create unzip dir: ' + l_str_unzip_dir_full_path)

        with ZipFile(l_str_zip_file_full_path, 'r') as zip_ref:
            zip_ref.extractall(l_str_unzip_dir_full_path)
            return True

    @staticmethod
    def get_new_files(l_str_dir_path, l_str_extension):
        if not l_str_dir_path or not os.path.exists(l_str_dir_path):
            raise Exception('Invalid dir: ' + l_str_dir_path)

        l_ary_files_list = []

        for file in os.listdir(l_str_dir_path):
            if file.endswith(l_str_extension) and not file.startswith('.'):
                l_ary_files_list.append(os.path.join(l_str_dir_path, file))

        return l_ary_files_list

    GENERAL_CONFIGURATION = {}

    @staticmethod
    def relocate_file(l_str_full_file_path, l_str_dest_full_file_path, l_bln_just_copy=False):
        l_str_dest_dir = os.path.dirname(l_str_dest_full_file_path)
        if Utils.create_dir(l_str_dest_dir) and os.path.exists(l_str_full_file_path) and os.path.isfile(
                l_str_full_file_path):
            if l_bln_just_copy:  # copy
                shutil.copy(l_str_full_file_path, l_str_dest_full_file_path)
            else:  # move
                shutil.move(l_str_full_file_path, l_str_dest_full_file_path)
            return True
        return False

    @staticmethod
    def log(msg, l_bln_print_to_screen=True, l_bln_die=False, l_str_log_type=LOG_TYPE_INFO, l_str_log_file=LOG_FILE,
            l_bln_log_to_file=LOG_TO_FILE):
        if l_str_log_type in Utils.GENERAL_CONFIGURATION.get(Utils.LOG_LEVEL_KEY, {}) and not \
                Utils.GENERAL_CONFIGURATION[Utils.LOG_LEVEL_KEY][l_str_log_type]:
            return

        l_bln_logged = False
        try:
            # convert array to string
            if isinstance(msg, (list, dict)):
                msg = str(msg)
            msg += "\n"
            if l_bln_print_to_screen:
                print(msg)

            if l_bln_log_to_file:
                os.makedirs(os.path.dirname(l_str_log_file), exist_ok=True)
                with open(l_str_log_file, 'a', encoding='utf-8') as f:
                    f.write(msg)
                l_bln_logged = True
        except Exception as ex:
            print(f"ERROR logging to ADP file {str(ex)} \nlog msg: {msg}\n\n")
        if l_bln_die:
            exit(1)
        return l_bln_logged

    @staticmethod
    def get_number_from_text(text, l_int_digits_to_extract=DEFAULT_DIGITS_FOR_NUMBER_FROM_TEXT_FUNCTION):
        matches = re.findall(r'\d{' + str(l_int_digits_to_extract) + '}', text)
        if matches and matches[0] in text:
            return matches[0]
        else:
            return text

    @staticmethod
    def get_bz2_file_meta_data(l_str_filename, l_bln_remove_prefix=True):
        Utils.log(f"Start - getting meta data for {l_str_filename}")

        if l_bln_remove_prefix:
            l_str_filename = os.path.basename(l_str_filename)

        l_str_date = Utils.get_number_from_text(l_str_filename, 8)
        l_str_table_name = l_str_filename.replace('.dat.bz2', '')
        l_str_table_name = l_str_table_name.replace('.dat', '')
        l_str_table_name = l_str_table_name.replace('_' + l_str_date, '')
        l_str_data_file_name = l_str_table_name.replace('ft_', '').lower()
        l_str_data_file_name = l_str_data_file_name.replace('_stage', '')

        l_ary_meta_data = {
            Utils.ORIGINAL_FILE_KEY: l_str_filename,
            Utils.DATE_KEY: l_str_date,
            Utils.TABLE_NAME_KEY: l_str_table_name,
            Utils.DATA_FILE_KEY: l_str_data_file_name + '.dat',
            Utils.BAD_FILE_KEY: l_str_data_file_name + '_' + Utils.RUN_TIME_KEY + '.bad',
            Utils.LOG_FILE_KEY: l_str_data_file_name + '_' + Utils.RUN_TIME_KEY + '.log',
        }

        Utils.log(f"Finished - getting meta data for {l_str_filename} : {l_ary_meta_data}")

        return l_ary_meta_data

    @staticmethod
    def get_data_file_meta_data(l_str_filename_full_path):
        Utils.log(f"Start - getting meta data for {l_str_filename_full_path}")

        l_str_filename = os.path.basename(l_str_filename_full_path)
        l_str_log_base_dir = os.path.dirname(os.path.realpath(__file__)) + '/../../log/'
        l_str_ctl_base_dir = os.path.dirname(os.path.realpath(__file__)) + '/../../ctl/'

        l_str_table_prefix = config.MY_TABLES_PREFIX
        l_str_date = datetime.now().strftime('%Y%m%d_%H%M%S')
        l_str_table_name = l_str_filename.replace('.dat', '')
        l_str_table_name = l_str_table_prefix + l_str_table_name.upper()

        l_str_data_file_name = l_str_table_name.replace(l_str_table_prefix, '').lower()

        l_ary_meta_data = {
            Utils.ORIGINAL_FILE_KEY: l_str_filename_full_path,
            Utils.DATE_KEY: l_str_date,
            Utils.TABLE_NAME_KEY: l_str_table_name,
            Utils.BASE_FILE_NO_EXT_KEY: l_str_data_file_name,
            Utils.DATA_FILE_KEY: l_str_filename_full_path,
            Utils.BAD_FILE_KEY: l_str_log_base_dir + l_str_data_file_name + '_' + l_str_date + '.bad',
            Utils.LOG_FILE_KEY: l_str_log_base_dir + l_str_data_file_name + '_' + l_str_date + '.log',
            Utils.CTL_FILE_KEY: l_str_ctl_base_dir + l_str_table_name + '.ctl',
        }

        Utils.log(f"Finished - getting meta data for {l_str_filename_full_path} : {l_ary_meta_data}")

        return l_ary_meta_data

    # Apply data trimming rule
    @staticmethod
    def apply_data_replacement_rule(l_ary_data, l_str_base_file_name):
        l_bln_has_active_rule = l_str_base_file_name in config.EXTRA_RULES['data_replacement_rules'] and \
                                config.EXTRA_RULES['data_replacement_rules'][l_str_base_file_name].get('active', False)

        # only move forward if we have an active rule for this file
        if l_bln_has_active_rule:
            l_ary_columns_to_process = config.EXTRA_RULES['data_replacement_rules'][l_str_base_file_name].get('columns',
                                                                                                             [])

            # column count is more than expected
            if l_ary_columns_to_process:
                for l_str_column_details in l_ary_columns_to_process:
                    if 'column_position' in l_str_column_details and l_str_column_details['column_position'] and \
                            'replacement_values' in l_str_column_details and l_str_column_details['replacement_values']:
                        if isinstance(l_str_column_details['column_position'], list):
                            for l_int_index in l_str_column_details['column_position']:
                                l_int_index -= 1  # make zero-based
                                # replace
                                for key, value in l_str_column_details['replacement_values'].items():
                                    l_ary_data[l_int_index] = l_ary_data[l_int_index].replace(key, value)
                        else:
                            l_int_index = l_str_column_details['column_position'] - 1  # make zero-based
                            # replace
                            for key, value in l_str_column_details['replacement_values'].items():
                                l_ary_data[l_int_index] = l_ary_data[l_int_index].replace(key, value)
        return l_ary_data

    # Apply Data Truncating Rule
    @staticmethod
    def apply_data_truncating_rule(l_ary_data, l_str_base_file_name):
        l_bln_has_active_rule = l_str_base_file_name in config.EXTRA_RULES['data_truncating_rules'] and \
                                config.EXTRA_RULES['data_truncating_rules'][l_str_base_file_name].get('active', False)

        # only move forward if we have an active rule for this file
        if l_bln_has_active_rule:
            l_ary_columns_to_process = config.EXTRA_RULES['data_truncating_rules'][l_str_base_file_name].get('columns',
                                                                                                            [])

            # column acount is more than expected
            if l_ary_columns_to_process:
                for l_str_column_details in l_ary_columns_to_process:
                    if 'column_position' in l_str_column_details and l_str_column_details['column_position'] and \
                            'max_length' in l_str_column_details and l_str_column_details['max_length']:
                        l_int_max_length = l_str_column_details['max_length']
                        if isinstance(l_str_column_details['column_position'], list):
                            for l_int_the_index in l_str_column_details['column_position']:
                                l_int_index = l_int_the_index - 1  # make zero-based
                                if l_int_index < len(l_ary_data) and len(l_ary_data[l_int_index]) > l_int_max_length:
                                    l_ary_data[l_int_index] = l_ary_data[l_int_index][:l_int_max_length].strip()
                        else:
                            l_int_index = l_str_column_details['column_position'] - 1  # make zero-based
                            if l_int_index < len(l_ary_data) and len(l_ary_data[l_int_index]) > l_int_max_length:
                                l_ary_data[l_int_index] = l_ary_data[l_int_index][:l_int_max_length].strip()
        return l_ary_data

    """ 
    Apply Data Trimming Rule
    
    :return array
    
    """

    @staticmethod
    def apply_data_trimming_rule(l_ary_data, l_str_base_file_name):
        l_bln_has_active_trimming_rule = l_str_base_file_name in config.EXTRA_RULES['data_trimming_rules'] and \
                                         config.EXTRA_RULES['data_trimming_rules'][l_str_base_file_name].get('active', False)

        # only move forward if we have an active rule for this file
        if l_bln_has_active_trimming_rule:
            l_int_expected_columns_count = config.EXTRA_RULES['data_trimming_rules'][l_str_base_file_name].get(
                'expected_columns_count', 0)
            l_ary_columns_positions_to_remove = config.EXTRA_RULES['data_trimming_rules'][l_str_base_file_name].get(
                'remove_column_in_positions', [])

            # column count is more than expected
            if l_int_expected_columns_count and l_ary_columns_positions_to_remove and len(
                    l_ary_data) > l_int_expected_columns_count:
                for l_int_index_to_remove in l_ary_columns_positions_to_remove:
                    l_int_index_to_remove -= 1  # make zero-based

                    # remove unexpected columns
                    if l_int_index_to_remove < len(l_ary_data):
                        del l_ary_data[l_int_index_to_remove]
        return l_ary_data

    """
    Parse File and Save to New File
    
    :param: string l_str_file_full_path
    """

    @staticmethod
    def parse_file_and_save_to_new_file(l_str_full_file_path: str, l_chr_file_delimiter: str = ',',
                                        l_int_max_records: Optional[int] = None,
                                        l_bln_append_header_to_file: bool = True,
                                        l_ary_append_data: Dict = {}, l_str_enclosure: str = '"',
                                        l_str_processed_ready_files_location: Optional[str] = None,
                                        l_chr_dest_file_delimiter: Optional[str] = None,
                                        l_bln_always_append_to_dest_file: bool = False,
                                        l_ary_transformer: List = []) -> Dict:

        l_bln_only_create_temp_file = True

        logging.info(f"{__name__}: Starting parsing file: {l_str_full_file_path}")
        is_successful = False

        if not l_str_full_file_path or not os.path.exists(l_str_full_file_path):
            msg = f'Invalid CSV file: {l_str_full_file_path}'
            logging.warning(f"{__name__}: {msg}")
            raise Exception(msg)

        """
        temp file name
        l_str_full_temp_file_name = l_str_full_file_path + Utils.TEMP_FILE_EXT
        """
        l_str_base_file_name = os.path.basename(l_str_full_file_path)
        l_str_full_temp_file_name = os.path.join(l_str_processed_ready_files_location, l_str_base_file_name)

        l_ary_data = {}
        l_ary_stats = []
        l_int_lines_written = l_int_skipped_lines = 0

        """
        Testing what files are coming to know what to expect
        """
        print(f"l_str_full_file_path: {l_str_full_file_path}")
        print(f"l_str_full_temp_file_name: {l_str_full_temp_file_name}")
        print(f"l_str_processed_ready_files_location: {l_str_processed_ready_files_location}")


        # Actions.dat Processing
        if '/actions.dat' in l_str_full_file_path:
            Utils.log(f"Found actions.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_actions(l_str_full_file_path, l_str_full_temp_file_name)

        # inspection_items.dat Processing
        if '/inspection_items.dat' in l_str_full_file_path:
            Utils.log(f"Found inspection_items.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_inspection_items(l_str_full_file_path, l_str_full_temp_file_name)

        # action_assignees.dat Processing
        if '/action_assignees.dat' in l_str_full_file_path:
            Utils.log(f"Found action_assignees.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_action_assignees(l_str_full_file_path, l_str_full_temp_file_name)

        # group_users.dat Processing
        if '/group_users.dat' in l_str_full_file_path:
            Utils.log(f"Found group_users.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_group_users(l_str_full_file_path, l_str_full_temp_file_name)

        if '/groups.dat' in l_str_full_file_path:
            Utils.log(f"Found groups.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_groups(l_str_full_file_path, l_str_full_temp_file_name)

        if '/inspections.dat' in l_str_full_file_path:
            Utils.log(f"Found inspections.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_inspections(l_str_full_file_path, l_str_full_temp_file_name)

        if '/schedule_assignees.dat' in l_str_full_file_path:
            Utils.log(f"Found schedule_assignees.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_schedule_assignees(l_str_full_file_path, l_str_full_temp_file_name)

        # schedules.dat Processing
        if '/schedules.dat' in l_str_full_file_path:
            Utils.log(f"Found schedules.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_schedules(l_str_full_file_path, l_str_full_temp_file_name)

        # sites.dat Processing
        if '/sites.dat' in l_str_full_file_path:
            Utils.log(f"Found sites.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_sites(l_str_full_file_path, l_str_full_temp_file_name)

        # template_permissions.dat Processing
        if '/template_permissions.dat' in l_str_full_file_path:
            Utils.log(f"Found template_permissions.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_template_permissions(l_str_full_file_path, l_str_full_temp_file_name)

        # templates.dat Processing
        if '/templates.dat' in l_str_full_file_path:
            Utils.log(f"Found templates.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_templates(l_str_full_file_path, l_str_full_temp_file_name)

        # users.dat Processing
        if '/users.dat' in l_str_full_file_path:
            Utils.log(f"Found users.dat: {l_str_full_file_path}", l_bln_print_to_screen=True, l_bln_log_to_file=True)
            is_successful = Utils.transform_users(l_str_full_file_path, l_str_full_temp_file_name)

        i = 100
        return {
            Utils.ORIGINAL_LINES_KEY: i,
            Utils.NEW_LINES_KEY: l_int_lines_written,
            Utils.SKIPPED_LINES_KEY: l_int_skipped_lines,
            Utils.ORIGINAL_FILE_KEY: l_str_full_file_path,
            Utils.TRANSFORMED_FILE_KEY: l_str_full_temp_file_name,
            Utils.SUCCESS_KEY: is_successful
        }

    """
    Parses CSV File
    :param string l_str_full_file_path - full path of file to be parsed.
    :return array - list of file content in array or array of number of lines successfully written to file
    """

    @staticmethod
    def parse_file(l_str_full_file_path: str, l_chr_file_delimiter: str = ',', l_int_max_records: Optional[int] = None,
                   l_bln_escape: bool = True, l_bln_only_create_temp_file: bool = False,
                   l_bln_append_header_to_file: bool = True, l_ary_transformation_info: Dict = {},
                   l_ary_append_data: Dict = {}, l_str_enclosure: str = '"', l_bln_insert_test_headers: bool = False,
                   l_str_processed_ready_files_location: Optional[str] = None,
                   l_chr_dest_file_delimiter: Optional[str] = None) -> Dict:

        logging.info(f"{__name__}: Starting parsing file: {l_str_full_file_path}")

        if not l_str_full_file_path or not os.path.exists(l_str_full_file_path):
            msg = f'Invalid CSV file: {l_str_full_file_path}'
            logging.warning(f"{__name__}: {msg}")
            raise Exception(msg)

        l_str_full_temp_file_name = os.path.join(l_str_processed_ready_files_location,
                                                 os.path.basename(l_str_full_file_path))

        l_chr_dest_file_delimiter = l_chr_dest_file_delimiter if l_chr_dest_file_delimiter else l_chr_file_delimiter

        # testing list vs dict
        l_ary_data = {}
        l_ary_stats = {}
        l_int_lines_written = l_int_skipped_lines = 0

        with open(l_str_full_file_path, "r", encoding='utf-8') as h:
            l_ary_headers = []
            l_ary_mapped_data = {}
            data = []
            i = 0
            l_int_records_count = 0
            l_bln_appended_header_to_file_already = False
            l_bln_skipped_appending_header_to_file_already = False
            l_ary_column_reverse_mapping = {}  # key = index of original column, value = name of original column

            reader = csv.reader(h, delimiter=l_chr_file_delimiter, quotechar=l_str_enclosure)

            for data in reader:
                i += 1
                l_str_skip_test = ''.join(data).strip()

                if not l_str_skip_test:  # skip empty lines
                    l_int_skipped_lines += 1
                    continue

                if i < 2:
                    # transform header if needed
                    if l_ary_transformation_info:
                        for key, value in enumerate(data):
                            value = value.strip()
                            # replace column name, if instructed
                            if value in l_ary_transformation_info and 'new_column_name' in l_ary_transformation_info[value]:
                                data[key] = l_ary_transformation_info[value]['new_column_name']
                                # track what key this new column belongs to for later use
                                l_ary_column_reverse_mapping[key] = value

                    if l_bln_insert_test_headers:
                        for key, value in enumerate(data):
                            data[key] = 'TEST_HEADER' + str(key)

                    # append additional headers
                    if l_ary_append_data:
                        data.extend(list(l_ary_append_data.keys()))

                    l_ary_headers = data  # store headers
                    continue

                # add header to file
                if not l_bln_appended_header_to_file_already and l_bln_only_create_temp_file and l_bln_append_header_to_file:
                    line = l_chr_dest_file_delimiter.join(l_ary_headers) + "\n"

                    if Utils.save_file(l_str_full_temp_file_name, line):
                        l_int_lines_written += 1
                        l_bln_appended_header_to_file_already = True
                elif not l_bln_skipped_appending_header_to_file_already and l_bln_only_create_temp_file:
                    l_int_skipped_lines += 1
                    l_bln_skipped_appending_header_to_file_already = True

                # l_ary_mapped_data = {}
                # map each field to its corresponding header, so we have an array that clearly identifies each field for later processing
                for key, value in enumerate(data):
                    value = value.strip()

                    # transform, if necessary
                    if key in l_ary_column_reverse_mapping and 'transform_column_value_to_number' in \
                            l_ary_transformation_info[l_ary_column_reverse_mapping[key]]:
                        # number of digits to extract
                        l_int_number_of_digits_to_extract = Utils.DEFAULT_DIGITS_FOR_NUMBER_FROM_TEXT_FUNCTION

                        if 'transform_column_value_to_number' in l_ary_transformation_info[l_ary_headers[key]]:
                            l_int_number_of_digits_to_extract = l_ary_transformation_info[l_ary_headers[key]][
                                'transform_column_value_to_number']
                        elif 'transform_column_value_to_number' in l_ary_transformation_info[l_ary_column_reverse_mapping[key]]:
                            l_int_number_of_digits_to_extract = \
                                l_ary_transformation_info[l_ary_column_reverse_mapping[key]][
                                    'transform_column_value_to_number']

                        value = Utils.get_number_from_text(value, l_int_number_of_digits_to_extract)

                    l_ary_mapped_data[l_ary_headers[key]] = value if not l_bln_escape else value.replace('"', '\\"')

                # append additional values
                if l_ary_append_data:
                    l_ary_mapped_data.update(l_ary_append_data)

                # add line to file
                if l_bln_only_create_temp_file:
                    line = l_chr_dest_file_delimiter.join(list(l_ary_mapped_data.values())) + "\n"

                    l_bln_append_to_file = l_int_lines_written > 0
                    if Utils.save_file(l_str_full_temp_file_name, line, l_bln_append_to_file):
                        l_int_lines_written += 1
                        l_bln_appended_header_to_file_already = True

                l_int_records_count += 1
                if l_int_max_records and l_int_records_count > l_int_max_records:
                    break

                if not l_bln_only_create_temp_file:
                    l_ary_data.update(l_ary_mapped_data)

        if not l_bln_only_create_temp_file:
            logging.info(f"{__name__}: Finished parsing file: {l_str_full_file_path}. Found {len(l_ary_data)} rows")
            return l_ary_data
        else:
            logging.info(f"{__name__}: Finished parsing file: {l_str_full_file_path}. Found {len(l_ary_data)} rows")
            return {
                Utils.ORIGINAL_LINES_KEY: i,
                Utils.NEW_LINES_KEY: l_int_lines_written,
                Utils.SKIPPED_LINES_KEY: l_int_skipped_lines,
                Utils.ORIGINAL_FILE_KEY: l_str_full_file_path,
                Utils.TRANSFORMED_FILE_KEY: l_str_full_temp_file_name,
                Utils.SUCCESS_KEY: i == (l_int_lines_written + l_int_skipped_lines)
            }

    """
    takes csvfile for inspection_items, creates dataframe, and returns a transformed dataframe
    """
    @staticmethod
    def transform_inspection_items(csvfile, processed_path):
        try:
            inspection_items_df = pl.read_csv(csvfile, dtypes=config.inspection_items_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        # Transformations needed for inspection_items
        inspection_items_transformed = inspection_items_df.with_columns([
            # Date formatting, remove extra precision
            pl.col("created_at").dt.strftime("%Y-%m-%d").alias("created_at"),  # Date formatting
            pl.col("modified_at").dt.strftime("%Y-%m-%d").alias("modified_at"),  # Date formatting
            pl.col("exported_at").dt.strftime("%Y-%m-%d").alias("exported_at"),  # Date formatting

            # Boolean transformation for is_failed_response, mandatory, and inactive columns
            pl.col("is_failed_response").str.slice(0, 1).alias("is_failed_response"),  # Slice first char
            pl.col("mandatory").str.slice(0, 1).alias("mandatory"),  # Slice first char
            pl.col("inactive").str.slice(0, 1).alias("inactive"),  # Slice first char

            # Truncate float to int, only need integer values
            pl.col("score_percentage").cast(pl.Int64).alias("score_percentage"),  # Truncate float to int
            pl.col("combined_score").cast(pl.Int64).alias("combined_score"),  # Truncate float to int
            pl.col("combined_score_percentage").cast(pl.Int64).alias("combined_score_percentage"),  # Truncate float to int
            pl.col("score").cast(pl.Int64).alias("score"),  # Truncate float to int

            # Media_ids transformation, truncate to 1000 characters
            pl.col("media_ids").str.slice(0, 1000).alias("media_ids")  # Slice first 1000 chars
        ])

        inspection_items_transformed.write_csv(processed_path, separator='\t')
        return True

    @staticmethod
    def transform_action_assignees(csvfile, processed_path):
        try:
            action_assignees_df = pl.read_csv(csvfile, dtypes=config.action_assignees_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        action_assignees_transformed = action_assignees_df.with_columns([

            # Date formatting, removing extra precision
            pl.col("modified_at").dt.strftime("%Y-%m-%d").alias("modified_at"),  # Date formatting
            pl.col("exported_at").dt.strftime("%Y-%m-%d").alias("exported_at"),  # Date formatting

            # truncate to 64 characters
            pl.col("id").str.slice(0, 64).alias("id")  # Slice first 1000 chars
        ])

        action_assignees_transformed.write_csv(processed_path, separator='\t')
        return True

    @staticmethod
    def transform_actions(csvfile, processed_path):
        Utils.remove_newlines_in_quotes(csvfile)
        try:
            actions_df = pl.read_csv(csvfile, dtypes=config.actions_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        actions_transformed = actions_df.with_columns([
            pl.col('due_date').dt.strftime("%Y-%m-%d").alias("due_date"),
            pl.col('created_at').dt.strftime("%Y-%m-%d").alias("created_at"),
            pl.col('modified_at').dt.strftime("%Y-%m-%d").alias("modified_at"),
            pl.col('exported_at').dt.strftime("%Y-%m-%d").alias("exported_at"),
            pl.col('completed_at').dt.strftime("%Y-%m-%d").alias("completed_at")
        ])

        actions_transformed.write_csv(processed_path, separator='\t')
        return True

    @staticmethod
    def transform_group_users(csvfile, processed_path):
        try:
            group_users_df = pl.read_csv(csvfile, dtypes=config.group_users_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        group_users_transformed = group_users_df.with_columns([

            # Date formatting, removing extra precision from DateTime columns
            pl.col("exported_at").dt.strftime("%Y-%m-%d").alias("exported_at"),  # Date formatting
        ])

        group_users_transformed.write_csv(processed_path, separator='\t')
        return True

    @staticmethod
    def transform_groups(csvfile, processed_path):
        try:
            groups_df = pl.read_csv(csvfile, dtypes=config.groups_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        groups_transformed = groups_df.with_columns([

            # Date formatting, removing extra precision from DateTime columns
            pl.col("exported_at").dt.strftime("%Y-%m-%d").alias("exported_at"),  # Date formatting
        ])

        groups_transformed.write_csv(processed_path, separator='\t')
        return True

    @staticmethod
    def transform_inspections(csvfile, processed_path):
        Utils.remove_newlines_in_quotes(csvfile)
        try:
            inspections_df = pl.read_csv(csvfile, dtypes=config.inspections_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        inspections_transformed = inspections_df.with_columns([

            # Date formatting, removing extra precision from DateTime columns
            pl.col("date_started").dt.strftime("%Y-%m-%d").alias("date_started"),  # Date formatting
            pl.col("date_completed").dt.strftime("%Y-%m-%d").alias("date_completed"),  # Date formatting
            pl.col("date_modified").dt.strftime("%Y-%m-%d").alias("date_modified"),  # Date formatting
            pl.col("created_at").dt.strftime("%Y-%m-%d").alias("created_at"),  # Date formatting
            pl.col("modified_at").dt.strftime("%Y-%m-%d").alias("modified_at"),  # Date formatting
            pl.col("exported_at").dt.strftime("%Y-%m-%d").alias("exported_at"),  # Date formatting
            pl.col("conducted_on").dt.strftime("%Y-%m-%d").alias("conducted_on"),  # Date formatting

            pl.col("score").cast(pl.Int64).alias("score"),  # Truncate float to int
            pl.col("max_score").cast(pl.Int64).alias("max_score"),  # Truncate float to int
            pl.col("duration").cast(pl.Int64).alias("duration"),  # Truncate float to int

            pl.col("archived").str.slice(0, 1).alias("archived")  # Slice first char
        ])

        inspections_transformed.write_csv(processed_path, separator='\t')
        return True


    """
    Remove newlines within quoted strings in a file. 
    Ensures each record is on a single line.
    """
    @staticmethod
    def remove_newlines_in_quotes(input_filename):
        # Read the entire file into a single string
        with open(input_filename, 'r', encoding='utf-8') as file:
            content = file.read()

        # Function to replace newlines within quotes
        def replace_newlines(match):
            return match.group(0).replace('\n', ' ')  # Replace newline with space or other delimiter

        # Use regex to find and modify all quoted substrings
        modified_content = re.sub(r'"[^"]*"', replace_newlines, content)

        # Write the modified content back to a new file
        with open(input_filename, 'w', encoding='utf-8') as file:
            file.write(modified_content)

    @staticmethod
    def transform_schedule_assignees(csvfile, processed_path):
        try:
            schedule_assignees_df = pl.read_csv(csvfile, dtypes=config.schedule_assignees_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        schedule_assignees_transformed = schedule_assignees_df.with_columns([

            # Date formatting, removing extra precision from DateTime columns
            pl.col("exported_at").dt.strftime("%Y-%m-%d").alias("exported_at"),  # Date formatting
        ])

        schedule_assignees_transformed.write_csv(processed_path, separator='\t')
        return True

    @staticmethod
    def transform_schedules(csvfile, processed_path):
        try:
            schedules_df = pl.read_csv(csvfile, dtypes=config.schedules_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        schedules_transformed = schedules_df.with_columns([

            # Date formatting, removing extra precision from DateTime columns
            pl.col("modified_at").dt.strftime("%Y-%m-%d").alias("modified_at"),  # Date formatting
            pl.col("exported_at").dt.strftime("%Y-%m-%d").alias("exported_at"),  # Date formatting
            pl.col("from_date").dt.strftime("%Y-%m-%d").alias("from_date"),  # Date formatting
            pl.col("to_date").dt.strftime("%Y-%m-%d").alias("to_date"),  # Date formatting

            # Truncate to first letter
            pl.col("all_must_complete").str.slice(0, 1).alias("all_must_complete"),  # Slice first char
            pl.col("can_late_submit").str.slice(0, 1).alias("can_late_submit")  # Slice first char
        ])

        schedules_transformed.write_csv(processed_path, separator='\t')
        return True

    @staticmethod
    def transform_sites(csvfile, processed_path):
        try:
            sites_df = pl.read_csv(csvfile, dtypes=config.sites_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        sites_transformed = sites_df.with_columns([

            # Date formatting, removing extra precision from DateTime columns
            pl.col("exported_at").dt.strftime("%Y-%m-%d").alias("exported_at"),  # Date formatting

            # Truncate to first letter
            pl.col("deleted").str.slice(0, 1).alias("deleted"),  # Slice first char
        ])

        sites_transformed.write_csv(processed_path, separator='\t')
        return True

    @staticmethod
    def transform_template_permissions(csvfile, processed_path):
        try:
            template_permissions_df = pl.read_csv(csvfile, dtypes=config.template_permissions_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        template_permissions_transformed = template_permissions_df.with_columns([

            # Truncate to 64 characters
            pl.col("permission_id").str.slice(0, 64).alias("permission_id"),  # Slice first char
        ])

        template_permissions_transformed.write_csv(processed_path, separator='\t')
        return True

    @staticmethod
    def transform_templates(csvfile, processed_path):
        try:
            templates_df = pl.read_csv(csvfile, dtypes=config.templates_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        templates_transformed = templates_df.with_columns([

            # Date formatting, removing extra precision from DateTime columns
            pl.col("created_at").dt.strftime("%Y-%m-%d").alias("created_at"),  # Date formatting
            pl.col("modified_at").dt.strftime("%Y-%m-%d").alias("modified_at"),  # Date formatting
            pl.col("exported_at").dt.strftime("%Y-%m-%d").alias("exported_at"),  # Date formatting

            # Truncate to 1 character
            pl.col("archived").str.slice(0, 1).alias("archived"),  # Slice first char

        ])

        templates_transformed.write_csv(processed_path, separator='\t')
        return True

    @staticmethod
    def transform_users(csvfile, processed_path):
        try:
            users_df = pl.read_csv(csvfile, dtypes=config.users_data_types)
        except Exception as e:
            print(f'Unable to read {csvfile} - error {e}')
            return False

        users_transformed = users_df.with_columns([

            # Date formatting, removing extra precision from DateTime columns
            pl.col("last_seen_at").dt.strftime("%Y-%m-%d").alias("last_seen_at"),  # Date formatting
            pl.col("exported_at").dt.strftime("%Y-%m-%d").alias("exported_at"),  # Date formatting

            # Truncate to 1 character
            pl.col("active").str.slice(0, 1).alias("active"),  # Slice first char

        ])

        users_transformed.write_csv(processed_path, separator='\t')
        return True

    """
    Gets the running duration
    :param int start_time
    :param bool l_bln_include_memory_usage
    
    :return string
    """

    @staticmethod
    def get_my_duration(start_time, include_memory_usage=False):
        end_time = time.time()

        timediff = end_time - start_time
        duration_secs = "{:.4f}".format(timediff)
        duration_mins = "{:.4f}".format(timediff / 60)
        duration_hrs = "{:.4f}".format(timediff / 60 / 60)

        duration_string = f"Duration: [{duration_secs} secs({duration_mins} mins, {duration_hrs} hrs)]"
        if include_memory_usage:
            duration_string += '. ' + Utils.get_memory_usage()

        return duration_string

    """
    Obtains script memory usage
    :param bool l_bln_pretty_format
    :return array/string
    """

    @staticmethod
    def get_memory_usage(pretty_format=True):
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()

        mem_usage = mem_info.rss  # in bytes
        mem_peak = mem_info.vms  # in bytes

        factor = 1048576  # bytes to MB
        units = 'MB'

        if pretty_format:
            return f'Memory: Current {round(mem_usage / factor)}{units}. Peak {round(mem_peak / factor)}{units}'
        else:
            return {'current': round(mem_usage / factor), 'peak': round(mem_peak / factor)}

    """
    Checks if running in test mode or not
    
    :return bool
    """

    @staticmethod
    def in_test_mode():
        return os.getenv('TEST_MODE', False)

    """
    Checks if we should move zip file after processing or not
    :return bool
    """

    @staticmethod
    def should_move_source_file_after_processing():
        return os.getenv('MOVE_SOURCE_FILE_AFTER_PROCESSING', False)
