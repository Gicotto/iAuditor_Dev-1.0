import os
import sys
from pathlib import Path
import subprocess
import time
from shutil import copy2

from config import *
from Utils import *
from SnowflakeDataImporter import *

DATA_FILES_BASE_DIR = Path(__file__).parent / '../../files/'


class MainProcessor:
    # Defining class constants (equivalent to const in PHP)
    PROCESSING_FILES_LOCATION_KEY = 'processing/'
    PROCESSING_READY_FILES_LOCATION_KEY = 'processing/ready/'
    PROCESSED_FILES_LOCATION_KEY = 'processed/'
    PROCESSED_DETAIL_FILES_LOCATION_KEY = 'processed/details/'
    UNPROCESSED_UNAPPROVED_FILES_LOCATION_KEY = 'unprocessed/unapproved/'  # files not in $ALLOWED_NAMES_LIST from config.php will be moved to this location
    ERROR_FILES_LOCATION_KEY = 'error/'
    SKIPPED_FILES_LOCATION_KEY = 'skipped/'
    STORE_NUMBER_FIELD_NAME = 'store_number'
    PREVIOUS_RUN_LOCATION_KEY = 'previous_run/'

    # Column transformation for specific files
    m_ary_transformer = {
        'orderhdr.dat': {
            'online': 'on_line'  # online to on_line column
        }
    }

    m_ary_processed_store_numbers = []

    @staticmethod
    def run(l_bln_delete_pre_existing_ready_files=True):
        if l_bln_delete_pre_existing_ready_files:
            l_ary_stats = MainProcessor.delete_pre_existing_ready_files()
            Utils.log(l_ary_stats, True, False)
            Utils.log("Above is the stats of deleted pre-existing ready files", True, False)

        if PULL_FILES_FROM_SOURCE:
            MainProcessor.import_new_files_from_main_source()

        # Important! Develop here to SnowFlake
        MainProcessor.load_data_to_database()

    @staticmethod
    def import_new_files_from_main_source(l_bln_delete_pre_existing_files=True, l_bln_stop_on_error=True):
        start_time = time.time()

        # copy imported files to be processed next
        for value in ALLOWED_NAMES_LIST:
            file_name = value + Utils.CSV_EXT
            # Exporter Tool file path
            src_full_file_path = EXPORTER_FILES_DIR / file_name

            dest_full_file_path = DATA_FILES_BASE_DIR / file_name

            copy_export = False
            if config.TEST_MODE:
                copy_export = True

            l_bln_success = Utils.relocate_file(l_str_full_file_path=src_full_file_path, l_str_dest_full_file_path=dest_full_file_path, l_bln_just_copy=copy_export)
            Utils.log(f"Copied({'yes' if l_bln_success else 'no'}) from {src_full_file_path} to {dest_full_file_path}",
                      True, False)

        Utils.log(f"\nFinished importing new files: \t{Utils.get_my_duration(start_time, True)}\n", True, False,
                  Utils.LOG_TYPE_INFO)

    """
    Note: Marked for removal
    """
    @staticmethod
    def build_delete_statements():
        l_str_queries = ''
        for value in ALLOWED_NAMES_LIST:
            l_str_queries += f"DELETE FROM {MY_TABLES_PREFIX}{value.upper()}\n"
        print(l_str_queries)

    @staticmethod
    def load_data_to_database(l_bln_check_for_new_files=True):
        start_time = time.time()

        l_ary_ready_files = MainProcessor.get_ready_files(l_bln_check_for_new_files)

        Utils.log(f"\tReady files: {l_ary_ready_files}", True, False, Utils.LOG_TYPE_INFO)

        l_int_total_rows = len(l_ary_ready_files)
        l_int_counter = 0

        # iterate through every ready file and load into database
        for l_ary_ready_file_metadata in l_ary_ready_files:

            # show operation progress
            l_int_counter += 1
            l_flt_progress = (l_int_counter / l_int_total_rows) * 100

            Utils.log(f"\nProcessing ready file {Path(l_ary_ready_file_metadata[Utils.DATA_FILE_KEY]).name} ({l_int_counter}/{l_int_total_rows})\t[{l_flt_progress}%] \t{Utils.get_my_duration(start_time, True)}\n", True, False, Utils.LOG_TYPE_INFO)
            Utils.log(f"\tMetadata: {l_ary_ready_file_metadata}", True, False, Utils.LOG_TYPE_INFO)

            l_str_name_to_check = l_ary_ready_file_metadata[Utils.BASE_FILE_NO_EXT_KEY]
            l_bln_is_approved = l_str_name_to_check in ALLOWED_NAMES_LIST

            if l_bln_is_approved:
                # load data into Snowflake
                if SAVE_TO_DATABASE:
                    """
                    Must Change to SnowFlakeDataImporter ->
                    """
                    print("Made it to the SnowFlakeDataImporter section."
                          "This is where the data will be loaded into the SnowFlake database.")
                    #Find actual file to import and change accordingly
                    print(f"File to import: {l_ary_ready_file_metadata['base_file_no_ext']}")
                    SnowflakeDataImporter.import_ready_files_to_snowflake(l_ary_ready_file_metadata['data_file'])

                # clean up this ready file. Move to processed/details
                l_bln_file_cleaned_up = MainProcessor.cleanup_ready_file(l_ary_ready_file_metadata, l_bln_is_approved)

                if not l_bln_file_cleaned_up:
                    Utils.log(f"WARNING: Was not able to clean up ready file: {l_ary_ready_file_metadata}", True, False, Utils.LOG_TYPE_WARN)
            else:
                # clean up this ready file. Move to unprocessed/unapproved
                l_bln_file_cleaned_up = MainProcessor.cleanup_ready_file(l_ary_ready_file_metadata, l_bln_is_approved)
                Utils.log(f"{__name__}\nWARNING: file not approved and it won't be inserted to the database:\t{Path(l_ary_ready_file_metadata[Utils.DATA_FILE_KEY]).name}\n\tThis file has been moved to files/{MainProcessor.UNPROCESSED_UNAPPROVED_FILES_LOCATION_KEY}", True, False, Utils.LOG_TYPE_WARN)

        Utils.log(f"{__name__}\nFinished processing all {l_int_total_rows} ready files.\t{Utils.get_my_duration(start_time, True)}\n\tSuccessful files were moved to files/processed directory", True, False, Utils.LOG_TYPE_INFO)

    @staticmethod
    def process_files(l_str_dir_path=None):
        start_time = time.time()

        Utils.log(f"{__name__}: Start - Processing {MODULE_NAME} files", True, False)

        if l_str_dir_path is None:
            l_str_dir_path = DATA_FILES_BASE_DIR  # source dir

        # processed dir
        l_str_success_processed_file_location = os.path.join(l_str_dir_path, MainProcessor.PROCESSED_FILES_LOCATION_KEY)

        # ready for loading to database dir
        l_str_processed_ready_files_location = os.path.join(l_str_dir_path, MainProcessor.PROCESSING_READY_FILES_LOCATION_KEY)

        # processing dir
        l_str_processing_file_location = os.path.join(l_str_dir_path, MainProcessor.PROCESSING_FILES_LOCATION_KEY)

        # error dir
        l_str_error_processed_file_location = os.path.join(l_str_dir_path, MainProcessor.ERROR_FILES_LOCATION_KEY)

        # skipped dir
        l_str_skipped_processed_file_location = os.path.join(l_str_dir_path, MainProcessor.SKIPPED_FILES_LOCATION_KEY)

        # last files dir
        l_str_last_processed_file_location = os.path.join(l_str_dir_path, MainProcessor.PREVIOUS_RUN_LOCATION_KEY)

        Utils.log(f"{__name__}: Processing files from: {l_str_dir_path}", True)

        # get all new files
        l_ary_files_list = Utils.get_new_files(l_str_dir_path, Utils.CSV_EXT)

        Utils.log(l_ary_files_list, True, False)  # print files found

        l_chr_file_delimiter = ','  # delimiter of source file
        l_int_max_records = None  # number of records to sample from source. By default, None, is set to process all records
        l_bln_append_header_to_file = True  # True to append header to destination file, False to leave header out
        l_str_enclosure = '"'  # enclosure of source files. " is typical, however sometimes files are enclosed with '

        l_chr_dest_file_delimiter = "\t"  # delimiter of destination files to be loaded into the database

        # When True, if destination ready file exists, new data is appended to it.
        # If we have multiple zip files, this might be handy, so data won't be overridden by the content of the latest zip file.
        # When False, if a destination file exists, only the data of the latest zip file will be accounted for.
        l_bln_always_append_to_dest_file = True

        l_ary_transformer = MainProcessor.m_ary_transformer

        l_ary_overall_processing_stats = []  # container of operation summary, including zip file names and the number of successful/failed data files processed.

        # l_bln_move_source_file = not Utils.should_move_source_file_after_processing()
        l_bln_move_source_file = Utils.should_move_source_file_after_processing()

        # relocate source files for processing
        for l_str_full_file_path in l_ary_files_list:
            Utils.log(f"{__name__}: Start - Relocating source file: {l_str_full_file_path}", True)

            l_str_file_name = Path(l_str_full_file_path).name
            l_str_file_name = l_str_file_name.replace(Utils.CSV_EXT, Utils.DAT_EXT)  # rename extension

            Utils.relocate_file(l_str_full_file_path=l_str_full_file_path, l_str_dest_full_file_path=os.path.join(l_str_processing_file_location, l_str_file_name), l_bln_just_copy=l_bln_move_source_file)
            Utils.relocate_file(l_str_full_file_path=l_str_full_file_path, l_str_dest_full_file_path=os.path.join(l_str_last_processed_file_location, l_str_file_name), l_bln_just_copy=True)

        # get all the zip file content (list of unzipped files within the zip file)
        l_ary_unprocessed_files = Utils.get_new_files(l_str_processing_file_location, Utils.DAT_EXT)

        # show list of unzipped files
        Utils.log(l_ary_unprocessed_files, True, False)

        # prepare data to append to unzipped rows (store number)
        l_ary_append_data = {}

        l_int_total_rows = len(l_ary_files_list)
        l_int_counter = 0
        l_bln_insert_test_headers = False

        # localized stats for this one zip file
        l_ary_processing_stats = {'total_files': l_int_total_rows, Utils.SUCCESS_KEY: 0, Utils.LOG_TYPE_ERROR: 0}

        # parse each unzipped file
        for l_str_unprocessed_full_file_path in l_ary_unprocessed_files:
            l_int_counter += 1
            l_flt_progress = (l_int_counter / l_int_total_rows) * 100
            l_str_unzip_base_name = Path(l_str_unprocessed_full_file_path).name

            # @todo remove after testing
            if RUN_SPECIAL_TEST_CONDITIONS:
                if l_str_unzip_base_name != 'reason.dat':
                    continue

            Utils.log(f"\nProcessing {Path(l_str_unprocessed_full_file_path).name} ({l_int_counter}/{l_int_total_rows})\t[{l_flt_progress}%] \t{Utils.get_my_duration(start_time, True)}\n")

            l_str_full_temp_file_name = os.path.join(l_str_processed_ready_files_location, l_str_unzip_base_name)
            l_bln_append_header_to_file = not os.path.exists(l_str_full_temp_file_name)  # append header to ready file only if file does not exist

            # parses unzipped file, append store number to each row and places new file in location specified by l_str_processed_ready_files_location
            l_ary_parsed_content = Utils.parse_file_and_save_to_new_file(l_str_unprocessed_full_file_path, l_chr_file_delimiter, l_int_max_records, l_bln_append_header_to_file, l_ary_append_data, l_str_enclosure, l_str_processed_ready_files_location, l_chr_dest_file_delimiter, l_bln_always_append_to_dest_file, l_ary_transformer)

            Utils.log(f"\nFinished processing {Path(l_str_unprocessed_full_file_path).name}.\t{Utils.get_my_duration(start_time, True)}\n", True, False)
            if not l_ary_parsed_content[Utils.SUCCESS_KEY]:

                # move file unzipped file to errors directory, if there were any error processing it
                Utils.relocate_file(l_str_unprocessed_full_file_path, os.path.join(l_str_error_processed_file_location, Path(l_str_unprocessed_full_file_path).name))

                l_ary_processing_stats[Utils.LOG_TYPE_ERROR] += 1
                Utils.log(f"\nERROR: could not parse and save {l_str_unprocessed_full_file_path}. {l_ary_parsed_content}.\t{Utils.get_my_duration(start_time, True)}\n", True, False, Utils.LOG_TYPE_ERROR)
            else:
                # delete original unprocessed file, since we're done processing it successfully
                Utils.delete_file(l_str_unprocessed_full_file_path)

                l_ary_processing_stats[Utils.SUCCESS_KEY] += 1

        # accumulate stats for every zipped file processed
        l_ary_overall_processing_stats.append(l_ary_processing_stats)

        Utils.log(f"{__name__}: Finished - Processing files[ found: {len(l_ary_overall_processing_stats)} files]. Processing Summary: {l_ary_overall_processing_stats}", True, False)

        if RUN_SPECIAL_TEST_CONDITIONS:
            Utils.log(l_ary_overall_processing_stats, True, True)

        return l_ary_overall_processing_stats

    @staticmethod
    def get_ready_files(l_bln_check_for_new_files=True):
        Utils.log(f"{__name__}: Start - getting ready files", True, False, Utils.LOG_TYPE_INFO)

        if l_bln_check_for_new_files:
            Utils.log(f"{__name__}: Checking for new zip files to process, if any", True, False, Utils.LOG_TYPE_INFO)
            l_ary_files_stats = MainProcessor.process_files()
            Utils.log(
                f"{__name__}: Finished checking for new zip files. Found: {len(l_ary_files_stats)}. Summary of zip files found: {l_ary_files_stats}",
                True, False, Utils.LOG_TYPE_INFO)

        l_str_dir_path = DATA_FILES_BASE_DIR  # source dir

        l_ary_meta_data = []

        # ready for loading to database dir
        l_str_processed_ready_files_location = os.path.join(l_str_dir_path, MainProcessor.PROCESSING_READY_FILES_LOCATION_KEY)

        # ready dir does not exist
        if not os.path.exists(l_str_processed_ready_files_location):
            Utils.log(f"ready dir does not exist yet: {l_str_processed_ready_files_location}", True, False)
            return l_ary_meta_data

        l_ary_unprocessed_files = Utils.get_new_files(l_str_processed_ready_files_location, Utils.DAT_EXT)

        for l_str_unprocessed_full_file_path in l_ary_unprocessed_files:
            l_ary_meta_data.append(Utils.get_data_file_meta_data(l_str_unprocessed_full_file_path))

        Utils.log(f"{__name__}: Finished - getting ready files. Found: {len(l_ary_meta_data)}", True, False,
                  Utils.LOG_TYPE_INFO)

        return l_ary_meta_data

    @staticmethod
    def cleanup_ready_file(l_ary_ready_file_metadata, l_bln_approved=True):
        """
        Cleans up the ready file of the given metadata.

        :param l_ary_ready_file_metadata: Expected format for one file produced by get_ready_files()
        :param l_bln_approved: Boolean value indicating if the file is approved
        :return: bool
        """

        l_str_dir_path = DATA_FILES_BASE_DIR  # source dir
        l_str_success_processed_detail_file_location = l_str_dir_path / MainProcessor.PROCESSED_DETAIL_FILES_LOCATION_KEY  # approved processed location
        l_str_unprocessed_unapproved_file_location = l_str_dir_path / MainProcessor.UNPROCESSED_UNAPPROVED_FILES_LOCATION_KEY  # unprocessed unapproved location

        l_str_destination_dir = l_str_success_processed_detail_file_location if l_bln_approved else l_str_unprocessed_unapproved_file_location  # final location based on approval

        if Utils.ORIGINAL_FILE_KEY in l_ary_ready_file_metadata:
            l_str_full_file_path = l_ary_ready_file_metadata[Utils.ORIGINAL_FILE_KEY]
            l_str_file_name = Path(l_ary_ready_file_metadata[Utils.ORIGINAL_FILE_KEY]).name
            l_str_new_file_full_path = l_str_destination_dir / l_str_file_name
            return Utils.relocate_file(l_str_full_file_path, l_str_new_file_full_path)

        return False

    @staticmethod
    def delete_pre_existing_ready_files():
        start_time = time.time()
        l_bln_check_for_new_files = False
        l_ary_ready_files = MainProcessor.get_ready_files(l_bln_check_for_new_files)

        l_int_total_rows = len(l_ary_ready_files)
        l_int_counter = 0
        l_ary_stats = {'error': [], 'success': []}

        # iterate through every ready file and load into database
        for l_ary_ready_file_metadata in l_ary_ready_files:
            # show operation progress
            l_int_counter += 1
            l_flt_progress = (l_int_counter / l_int_total_rows) * 100

            Utils.log(
                f"\nProcessing ready file {Path(l_ary_ready_file_metadata[Utils.DATA_FILE_KEY]).name} ({l_int_counter}/{l_int_total_rows})\t[{l_flt_progress}%] \t{Utils.get_my_duration(start_time, True)}\n")
            Utils.log(f"\tMetadata: {l_ary_ready_file_metadata}")

            l_str_name_to_check = l_ary_ready_file_metadata[Utils.BASE_FILE_NO_EXT_KEY]
            l_bln_is_approved = l_str_name_to_check in ALLOWED_NAMES_LIST
            l_str_full_file_path = l_ary_ready_file_metadata[Utils.ORIGINAL_FILE_KEY]
            if l_bln_is_approved:
                # delete file
                l_bln_file_deleted = Utils.delete_file(l_str_full_file_path)

                if not l_bln_file_deleted:
                    l_ary_stats['error'].append(l_str_full_file_path)
                    Utils.log(f"WARNING: Was not able to delete ready file: {l_ary_ready_file_metadata}",
                              l_str_log_type=Utils.LOG_TYPE_WARN)
                else:
                    l_ary_stats['success'].append(l_str_full_file_path)
            else:
                # not approved
                Utils.log(
                    f"\nWARNING: file not approved and it won't be deleted:\t{Path(l_ary_ready_file_metadata[Utils.DATA_FILE_KEY]).name}",
                    l_str_log_type=Utils.LOG_TYPE_WARN)

        Utils.log(
            f"\nFinished processing all {l_int_total_rows} ready files.\t{Utils.get_my_duration(start_time, True)}\n",
            l_str_log_type=Utils.LOG_TYPE_INFO)
        return l_ary_stats


