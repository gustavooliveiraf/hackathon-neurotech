import sys
import os
import time
import json
import shutil
from subprocess import check_call
from subprocess import Popen
from datetime import datetime
# import tools.base_classes.convert_file_query as FileConvert
# --------------------------------Neurolake Imports----------------------------------------------
import utils.Lake_Utils as Utils # Provide general methods used by most queries
import utils.Lake_Enum as Enums # Provide values and general information used by the architecture
import utils.Lake_Exceptions as Exceptions
class FileDownloaderException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class ExtractFileException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class FileDownloader():
    """Class made to abstract the File Downloading proccess to queries that need only to download files from servers
    This Class downloads the content directly to EFS following the correct structure defined in the Neurolake Documentation
    :argument target_url: Url from which the content will be downloaded
    :argument query_name: Query Code. Used to create the correct filename at EFS
    :argument file_format: Format of the downloaded File (The Default Value is 'zip'
    :argument wget_headers: (optional) headers to be passed down to wget tool
    :argument efs_origin: (optional) folder in EFS where the content will be saved (CRAWLER, PARSER, MEDIA)
    :argument avoid_normalization: (optional) flag used to avoid normalization of filename"""
    def __init__(self, target_url, query_name, query_input, file_format='zip', wget_headers=None, efs_origin='PARSER'):
        self.target_url = target_url
        self.query_name = query_name
        self.query_input = query_input
        self.file_format = file_format
        self.wget_headers = wget_headers
        self.efs_origin = efs_origin
        self.unsuported_formats = ['jpg', 'png', 'doc']

    def download_file(self, post_data=None, ref_date=False, send_s3=False):
        """Method to download a file directly to EFS
        :argument post_data: dictionary or string containing the post data to be sent. If you set this variable, wget
        will send a post request to the website"""
        file_timestamp = time.strftime(Enums.Defaults['TIMESTAMP_FORMAT'])
        print(file_timestamp)
        if self.wget_headers is not None:
            self.wget_headers['Connection'] = 'close'
        file_name = Utils.save_data(origin=Enums.EFS_ORIGINS[self.efs_origin],
                                    query_name=self.query_name,
                                    timestamp=file_timestamp,
                                    filename=Utils.generate_filename(list(self.query_input.values()),
                                                                     extension=self.file_format,
                                                                     status="SUCCESS",
                                                                     timestamp=file_timestamp,
                                                                     ref_date=ref_date),
                                    data=self.target_url,
                                    is_data_url=True,
                                    headers_dic=self.wget_headers,
                                    post_data=post_data)

        # Check if the file downloaded is valid
        if os.stat(file_name).st_size <= 100:
            try:
                os.remove(file_name)
            except OSError:
                pass
            raise FileDownloaderException("FileDownloader: Wget Downloaded an Empty File")

        elif send_s3:
            compressed_file = self.__send_file_to_s3__(file_name)
            os.remove(compressed_file)

        return file_name


    def get_timestamp_content(self, file_name):
        if self.file_format == 'zip':
            import zipfile
            file = zipfile.ZipFile(file_name, "r")
            timestamp = []
            for info in file.infolist():
                time_info = str(info.date_time[2])+'#@#'+\
                            str(info.date_time[1])+'#@#'+\
                            str(info.date_time[0])
                timestamp.append(time_info)
            return timestamp

        elif self.file_format == 'rar':
            import rarfile
            file = rarfile.RarFile(file_name, "r")
            timestamp = []
            for info in file.infolist():
                time_info = str(info.date_time[2]) + '#@#' + \
                            str(info.date_time[1]) + '#@#' + \
                            str(info.date_time[0])
                timestamp.append(time_info)
            return timestamp

    def extract_content(self, file_name, avoid_normalization=False, wanted_file='', codif='utf8', new_extension='csv'):
        file_timestamp = time.strftime(Enums.Defaults['TIMESTAMP_FORMAT'])
        sep=Enums.Defaults['VERSION_SEPARATOR']
        if self.file_format == 'zip':
            temp_folder = file_name.replace('.zip', '')
            os.makedirs(temp_folder)
            try:
                os.system('unzip '+file_name+' -d '+temp_folder)
                file_list = []
                count=0
                for basedir, subdirs, files in os.walk(temp_folder):
                    if len(files) > 0:
                        for file_path in files:
                            file_list.append(os.path.join(basedir, file_path))

                for _file in file_list:
                    # Check if the extension is not allowed
                    extension = _file.split('.')[-1]
                    if extension not in self.unsuported_formats:
                        if extension == 'kmz':
                            file_name_no_extension = os.path.basename(_file).split('.')[0]
                            personal_timestamp = self.get_timestamp_content(file_name)
                            if avoid_normalization == False:
                                encoded = Utils.codec_removal(file_name_no_extension)
                            else:
                                encoded = file_name_no_extension
                            no_timestamp_list = file_name.split('#@#')
                            no_timestamp = '#@#'.join(no_timestamp_list[:-1])
                            new_file_name = no_timestamp+sep+encoded+sep+personal_timestamp[count]+sep+file_timestamp+\
                                            '.'+new_extension
                            Popen(['mv', _file, new_file_name]).wait()
                            count+=1
                        if extension == 'mdb':
                            data_ref_mdb = '01#@#01#@#' + file_name.replace('.zip', '').split('/')[8][:-23]
                            no_timestamp_list = file_name.split('#@#')
                            no_timestamp = '#@#'.join(no_timestamp_list[:-1])
                            first_file=no_timestamp+\
                                    sep+Utils.normalize_content(wanted_file, codif=codif)+ \
                                    sep+str(count)+sep+data_ref_mdb+sep+file_timestamp+'.'+new_extension
                            casco = open(first_file, "w")
                            first=Popen(['mdb-export', _file, wanted_file], stdout=casco)
                            first.wait()
                            if os.stat(first_file).st_size <= 0:
                                os.remove(first_file)
                            count += 1

                        elif extension in ['txt', 'csv'] and wanted_file in _file:
                            file_name_no_extension = os.path.basename(_file).split('.')[0]
                            personal_timestamp = self.get_timestamp_content(file_name)
                            if avoid_normalization == False:
                                encoded = Utils.normalize_content(file_name_no_extension, codif=codif)
                            else:
                                encoded = file_name_no_extension
                            no_timestamp_list = file_name.split('#@#')
                            no_timestamp = '#@#'.join(no_timestamp_list[:-1])
                            # Rename the file to standardized date
                            new_file_name = no_timestamp+sep+encoded+sep+ personal_timestamp[count]+sep+file_timestamp+\
                                            '.'+new_extension
                            Popen(['mv', _file, new_file_name]).wait()
                            count+=1
            except OSError:
                try:
                    os.remove(file_name)
                    shutil.rmtree(temp_folder)
                except OSError:
                    raise ExtractFileException("Couldn't remove zip file")
            finally:
                try:
                    os.remove(file_name)
                    shutil.rmtree(temp_folder)
                    #print 'a'
                except OSError:
                    pass


        elif self.file_format == 'rar':
            temp_folder = file_name.replace('.rar', '')
            os.makedirs(temp_folder)
            try:
                os.system('unrar ' + ' e ' + file_name + ' ' + temp_folder)
                file_list=[]
                count=0
                for basedir, subdirs, files in os.walk(temp_folder):
                    if len(files) > 0:
                        for file_path in files:
                            file_list.append(os.path.join(basedir, file_path))
                for _file in file_list:
                    # Check if the extension is not allowed
                    extension = _file.split('.')[-1]
                    if extension not in self.unsuported_formats:
                        if extension in ['txt', 'csv'] and wanted_file in _file:
                            file_name_no_extension = os.path.basename(_file).split('.')[0]
                            personal_timestamp = self.get_timestamp_content(file_name)
                            if avoid_normalization == False:
                                encoded = Utils.normalize_content(file_name_no_extension, codif=codif)
                            else:
                                encoded = file_name_no_extension
                            no_timestamp_list = file_name.split('#@#')
                            no_timestamp = '#@#'.join(no_timestamp_list[:-1])
                            new_file_name = no_timestamp+sep+encoded+sep+personal_timestamp[count]+sep+file_timestamp+\
                                            '.'+new_extension
                            Popen(['mv', _file, new_file_name]).wait()
                            count+=1
            except OSError:
                try:
                    os.remove(file_name)
                    shutil.rmtree(temp_folder)
                except OSError:
                    raise ExtractFileException("Couldn't remove rar file")
            finally:
                try:
                    os.remove(file_name)
                    shutil.rmtree(temp_folder)
                except OSError:
                    pass

