"""
 __      __   _                    _         _  _         _
 \ \    / /__| |__ ___ _ __  ___  | |_ ___  | || |_  _ __| |_ _ __ _
  \ \/\/ / -_) / _/ _ \ '  \/ -_) |  _/ _ \ | __ | || / _` | '_/ _` |
   \_/\_/\___|_\__\___/_|_|_\___|  \__\___/ |_||_|\_, \__,_|_| \__,_|
                                                  |__/

Welcome to NeuroLake Hydra! This is a simple query template for you to play and develop!
This little piece of code has one job, access a website on the internet and collect data.

You must implement all your code inside the 'query_execution' method. It will receive a dictionary
called 'input_data' containing the input to be used by the query and another dictionary
called 'properties' with some useful tools and information about how we want you to build the Hydra query.

"""
"""
<#@#HydraMetadata#@#>
{"version":"1.0.0",
"requirements":["selenium"],
"developer_contact":"raony.alves@neurotech.com.br",
"host": "http://www.portaltransparencia.gov.br/download-de-dados/ceaf/",
"query_key": "DATE",
"timeout":"15",
"selenium_usage":"true",
"query_name":"PES058"}
</#@#HydraMetadata#@#>
"""
""" NeuroLake imports """
import utils.Lake_Utils as Utils
import utils.Lake_Exceptions as Exceptions
import utils.Lake_Enum as Enums
import tools.base_classes.download_file_query as DownloaderTool

"""You should need at least these imports"""
import json
import time
import sys

"""Your own imports go down here"""
from selenium import webdriver
from selenium.webdriver.common.by import By
import json

def get_target_host():
    return Enums.environ_variables['host']

def save_scraper_data(html_content, input_data):
    """This method is responsible for taking your HTML data and saving it according to our storage standards, we will apply
    some transfromations to ensure no excessive data is saved in our infrastructure"""
    file_timestamp = time.strftime(Enums.Defaults["TIMESTAMP_FORMAT"])
    cleaned_html = Utils.clean_html(html_content)

    Utils.save_data(Enums.SAVE_TARGETS['SCRAPER'],
                    Enums.environ_variables['query_name'],
                    file_timestamp,
                    Utils.generate_filename(list(input_data.values()),
                                            extension='html',
                                            status="SUCCESS",
                                            timestamp=file_timestamp),
                    html_content)
    return cleaned_html

def query_execution(input_data, properties):
    """query_execution method
    :param: input_data: Dictionary containing all the content to be searched in the data sources
    :param: properties: Dictionaty containing execution properties such as TIMEOUT, Proxy configurations, IP configurations etc."""

    # First, you need to retrieve the information from the source we passed to you. You can use the method 'get_target_host()'
    # to do so. Here, that method only accesses an environment variable.

    DRIVER = properties["driver"]
    DRIVER.get('http://www.portaltransparencia.gov.br/download-de-dados/ceaf/')
    time.sleep(1)

    link_list = DRIVER.find_elements(By.XPATH, '//*[@id="link-unico"]/li/a')
    main_dict = {}
    for e in link_list:
        link_json = {str(e.text.encode('UTF-8')):str(e.get_attribute('href'))}
        main_dict.update(link_json)
    DRIVER.close()
    entry = None

    result = None
    list_entries=[]
    keys = list(main_dict.keys())
    for f in keys:
        if input_data['month']+'/'+input_data['year'] in f:
            entry = main_dict[f]
            list_entries.append(f)
        if entry is not None:
            file_downloader = DownloaderTool.FileDownloader(query_name=Enums.environ_variables['query_name'],
                                                            target_url=entry,
                                                            query_input=input_data,
                                                            file_format='zip',
                                                            wget_headers={})
            result = file_downloader.download_file()
            file_downloader.extract_content(result)
        print("QUERY STAGE COMPLETED")

    return {"Downloaded_File":result}


def request(input_data, properties):
    file_timestamp = time.strftime(Enums.Defaults["TIMESTAMP_FORMAT"])

    print(("My parameters were: "+str(input_data)+" "+str(properties)))
    result = query_execution(input_data, properties)

    query_info = {}
    query_info['query_name'] = Enums.environ_variables['query_name']
    query_info['query_version'] = Enums.QUERY_VERSIONS.get(Enums.environ_variables['query_name'])
    query_info['query_input'] = input_data
    query_info['query_date'] = time.strftime(Enums.Defaults['TIMESTAMP_FORMAT'])
    query_info['file_timestamp'] = file_timestamp
    query_info.update(result)
    
    return query_info

def test_request():
    # You can extend the properties from you file metadata
    my_test_properties = Utils.load_parameters(__file__)
    driver = my_test_properties['driver']
    result = request({"year":'2019',"month":"04"},{'driver':driver,'timeout':15})
    assert type(result) == dict


