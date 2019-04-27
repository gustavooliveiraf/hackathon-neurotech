"""
Welcome to NeuroLake Hydra! This is a simple query template for you to play and develop!
This little piece of code has one job, access a website on the internet and collect data.

You must implement all your code inside the 'query_execution' method. It will receive a dictionary
called 'input_data' containing the input to be used by the query and another dictionary
called 'properties' with some useful tools and information about how we want you to build the Hydra query.

"""
"""
<#@#HydraMetadata#@#>
{"version":"1.0.1",
"requirements":["beautifulsoup4"],
"developer_contact":"camposmoraes@outlook.com",
"host": "https://www.jusbrasil.com.br/busca?q=",
"timeout":"15",
"selenium_usage":"true",
"query_key":"cnpj",
"query_name":"GRD001"}
</#@#HydraMetadata#@#>
"""
""" NeuroLake imports """
import utils.Lake_Utils as Utils
import utils.Lake_Exceptions as Exceptions
import utils.Lake_Enum as Enums

"""You should need at least these imports"""
import json
import time
import sys

"""Loading the metadata from this file into the environment. This is crucial for the correct execution"""
if 'pytest' in sys.modules:
    __query_properties__ = Utils.load_parameters(__file__)

"""Your own imports go down here"""
import requests
from bs4 import BeautifulSoup


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
    print('>>>>>>>>>>>>.',input_data['cnpj'])
    def parser_string(text):
        text =  text.strip()
        text = text.replace('\n', " ")

        return text
        
    query_result = {}
    DRIVER = properties['driver']

    target_host = get_target_host()
    target_host += input_data.get(Enums.environ_variables.get('query_key'))
    DRIVER.get(target_host)

    cleaned_html = save_scraper_data(DRIVER.page_source, input_data)

    parsed_html = BeautifulSoup(cleaned_html)
    table = parsed_html.body.find_all('div', attrs={'class': 'SearchResults-count'})

    result = {}

    for element in table:
        key = "citacoes_jus"
        value = parser_string(element.find('span').text)
        value = value.split(" ")
        result[key] = value
    
    return result


def request(input_data, properties):
    file_timestamp = time.strftime(Enums.Defaults["TIMESTAMP_FORMAT"])

    print(("My parameters were: "+str(input_data)+" "+str(properties)))
    result = query_execution(input_data, properties)
    query_name = Enums.environ_variables['query_name']
    query_info = {}
    query_info['query_name'] = query_name
    query_info['query_version'] = Enums.QUERY_VERSIONS[query_name]
    query_info['query_input'] = input_data
    query_info['query_date'] = time.strftime(Enums.Defaults['TIMESTAMP_FORMAT'])
    query_info['file_timestamp'] = file_timestamp
    query_info.update(result)
    Utils.save_data(Enums.SAVE_TARGETS['PARSER'],
                    query_info['query_name'],
                    file_timestamp,
                    Utils.generate_filename(list(input_data.values()),
                                            extension='json',
                                            status="SUCCESS",
                                            timestamp=file_timestamp),
                    query_info)
    return query_info

def test_request():    
    # You can extend the properties from you file metadata
    my_test_properties = Utils.load_parameters(__file__)

    result = request({"cnpj":"00948060000130"}, my_test_properties)
    assert type(result) == dict
    # assert result["Nome de fantasia"] == "NEUROTECH"

