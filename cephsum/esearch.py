import requests,os,logging
from requests.exceptions import Timeout
from datetime import date,datetime
import urllib3

from socket import getfqdn

# very simple module to send data via requests to elastich search instance.
# Uses env variables to extract host name. 

def send_data(data,type_name='echo_xrdcks'):
    """Send data to ES, data is a dict. 
    For hostname of es, use os.environ 
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    es_host = os.environ['CEPHSUM_ES_HOSTNAME']
    day = date.today().strftime("%Y.%m.%d")
    path = f'/logstash-{day}/doc/'

    params = dict(data)
    # add any extra variables 
    params['fqdn'] = getfqdn()

    # add the type name as prefix to all varaibles 
    for k,v in params.items():
        params[k] = f'{type_name}_{v}' 
    # do forget to add the type
    params['type'] = type_name

    #add some additional parameters
    params['@timestamp'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        req = requests.post(url=es_host+path, verify=False,
                    json=params, timeout=2)
        req.raise_for_status()
        logging.debug(f'ES data result {req.status_code}' )
    except Timeout:
        logging.warning("ES data submission hit timeout")

    return req

