import requests,os,logging
from requests.exceptions import Timeout
from datetime import date
import urllib3

# very simple module to send data via requests to elastich search instance.
# Uses env variables to extract host name. 

def send_data(data,type_name='echo_xrdcks'):
    """Send data to ES, data is a dict. 
    For hostname of es, use os.environ 
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    es_host = os.environ['CEPHSUM_ES_HOSTNAME']
    day = date.today().strftime("%Y.%m.%d")
    path = f'/logstash-{day}/_doc/'

    params = dict(data)
    params['type'] = type_name

    try:
        req = requests.post(url=es_host+path, verify=False,
                    json=params, timeout=2)
        req.raise_for_status()
    except Timeout:
        logging.warning("ES data submission hit timeout")

    logging.debug(f'ES data result {req.status_code}' )
    return req

