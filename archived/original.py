import json

from os.path import expanduser

from os import environ

import pandas as pd

import requests

from requests.auth import HTTPBasicAuth

import time

# Load credentials

try:

    with open(expanduser('brain_credentials.txt')) as f:

        credentials = json.load(f)

except FileNotFoundError:

    credentials = (environ.get('BRAIN_USERNAME'), environ.get('BRAIN_PASSWORD'))

# Extract username and password from the list

username, password = credentials

# Create a session object

sess = requests.Session()

# Set up basic authentication

sess.auth = HTTPBasicAuth(username, password)

# Send a POST request to the API for authentication

response = sess.post('https://api.worldquantbrain.com/authentication')

# Print response status and content for debugging

print(response.status_code)

print(response.json())
 
## 获取数据集ID为 fundamental6 下的所有数据字段

def get_datafields(

        s,

        instrument_type: str = 'EQUITY',

        region: str = 'USA',

        delay: int = 1,

        universe: str = 'TOP3000',

        dataset_id: str = '',

        data_type: str = 'MATRIX',

        search: str = ''

):

    offset = 0

    datafields_list = []

    while True:

        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" + \
                       f"&offset={offset}" + \
                       f"&type={data_type}"

        url_template += (f"&search={search}" if search else "")

        resp = sess.get(url_template)

        results = resp.json()

        # print(results)

        if 'results' not in results:

            print(f"Unexpected response: {results}")

            break

        else:

            print(f"Fetched {len(results['results'])} data fields with offset {offset}.")

            datafields_list.append(results['results'])

            if len(results['results']) < 50:

                print("Fetched the last batch of data fields.")

                break

            offset += 50

            time.sleep(5)

       

    datafields_list_flat = [item for sublist in datafields_list for item in sublist]

    datafields_df = pd.DataFrame(datafields_list_flat)

    return datafields_df

fundamental6 = get_datafields(s=sess, dataset_id='pv13', data_type='GROUP')
 
datafields_list = fundamental6['id'].values

len(datafields_list)
 
alpha_list = []
group_ops_list = ['group_mean', 'group_neutralize']
ts_ops_list = ['ts_mean', 'ts_rank']
days = [63, 126]
group = ['market', 'sector', 'industry']

for datafield in datafields_list:

    for group_ops in group_ops_list:

        for ts_ops in ts_ops_list:

            for day in days:

                for group in group:                  

                    print("正在将如下 Alpha 表达式与 setting 封装")

                    print(f'{group_ops}({ts_ops}({datafield}, {days}), {group})')

           

                    simulation_data = {

                        'type' : 'REGULAR',

                        'settings' : {

                            'instrumentType' : 'EQUITY',

                            'region' : 'USA',

                            'universe' : 'TOP3000',

                            'delay' : 1,

                            'decay' : 0,

                            'neutralization' : 'MARKET',

                            'truncation' : 0.08,

                            'pasteurization': 'ON',

                            'unitHandling' : 'VERIFY',

                            'nanHandling': 'ON',

                            'language' :'FASTEXPR',

                            'visualization':False,

                        },

                      "regular": f'{group_ops}({ts_ops}({datafield}, {days}), {group})'

                    }

                    alpha_list.append(simulation_data)
 
for alpha in alpha_list:

  sim_resp = sess.post(

    "https://api.worldquantbrain.com/simulations",

    json = alpha

  )

  try:

    sim_progress_url = sim_resp.headers['Location']

    while True:

      sim_progress_resp = sess.get(sim_progress_url)

      retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))

      if retry_after_sec == 0:  # simulation done!

          break

      time.sleep(retry_after_sec)

    alpha_id = sim_progress_resp.json()["alpha"]  # the final simulation result

    print(alpha_id)

  except:

    print("no location, sleep for 10 seconds and try next alpha")

    time.sleep(10)