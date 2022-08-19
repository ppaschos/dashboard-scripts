import elasticsearch
import urllib3
import pandas as pd
# Get rid of insecure warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
es = elasticsearch.Elasticsearch(
        ['https://gracc.opensciencegrid.org/q'],
        timeout=300, use_ssl=True, verify_certs=False)

from elasticsearch_dsl import A, Search
s = Search(using=es, index='gracc.osg.summary')
s = s.filter('range', EndTime={'gte': 'now-1M', 'lt': 'now'})
s = s.filter('term', ProbeName="condor-ap:login.collab.ci-connect.net")
s.aggs.bucket('EndTime', 'date_histogram', field='EndTime', interval='day') \
.metric('CoreHours', 'sum', field='CoreHours') \
.metric('Njobs', 'sum', field='Njobs')

response = s.execute()
df = pd.DataFrame(columns = ['Date', 'Core Hours', 'Number of Jobs', 'Average Core Hours'])
for day in response.aggregations['EndTime']['buckets']:
    avg_corehours = 0
    try:
        avg_corehours = day['CoreHours']['value'] / day['Njobs']['value']
    except ZeroDivisionError as zde:
        avg_corehours = 0

    df.loc[len(df)] = [pd.Timestamp(day['key_as_string']), day['CoreHours']['value'], day['Njobs']['value'], avg_corehours]


df.to_csv('avgcorehours.csv')
@ppaschos
 
