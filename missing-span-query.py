from datetime import datetime
import json
import os
from pathlib import Path
import sys
import time
import requests
import urllib
import logging
from dotenv import load_dotenv

load_dotenv()
LS_ORGANIZATION = os.getenv('LS_ORGANIZATION')
LS_API_KEY = os.getenv('LS_API_KEY')

LS_API_HEADERS = {"Authorization": LS_API_KEY}

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)
log_format = logging.Formatter('[%(asctime)s] [%(levelname)s] - %(message)s')
# writing to stdout                                                     
handler = logging.StreamHandler(sys.stdout)                             
handler.setLevel(logging.DEBUG)                                        
handler.setFormatter(log_format)                                        
LOG.addHandler(handler)  

def main():
    stream_id = 'Pt2nKMGp'
    project = 'demo'

    try:
        start = time.time()
        exemplars = get_stream_exemplars(project, stream_id, '2022-08-23T01:05:00-05:00', '2022-08-23T01:10:00-05:00')
        trace_count = process_exemplars(stream_id, exemplars, project)
        end = time.time()
        exec_time = round(end - start, 3)
        LOG.info(f'Processing completed! - {trace_count} traces processed in {exec_time} seconds')
    except Exception as ex:
        LOG.exception(f'ERROR: {ex}')


def get_stream_exemplars(project, stream_id, start_time, end_time, resolution_ms = 60000, include_ops_counts = 0, include_error_counts = 0):
    url = f'https://api.lightstep.com/public/v0.2/{LS_ORGANIZATION}/projects/{project}/streams/{stream_id}' \
          f'/timeseries?oldest-time={urllib.parse.quote(start_time)}&youngest-time={urllib.parse.quote(end_time)}&resolution-ms={resolution_ms}&include-exemplars=1' \
          f'&include-ops-counts={include_ops_counts}&include-error-counts={include_error_counts}'

    LOG.info(f'Getting exemplars for stream {stream_id}...')
    response =  requests.get(url, headers=LS_API_HEADERS).json()
    LOG.info(f'Exemplars retrieved successfully')
    return response['data']['attributes']['exemplars']


def process_exemplars(stream_id, exemplars, project):
    LOG.info(f'Processing exemplars for stream {stream_id}...')
    stream_data = {
            'stream-id': stream_id,
            'traces': []
    }

    processed_traces = []
    for exemplar in exemplars:
        trace_guid = exemplar['trace_guid']
        if trace_guid not in processed_traces:
            trace_data = process_trace(trace_guid, exemplar['span_guid'], project)
            processed_traces.append(trace_guid)
            if trace_data:
                trace_data['trace-url'] = f'https://app.lightstep.com/{project}/trace?trace_handle={exemplar["trace_handle"]}'
                trace_data['oldest-micros'] = exemplar['oldest_micros']
                trace_data['youngest-micros'] = exemplar['youngest_micros']
                stream_data['traces'].append(trace_data)

    final_data = analyze_traces(stream_data)
    write_trace_to_file(final_data)
    return len(processed_traces)

def process_trace(trace_id, span_id, project):
    LOG.info(f'Processing trace {trace_id}...')
    spans = get_trace_for_span(span_id, project)
    if not spans:
        return None

    span_count = len(spans)
    span_names = []
    
    for span in spans:
        span_name = span['span-name']
        if span_name not in span_names:
            span_names.append(span_name)


    return {
        'trace-id': trace_id,
        'span-count': span_count,
        'span-names': span_names
    }


def get_trace_for_span(span_id, project):    
    url = f'https://api.lightstep.com/public/v0.2/{LS_ORGANIZATION}/projects/{project}/stored-traces?span-id={span_id}'

    LOG.info(f'Getting spans from trace with span ID {span_id}')
    response =  requests.get(url, headers=LS_API_HEADERS)
    if response.status_code == 200:
        return response.json()['data'][0]['attributes']['spans']
    else:
        LOG.warn(f'No traces for span-id {span_id}')
        return []


def write_trace_to_file(stream_data):
    date = datetime.now()
    file_date = f'{date.strftime("%Y")}_{date.strftime("%m")}_{date.strftime("%d")}'
    stream_id = stream_data['stream-id']
    stream_file = Path(f'traceanalysis_{stream_id}_{file_date}.json')

    LOG.info('Writing data to file...')
    with open(stream_file, "w") as outfile:
        json.dump(stream_data, outfile)
    LOG.info(f'Data was successfully written to file {stream_file}')


def analyze_traces(stream_data):
    sorted_traces = sorted(stream_data['traces'], key=lambda d: d['span-count'], reverse=True)
    baseline = sorted_traces[0]

    for trace in sorted_traces:
        if trace['span-count'] < baseline['span-count']:
            msg = f'Trace {trace["trace-id"]} ({trace["trace-url"]}) is missing the following spans: \n-->'
            s = set(trace['span-names'])
            missing_spans = [x for x in baseline['span-names'] if x not in s]
            msg += '\n--> '.join(missing_spans)
            trace['missing-spans'] = missing_spans
            LOG.warn(msg)

    stream_data['traces'] = sorted_traces
    return stream_data



if __name__ == "__main__":
    main()