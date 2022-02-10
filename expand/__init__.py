from datetime import datetime
import json
import logging

import azure.functions as func  # type: ignore
from dateutil.rrule import rrule

MAX_COUNT = 1000

def main(req: func.HttpRequest) -> func.HttpResponse:
    raw_body = req.get_body().decode('utf-8','ignore')
    logging.info(f'Request body:\n {raw_body}')
    
    try:
        body = req.get_json()
    except ValueError:
        raise ValueError('Unable to parse request body')

    try:
        r = parse_rrule(body)
    except ValueError as e:
        logging.error(f'Parse error: {e}')
        raise e
    
    # TODO but what about timezones
    resp = {
      "instances": [ dt.strftime('%Y-%m-%d') for dt in next_instances(r) ]
    }
    resp_body = json.dumps(resp)

    logging.info(f'Response body:\n {resp_body}')

    return func.HttpResponse(
        resp_body.encode('utf-8'),
        status_code=200,
        mimetype='application/json',
        charset='utf-8',
    )
    

def parse_rrule(body) -> rrule:
    freq = body.get('freq',None)
    start = body.get('start',None)
    interval = int(body.get('interval',1))
    until = body.get('until',None)
    if freq is None:
        raise ValueError('Missing required: freq')
    if start is None:
        raise ValueError('Missing required: start')
    return rrule(
        freq=parse_freq(freq),
        dtstart=parse_datetime(start),
        interval=interval,
        until=None if until is None else parse_datetime(until),
    )

def parse_freq(s: str):
    try:
        return ['years','months','weeks'].index(s)
    except IndexError:
        raise ValueError(f'Unknown freq: "{s}"')

# TODO: timezone

def parse_datetime(s: str) -> datetime:
    try:
        return datetime.strptime(s,'%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f'Unknown date string: "{s}": {e}')


def next_instances(r: rrule):
    return r.xafter(r._dtstart, count=None if is_finite(r) else MAX_COUNT, inc=False)


def is_finite(r: rrule) -> bool:
    return not (r._count is None and r._until is None)


