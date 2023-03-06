from datetime import datetime
from enum import Enum
import json
import logging
from typing import List

import azure.functions as func  # type: ignore
from dateutil.rrule import rrule
from dateutil.rrule import MO, TU, WE, TH, FR, SA, SU, MONTHLY, WEEKLY

MAX_COUNT = 1000
WEEKDAYS = [MO,TU,WE,TH,FR]

class FreqValues(Enum):
    WEEKLY = "weeks"
    MONTHLY = "months"

class MonthTypeValues(Enum):
    DAYS_OF_WEEK = "days of week"
    DAYS_OF_MONTH = "days of month"
    FIRST_WEEKDAY = "first weekday"
    LAST_WEEKDAY = "last weekday"

class WeekDayValues(Enum):
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"

class MonthNthDayValues(Enum):
    FIRST = "1st"
    SECOND = "2nd"
    THIRD = "3rd"
    FOURTH = "4th"
    LAST = "last"


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
    raw_freq = body.get('freq',None)
    raw_start = body.get('start',None)
    raw_interval = body.get('interval',1)
    raw_until = body.get('until',None)
    raw_month_type = body.get('month_type',None)
    raw_month_nth_days = body.get('month_nth_days',[])
    raw_month_days = body.get('month_days',[])
    raw_week_days = body.get('week_days',[])

    if raw_freq is None:
        raise ValueError('Missing required: freq')
    if raw_start is None:
        raise ValueError('Missing required: start')
    
    freq = parse_freq(raw_freq)
    start = parse_datetime(raw_start)
    interval = int(raw_interval)
    until = None if raw_until is None else parse_datetime(raw_until)

    if freq == WEEKLY:
        week_days = parse_week_days(raw_week_days)
        return rrule(
            freq=freq,
            dtstart=start,
            interval=interval,
            until=until,
            byweekday=week_days,
        )

    if freq == MONTHLY:
        if raw_month_type is None:
            raise ValueError('Missing required: month_type')

        month_type = parse_month_type(raw_month_type)

        if month_type == MonthTypeValues.DAYS_OF_MONTH:
            month_days = parse_month_days(raw_month_days)
            return rrule(
                freq=freq,
                dtstart=start,
                interval=interval,
                until=until,
                bymonthday=month_days,
            )

        if month_type == MonthTypeValues.DAYS_OF_WEEK:
            month_nth_days = parse_month_nth_days(raw_month_nth_days)
            week_days = parse_week_days(raw_week_days)
            return rrule(
                freq=freq,
                dtstart=start,
                interval=interval,
                until=until,
                bysetpos=month_nth_days,
                byweekday=week_days,
            )
            
        if month_type == MonthTypeValues.FIRST_WEEKDAY:
            return rrule(
                freq=freq,
                dtstart=start,
                interval=interval,
                until=until,
                bysetpos=1,
                byweekday=WEEKDAYS,
            )

        if month_type == MonthTypeValues.LAST_WEEKDAY:
            return rrule(
                freq=freq,
                dtstart=start,
                interval=interval,
                until=until,
                bysetpos=-1,
                byweekday=WEEKDAYS,
            )

        raise ValueError('Month recurrence type not implemented: "{month_type}"')

    raise ValueError('Frequency not implemented: "{freq}"')



def parse_freq(s: str):
    try:
        freq = FreqValues(s)
    except ValueError:
        raise ValueError(f'Unknown freq: "{s}"')
    if freq == freq.WEEKLY:
        return WEEKLY
    if freq == freq.MONTHLY:
        return MONTHLY

    raise ValueError(f'Unknown freq: "{s}"')

def parse_week_days(raw: List[str]):
    return [ parse_week_day(s,i) for (i,s) in enumerate(raw) ]

def parse_week_day(s: str, i: int):
    try:
        week_day = WeekDayValues(s.lower())
    except ValueError:
        raise ValueError(f'Unknown weekday: "{s}" ({i})')
    if week_day == WeekDayValues.MONDAY:
        return MO
    if week_day == WeekDayValues.TUESDAY:
        return TU
    if week_day == WeekDayValues.WEDNESDAY:
        return WE
    if week_day == WeekDayValues.THURSDAY:
        return TH
    if week_day == WeekDayValues.FRIDAY:
        return FR
    if week_day == WeekDayValues.SATURDAY:
        return SA
    if week_day == WeekDayValues.SUNDAY:
        return SU

    raise ValueError(f'Unknown weekday: "{s}" ({i})')

def parse_month_type(s: str):
    try:
        return MonthTypeValues(s.lower())
    except ValueError:
        raise ValueError(f'Unknown month type: "{s}"')

def parse_month_days(raw: List[str]):
    return [ int(s) for s in raw ]

def parse_month_nth_days(raw: List[str]):
    return [ parse_month_nth_day(s,i) for (i,s) in enumerate(raw) ]

def parse_month_nth_day(s: str, i: int):
    try:
        month_nth_day = MonthNthDayValues(s)
    except ValueError:
        raise ValueError(f'Unknown Month nth value: "{s}" ({i})')
    if month_nth_day == MonthNthDayValues.FIRST:
        return 1
    if month_nth_day == MonthNthDayValues.SECOND:
        return 2
    if month_nth_day == MonthNthDayValues.THIRD:
        return 3
    if month_nth_day == MonthNthDayValues.FOURTH:
        return 4
    if month_nth_day == MonthNthDayValues.LAST:
        return -1

    raise ValueError(f'Unknown Month nth value: "{s}" ({i})')

 
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


