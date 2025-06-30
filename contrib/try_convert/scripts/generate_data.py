import random
import datetime
import time
import pytz

random.seed(42)

### NUMBERS

numbers = {
    'int2' : ((-32768, 32767), False),
    'int4' : ((-2147483648, 2147483647), False),
    'int8' : ((-9223372036854775808, 9223372036854775807), False),
    'float4' : ((10**6, 10**6), True),
    'float8' : ((10**15, 10**15), True),
    'numeric' : ((10**20, 10**20), True),
}


def save_datafile(t, data):
    filename = f'data/tt_{t}.data'
    file = open(filename, 'w')
    file.write('\n'.join([str(d) for d in data]))

    return filename


for number_type in numbers:

    type_range = numbers[number_type][0]
    is_float = numbers[number_type][1]

    mn = type_range[0]
    mx = type_range[1]

    nln = 0
    ln = len(str(type_range[1]))-1

    table_name = f'tt_{number_type}'

    values = []

    for c in range(nln, ln):
        rep = 20 // ln + 1
        for _ in range(rep):
            n = random.random()
            if c >= 0:
                n *= (10 ** (c+1))
            else:
                n /= (10 ** (-c))
            if not is_float:
                n = int(n)
            
            values += [n]

    filename = save_datafile(number_type, values)

### TIMES

MINTIME = datetime.datetime.fromtimestamp(0)
MAXTIME = datetime.datetime(2024,12,2,10,39,59)
mintime_ts = int(time.mktime(MINTIME.timetuple()))
maxtime_ts = int(time.mktime(MAXTIME.timetuple()))

timestamp_values = []
timestamptz_values = []
time_values = []
timetz_values = []
date_values = []
interval_values = []

timezones = [pytz.timezone("UTC"), pytz.timezone("Asia/Istanbul"), pytz.timezone("US/Eastern"), pytz.timezone("Asia/Tokyo")]

for _ in range(12):
    random_ts = random.randint(mintime_ts, maxtime_ts)
    randtom_tz = timezones[random.randint(0, len(timezones)-1)]
    RANDOMTIME = datetime.datetime.fromtimestamp(random_ts, randtom_tz)
    R_clear = datetime.datetime.fromtimestamp(RANDOMTIME.timestamp())
    
    timestamp_text = str(R_clear)
    timestamptz_text = str(RANDOMTIME)
    timetz_text = str(RANDOMTIME.time()) + " " + RANDOMTIME.tzname()
    time_text = str(RANDOMTIME.time())
    date_text = str(RANDOMTIME.date())
    dt = (R_clear-MINTIME)
    interval_text = str(dt)

    timestamp_values += [timestamp_text]
    timestamptz_values += [timestamptz_text]
    time_values += [time_text]
    timetz_values += [timetz_text]
    date_values += [date_text]
    interval_values += [interval_text]

save_datafile("timestamp", timestamp_values)
save_datafile("timestamptz", timestamptz_values)
save_datafile("time", time_values)
save_datafile("timetz", timetz_values)
save_datafile("date", date_values)
save_datafile("interval", interval_values)
