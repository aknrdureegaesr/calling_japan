
import calendar, datetime, os, re, sys, time, urllib.request, zipfile
import pandas as pd
import numpy as np

def pull_rbn(date):
    os.makedirs("zip", exist_ok = True)
    os.makedirs("csv", exist_ok = True)
    basename = "{0:04d}{1:02d}{2:02d}".format(date.year, date.month, date.day)
    zip_filename = "zip/{0}.zip".format(basename)
    csv_filename = "csv/{0}.csv".format(basename)
    if not os.path.exists(zip_filename):
        with urllib.request.urlopen("http://www.reversebeacon.net/raw_data/dl.php?f={0}.zip".format(basename)) as resp:
            if resp.getcode() != 200:
                raise RuntimeError("Could not retrieve {0}: {1}: {2}".format(basename, resp.getcode(), resp.msg))
            with open(zip_filename, "wb") as f:
                f.write(resp.read())
    if not os.path.exists(csv_filename):
        with zipfile.ZipFile(zip_filename) as zf:
            with zf.open("{}.csv".format(basename)) as csv_from_zip:
                csv_raw = csv_from_zip.read()
            final_row = re.search(b'\(\d+ rows\)', csv_raw)
            if final_row:
                with open(csv_filename, "wb") as csv:
                    csv.write(csv_raw[0:final_row.start()])
            else:
                raise RuntimeError("Could not find expected end line in csv from {0}".format(zip_filename))
    sys.stderr.write('.')
    return csv_filename

def read_csv_file(csv_filename):
    split_band_re_m = re.compile(r"(\d+)m")
    split_band_re_cm = re.compile(r"(\d+)cm")
    fl_type = np.dtype("float64")

    count = 0

    def band2num(band):
        nonlocal count, split_band_re_m, split_band_re_cm, fl_type
        m = split_band_re_m.match(band)
        if m:
            return fl_type.type(m.group(1))
        elif band == '472kHz':
            return fl_type.type(636)
        else:
            m2 = split_band_re_cm.match(band)
            if m2:
                return fl_type.type(m2.group(1)) / 100
            else:
                raise RuntimeError("Could not parse the band \"{}\".".format(band))

    converters = {'band': band2num}
    dtype = {'freq': float, 'db': float, 'speed': float}
    df = pd.read_csv(csv_filename, keep_default_na=False, \
                     dtype = dtype, engine = 'c', \
                     converters = converters, \
                     parse_dates = ['date'])
    return df

def pull_rbn_month(year, month):
    cal = calendar.Calendar()
    # This is crude.  It happens to work in time zones not too
    # different from UTC when called not too far close to midnight.
    yesterday = datetime.date.fromtimestamp(time.time() - 3600 * 24)
    dfs = []
    for date in cal.itermonthdates(year, month):
        if date <= yesterday:
            csv_filename = pull_rbn(date)
            df = read_csv_file(csv_filename)
            dfs.append(df)

    sys.stderr.write(" OK.\n{0} files input\n".format(len(dfs)))
    return pd.concat(dfs)
