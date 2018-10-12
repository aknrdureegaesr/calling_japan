
import calendar, datetime, os, sys, time, urllib.request, zipfile
import pandas as pd 

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
                print("INFO: Writing {0}.".format(csv_filename))
                with open(csv_filename, "wb") as csv:
                    csv.write(csv_from_zip.read())
    sys.stderr.write('.')
    return csv_filename

def pull_rbn_month(year, month):
    cal = calendar.Calendar()
    # This is crude.  It happens to work in time zones not too
    # different from UTC when called not too far close to midnight.
    yesterday = datetime.date.fromtimestamp(time.time() - 3600 * 24)
    dfs = []
    dtype = {'freq': float, 'db': float, 'speed': float}
    for date in cal.itermonthdates(year, month):
        if date.month == month and date <= yesterday:
            csv_filename = pull_rbn(date)
            df = pd.read_csv(csv_filename, keep_default_na=False, \
                             engine='python', skipfooter=1, \
                             dtype = dtype, \
                             parse_dates = ['date'])
            dfs.append(df)
    sys.stderr.write(" OK.\n{0} files input\n".fomat(len(dfs)))
    return pd.concat(dfs)
