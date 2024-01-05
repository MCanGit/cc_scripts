import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# -- Manual Update required
shift_cutoff = 15
day = 'today'
# --


compiled = pd.read_excel(r"Z:\Operations - Management\WFM\Personal Folders\Pedro\aqui\attendance\IW_AttendanceCompilation_2023.xlsx",
                         usecols=['Username',
                                  'Date',
                                  'Status Final'])

adjust = pd.read_excel(r"Z:\Operations - Management\WFM\Personal Folders\Pedro\aqui\attendance\IW Adjustment.xlsx",
                       usecols=['Username',
                                  'Date',
                                  'Status Final'])

shift = pd.read_excel(r"Z:\Operations - Management\WFM\Personal Folders\Pedro\aqui\attendance\Shift Attendance.xlsx",
                      header=1,
                      usecols=['User Name', 
                               'Date',
                               'Event Name',
                               'Updated Status',
                               'System Generated Status',
                               'Shift Start'])



shift["Shift Start"] = pd.to_datetime(shift["Shift Start"])
shift["shift_start_str"] = pd.to_datetime(shift['Shift Start']).dt.strftime('%Y-%m-%d %H:%M:%S')

# Filter Shifts
def shift_filter_function(day):
    shift_filter = None
    if day == 'today':
        shift_filter = str(today + timedelta(hours=shift_cutoff))
    elif day == 'yesterday':
        shift_filter = str(yesterday + timedelta(hours=shift_cutoff))
    return shift_filter

today = datetime.today().date()
today = pd.to_datetime(today)
yesterday = today - timedelta(days=1)
shift_filter = shift_filter_function(day)
shift = shift[shift["shift_start_str"] <= shift_filter]

shift["Shift_Start"] = shift["Shift Start"].dt.tz_localize(None)

shift["Date"] = pd.to_datetime(shift["Date"])
shift["Status Final"] = shift["Event Name"]
shift["Status Final"] = np.where(shift["Status Final"].isnull(), shift['Updated Status'], shift["Status Final"])
shift["Status Final"] = np.where(shift["Status Final"].isnull(), shift['System Generated Status'], shift["Status Final"])
shift = shift.rename(columns={'User Name':'Username'})
shift = shift[['Username', 'Date', 'Status Final']]

df = pd.concat([compiled, adjust, shift])

df = df.drop_duplicates()
df["Date"] = df['Date'].dt.strftime('%m/%d/%Y')

dup_check = df.groupby(["Username", "Date"], as_index=False).size()
dup_check = dup_check.query('size != 1')
print(dup_check)

df.to_excel('Attendance 2023-2024.xlsx',index=False)