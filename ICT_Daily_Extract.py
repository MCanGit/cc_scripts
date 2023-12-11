import numpy as np
import pandas as pd
from datetime import datetime, timedelta


shift_cutoff = 23
day = 'today'


File_Path = r"Z:\Operations - Management\Lisbon Reporting\26. WFM (Reporting)\15. MMT_IW\Attendance 2023-11-27-2023-12-11.xlsx"


df = pd.read_excel(File_Path, header=1, usecols=["Primary Group",
                                                 "User Name",
                                                 "Primary Supervisor",
                                                 "Date",
                                                 "System Generated Status",
                                                 "Event Name",
                                                 "Updated Status",
                                                 "Shift Start",
                                                 "Shift End",
                                                 "First Check In",
                                                 "Last Check Out",
                                                 "Time Checked In (min)",
                                                 "Last Modified By",
                                                 "Last Modified At"])


df.columns = df.columns.str.replace(" ", "_")
df["Shift_Start"] = pd.to_datetime(df["Shift_Start"])
df["shift_start_str"] = pd.to_datetime(df.Shift_Start).dt.strftime('%Y-%m-%d %H:%M:%S')

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
df = df[df["shift_start_str"] <= shift_filter]

# Remove Timezone from column
df["Shift_Start"] = df["Shift_Start"].dt.tz_localize(None)

# Status Final Column
df["status_final"] = df["Event_Name"]
df["status_final"] = np.where(df["status_final"].isnull(), df["Updated_Status"], df["status_final"])
df["status_final"] = np.where(df["status_final"].isnull(), df["System_Generated_Status"], df["status_final"])

# save backup
backup_name = 'Backup' + str(shift_filter[:10])
backup_path = "//lisfs1003/honey_badger$//Operations - Management/WFM/01. IW Report/CO/Daily Report wip/Daily Extract/Daily Extract Backup/%s.csv"%backup_name
df.to_csv(backup_path, index=False)
