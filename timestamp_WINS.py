import pandas as pd

# --- Manual Change Required---
File = 'WINS Timestamp Template - 12.05 - Partial'

# --- File Path and Export Destination Path---
ImportPath = "//lisfs1003/Honey_Badger$/Operations - Management/Lisbon Reporting/02. Activity Code/02. Time Stamps/02. Timestamp - Transformation Backup/%s.xlsm" %File
ExportPath = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/000. Database/22. EMEA AWS/12. Timestamp/%s.parquet" % File

df = pd.read_excel(ImportPath, sheet_name='Time Stamp')

df = df.loc[:, :'Total Hours']

# Date and Week Ending
df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], format='%m-%d-%Y')
df["Week Ending"] = df["Transaction Date"] - pd.to_timedelta((df["Transaction Date"].dt.weekday-4)%-7, unit="d")

# Create Date string columns
df["week_ending_str"] = df["Week Ending"].dt.strftime('%m-%d-%Y')
df["date_str"] = df["Transaction Date"].dt.strftime('%m-%d-%Y')

# Replace empty spaces in column names
df.columns = df.columns.str.replace(" - ", "_")
df.columns = df.columns.str.replace(" ", "_")
df.columns = df.columns.str.replace("__", "_")

# SRT ID 
df['SRT_ID'] = df['SRT_ID'].astype('string')
df['SRT_ID'].replace(to_replace='#', value='', inplace=True, regex=True)

# Renaming
df.rename(
    columns=({
        'Transaction_Date': 'Date',
        'Assigned_To': 'Enterprise_ID',
        'Time_Elapsed_(Active)': 'Available',
        'Business':'business_unit'
    }), inplace=True)

df.columns = df.columns.str.lower()

# Change data type
df["non-srt_production"] = df["non-srt_production"].astype(float)
df["onboarding"] = df["onboarding"].astype(float)
df["day_onboarding"] = df["day_onboarding"].astype(float)
df["night_onboarding"] = df["night_onboarding"].astype(float)
df["non-fb_learning"] = df["non-fb_learning"].astype(float)

# Export
df.to_parquet(ExportPath)
#df.to_csv('//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/test.csv')

print('done')