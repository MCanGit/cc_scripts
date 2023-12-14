import pandas as pd
from pandas.tseries.offsets import *

# --- Manual Change Required---
RWeek='12-15-2023'
File = 'GLOBAL_ROSTER Lisbon WE120823'
# --- Manual Change Required---

# Path to folder -- adjust if required
ImportPath = "//lisfs1003/honey_badger$/Operations - Management/WFM/Weekly Roster HC Reports/TnS Roster WE/%s.xlsx"%File
ExportPath = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/000. Database/22. EMEA AWS/Weekly TNS Roster/TNS Lisbon %s.parquet"%RWeek

df = pd.read_excel(ImportPath, usecols=[
    'Location',
    'Enterprise ID',
    'Resource Name',
    'SRT ID',
    'SRT Name',
    'Role',
    #"SOW",
    #"Phase"
    'PBU', #mudar nome para Local Team Name
    'Workflow',
    'Current Workflow',
    'Sub-Workflow',
    'Microflow',
    'Planning Group', #mudar nome para Market / Base Area / Pillar
    'Language',
    'Team Lead EID',
    'Group Lead EID',
    'Roll Off Date',
    #'OJT Start Date',
    'Employee Status',
    #'Date of Movement',
    #'Resignation Date'
])

# Rename columns due to TnS Roster Change 03.2023
df.rename(columns={'Planning Group': 'Market / Base Area / Pillar'}, inplace=True)
df.rename(columns={'PBU': 'Local Team Name'}, inplace=True)

# Add Program Column
df.insert(0, "Program", "CO")

#Add missing columns due to TnS Roster change
df.insert(7, "SOW", "")
df.insert(8, "Phase", "")
df.insert(19, "OJT Start Date", "")
df.insert(len(df.columns), "Date of Movement", "")
df.insert(len(df.columns), "Resignation Date", "")

# SRTID Correction
df['SRT ID'] = df['SRT ID'].astype('string')
df['SRT ID'] = df['SRT ID'].str[:-2]

df['Team Lead EID'] = df['Team Lead EID'].str.lower()

df.replace(to_replace='%26', value=' & ', inplace=True, regex=True)
df['Program'].replace(to_replace='CO', value='COMMUNITY_OPS ', inplace=True, regex=True)
df['Program'].replace(to_replace='PDO', value='PRODUCT_DATA_OPS ', inplace=True, regex=True)

# Creation of weekend
df['Roster Week Index'] = RWeek
df['Roster Week Index'] = df['Roster Week Index'].astype('string')

# Data Type Change
df['Roll Off Date'] = df['Roll Off Date'].astype('string')
#df['Date of Movement'] = df['Date of Movement'].astype('string')
#df['Resignation Date'] = df['Resignation Date'].astype('string')
#df['OJT Start Date'] = df['OJT Start Date'].astype('string')

#df['OJT Start Date'].replace(to_replace='1/1/1900', value='1900-01-01', inplace=True, regex=True)

RepRoles = {
    'Reviewer':'Analyst',
    'Operation Team Lead':'Team Lead'
    }
df = df.replace({"Role":RepRoles})
df["SRT Name"] = df["SRT Name"].str.replace("0", "NA")

df.to_parquet(path=ExportPath, engine='auto', compression='none')
#df.to_excel("//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/11. Extras/11. Mário/2. Roster Tns/test_new roster_test_WE032423.xlsx", index=False)
