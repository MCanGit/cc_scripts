# 05 SRTF Lisbon
import numpy as np
import pandas as pd
import os

# --- Manual Change Required---
File = 'Lisbon SRT & Onboarding - 12.09 - 12.15 (WW50)'
# --- Manual Change Required---

ImportPath = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/02. Activity Code/04. Billable Hours/03. SRT File sent (client format)/03. Data/2023/%s.csv" % File
ExportPath = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/000. Database/22. EMEA AWS/08. SRTF/%s.parquet" % File

# Import csv file with data with specified columns only to reduce data size
df = pd.read_csv(ImportPath,
                 usecols=[
                     'Work City',
                     'Week Ending',
                     'Transaction Date',
                     'Process',
                     'Assigned To',
                     'SRT ID',
                     'Staffing Market',
                     'Language',
                     'Type',
                     'Position',
                     'Time Elapsed (Active)',
                     'Breaks',
                     'Coaching',
                     'FB Learning',
                     'Onboarding',
                     'Meal',
                     'Non-FB  Learning',
                     'Meeting',
                     'Well-being',
                     'Non-SRT Production',
                     'Completed Time',
                     'Total Hours',
                     'Adjustment Types',
                     'Comment',
                     'HC',
                     'Business'
                 ])

# DUBfix
df['Work City'].replace(to_replace='Dublin,Ireland', value='Dublin, Ireland', inplace=True, regex=True)

# SRT ID prep
df['SRT ID'] = df['SRT ID'].astype('string')
df['SRT ID'].replace(to_replace='#', value='', inplace=True, regex=True)

# Market Capitalization
df['Staffing Market'] = df['Staffing Market'].str.upper()

# Week Normalization
df['Week Ending'] = pd.to_datetime(df['Week Ending'])
df['Week Ending'] = df['Week Ending'].dt.strftime('%m-%d-%Y')
df['Week Ending'] = df['Week Ending'].astype('string')

df['Week Ending Str'] = df['Week Ending']

df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
df['Transaction Date'] = df['Transaction Date'].dt.strftime('%m-%d-%Y')
df['Transaction Date'] = df['Transaction Date'].astype('string')

# Renaming
df.rename(
    columns=({
        'Transaction Date': 'Date',
        'Assigned To': 'Enterprise ID',
        'Time Elapsed (Active)': 'Available',
        'Non-FB  Learning': 'Non-FB Learning',
        'Business':'business unit'
    }), inplace=True)

# Data Types Change
#df['Non-SRT Production'] = df['Non-SRT Production'].str.replace("-", "0")
df['Non-SRT Production'] = df['Non-SRT Production'].astype('float')
df['Onboarding'] = df['Onboarding'].astype('float')


print(df.info())
df.to_parquet(path=ExportPath, engine='auto', compression='none')