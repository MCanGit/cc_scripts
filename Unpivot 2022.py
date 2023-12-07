# -- Unpivot
import pandas as pd
import numpy as np
import tempfile
import shutil
from datetime import datetime, timedelta
import os

# Run MMT_IW.py
#import MMT_IW

print(datetime.now())
# -- Manual update required
# Regular date - StartingDate = "4/29/2023"  EndingDate = "1/12/2024"
# Date in format m/d/yyyy (no leading zeros on month or day)
StartingDate = "1/1/2022"
EndingDate = "12/31/2022"

File = "CO - Master_Schedule - 2022"

MFCodes = "CODES MF.WH"
CrossTraining = "Heatmap_Cross&Trainee"
ExitsList = "CO IW Absenteeism v.12.5 ICT"
MMT_Daily = 'Latest_MMT'
AdjustmentList = 'IW_AttendanceCompilation_2023'
# --
ImportPath = r"Z:\Operations - Management\Lisbon Reporting\11. Extras\11. Mário\.01\20. Unpivot 2022\Copy of Master_Schedule 2022.xlsb"
#ImportPath = "//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/08. Daily Master File/%s.xlsb"%File
MFCodesPath = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/000. Database/14. Codes MF & WH/%s.xlsx"%MFCodes
CrossTrainingPath = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/57. Heatmap/06. CrossTraining-Magic List/%s.xlsx"%CrossTraining
ExitsListPath = "//lisfs1003/honey_badger$/Operations - Management/WFM/01. IW Report/CO/%s.xlsb"%ExitsList
MMT_DailyPath = r"Z:\Operations - Management\Lisbon Reporting\11. Extras\11. Mário\.01\20. Unpivot 2022\MMT_2022.parquet"
AdjustmentListPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/04. Adjustment List/%s.xlsx'%AdjustmentList
Path = "//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/09. Daily Unpivot/"

OutputName = str('Unpivot ' + File[-6:])
#ExportPath = '//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/09. Daily Unpivot/%s.csv'%OutputName

BackupName = str('Backup Unpivot ' + ' ' + File[-6:])
#BackupExportPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/13. Unpivot_MF Main/01. Backup/%s.csv'%BackupName

df = pd.read_excel(ImportPath, sheet_name= "Master_Schedule",  header= 2)
# Remove #N/A (#N/A happen when someone is added to MF before being added to Roster)
#df = df.query('NAME != "0x2a"')

#Establish the static column range to be use

df_cut1 = df.loc[ : , :"SHIFT BLOCK"]

#Establish the date column range to be use

df_cut2 = df.loc[ : , StartingDate : EndingDate]

#Concat the static  column with the dynamics ones (date columns)
df3 = pd.concat([df_cut1, df_cut2],axis=1)

#list = ["Analyst", "Quality & Support Analyst", "SME", "quality reviewer", "Trainee", "Team Lead"]
#df3 = df3.query('ROLE.isin(list)')

#Unpivot

df4 = df3.melt(id_vars= df3.loc[:,"EID":"SHIFT BLOCK"], var_name='date')

# -- All EIDs in lower case
df4["EID"] = df4["EID"].str.lower()

# -- Import MF Codes

mf_codes = pd.read_excel(MFCodesPath, sheet_name= "Codes", header=None)

mf_codes = mf_codes.rename(columns={0:"indice", 1:"code", 2:"description"})

mf_codes = mf_codes.query('indice == "MF/WH" or indice == "MF"')
codes_cond = [
    mf_codes.code.isin(["L1", "L2"]),
    mf_codes.code.str.endswith('1'),
    mf_codes.code.str.startswith('V'),
    mf_codes.code == 'T',
    mf_codes.code.isin(["COM", "COM(s)", "COE", "CON"]),
    mf_codes.code.str.endswith('ct'),
    mf_codes.code.str.endswith('fa'),
    mf_codes.code.str.endswith('mpl'),
    mf_codes.code.str.endswith('2'),
    mf_codes.code.str.endswith('t'),
    mf_codes.code.str.endswith('wri'),
    mf_codes.code.str.endswith('ml')
    ]

codes_result = [
    "Day Off",
    "LOA",
    "Vacation",
    "Trainees",
    "Vacation",
    "LOA",
    "LOA",
    "LOA",
    "LOA",
    "LOA",
    "LOA",
    "LOA"
    ]

mf_codes["codes_group"] = np.select(codes_cond, codes_result, 'Work')
mf_codes = mf_codes.drop(columns=["indice", "description"])
mf_codes = mf_codes.pivot(columns="codes_group", values="code")

# SRT ID
df4["SRT ID"] = df4["SRT ID"].astype('string')
df4["SRT ID"] = '#' + df4["SRT ID"]
df4["SRT ID"] = df4["SRT ID"].str[:-2]

# create week ending based on day of week
df4["date"] = pd.to_datetime(df4["date"], format='%m/%d/%Y', errors='ignore').astype('string')
df4["date"] = df4["date"].str.replace("44929", "1/3/2023")
df4["date"] = pd.to_datetime(df4["date"])
df4["week_ending"] = df4["date"] - pd.to_timedelta((df4["date"].dt.weekday-4)%-7, unit="d")

# -- Check against MMT Daily Roster
mmt_daily = pd.read_parquet(MMT_DailyPath)

df4["date_aux"] = pd.to_datetime(df4["date"], format='%m/%d/%Y').dt.strftime('%m/%d/%Y')
df4 = df4.merge(mmt_daily, how='left', left_on=['EID', 'date_aux'], right_on=['enterprise_id', 'roster_date'])

rename_mmt = {
    'role':'role_mmt',
    'employee_status':'employee_status_mmt',
    'cluster':'market_mmt(cluster)',
    'workflow':'LOB_mmt',
    'current_workflow':'SLOB_mmt',
    'team_lead':'team_lead_mmt',
    'ops_lead_eid':'ops_lead_mmt'
}

df4 = df4.rename(columns=rename_mmt)

# Fill NULLs from MMT Roster
df4 = df4.sort_values(["EID", "date"])
df4["LOB_mmt"] = df4.groupby('EID')["LOB_mmt"].fillna(method="ffill")
df4["SLOB_mmt"] = df4.groupby('EID')["SLOB_mmt"].fillna(method="ffill")
df4["role_mmt"] = df4.groupby('EID')["role_mmt"].fillna(method="ffill")
df4["employee_status_mmt"] = df4.groupby('EID')["employee_status_mmt"].fillna(method="ffill")
df4["market_mmt(cluster)"] = df4.groupby('EID')["market_mmt(cluster)"].fillna(method="ffill")
df4["team_lead_mmt"] = df4.groupby('EID')["team_lead_mmt"].fillna(method="ffill")
df4["ops_lead_mmt"] = df4.groupby('EID')["ops_lead_mmt"].fillna(method="ffill")
df4["skills"] = df4.groupby("EID")["skills"].fillna(method="ffill")
df4["billable"] = df4.groupby("EID")["billable"].fillna(method="ffill")


# Change V0 to "-"
df4["value"] = df4["value"].str.replace("V0", "-")

# -- Conditional Columns

# Schedule
df4["Schedule"] = np.where(df4["value"].isin(['L1', 'L2', '-']),0, 1)

# WD
df4["WD"] = np.where((df4["value"].isin(mf_codes["Work"])) |
                     (df4["value"].isin(mf_codes["Trainees"])), 1, 0)

# LOA
df4["LOA"] = np.where(df4["value"].isin(mf_codes["LOA"]), 1, 0)

# PTO
df4["PTO"] = np.where(df4["value"].isin(mf_codes["Vacation"]), 1, 0)

# ABS
#df4["ABS"] = np.where((df4["WD"] == 1) & (df4["Schedule"] < 1), 1, 0)
df4["ABS"] = np.where((df4["WD"] == 1) & (df4["rta_status"] == "Abs") & ((df4["srtf_total_hrs"] < 1) | (df4["srtf_total_hrs"].isnull())), 1, 0)

# Leave
df4["Leave"] = np.where(df4["value"].isin(["L1", "L2"]), 1, 0)

# Roll in and off Dates as Date
df4["ROLL OFF DATE"] = df4["ROLL OFF DATE"].replace(0, 2)
df4["ROLL IN DATE"] = pd.to_datetime(df4["ROLL IN DATE"].astype(int),  unit='d', origin='12/30/1899').dt.strftime('%m/%d/%Y')
df4["ROLL OFF DATE"] = pd.to_datetime(df4["ROLL OFF DATE"].astype(int), unit='d', origin='12/30/1899').dt.strftime('%m/%d/%Y')
df4["value"] = df4["value"].fillna('-')

# -- Change Trainee Role if it has WD Codes
trainees = df4.query('value in @mf_codes.Work and role_mmt == "Trainee"')
trainees = trainees.groupby(['EID'])['date'].min().to_frame('training_enddate').reset_index()
trainees = trainees.rename(columns={"EID":"EID_trainees"})

df4 = df4.merge(trainees, how='left', left_on=['EID', 'date'], right_on=['EID_trainees', 'training_enddate'])
df4["training_enddate"] = df4.groupby('EID')["training_enddate"].fillna(method="ffill")
df4["role_mmt"] = np.where((df4["role_mmt"] == "Trainee") &
                           (df4["date"] >= df4["training_enddate"]),
                             'Analyst', df4["role_mmt"])

# -- Create Business Unit column based on LOB from MMT Roster
pbu_cond = [
    df4["LOB_mmt"] == 'CO - Customer Support',
    df4["LOB_mmt"].str.startswith('CO -', na=False),
    df4["LOB_mmt"] == 'Non-Rec',
    df4["LOB_mmt"] == 'CO - Non-Rec'
]

pbu_results = [
    'CUSTOMER_SUPPORT',
    'COMMUNITY_OPS',
    'COMMUNITY_OPS',
    'COMMUNITY_OPS'
]

df4['PBU'] = np.select(pbu_cond, pbu_results, df4["LOB_mmt"])

# -- Import Change Log and update

df4["market_mmt(cluster)"] = df4["market_mmt(cluster)"].str.upper()

# WSC New
eidgroup = df4.groupby(['EID', 'week_ending'], as_index=False)[['Schedule', 'Leave']].sum()
eidgroup["schedule_aux"] = eidgroup["Schedule"] + eidgroup["Leave"]
eidgroup["scheduled_days"] = eidgroup["Schedule"]

eidWE = df4.groupby(['EID', 'week_ending', 'date'], as_index=False)[["Schedule", "Leave"]].sum()
eidWE = eidWE.merge(eidgroup[["EID", "week_ending", "schedule_aux", "scheduled_days"]], how='left', on=["EID", "week_ending"])

#eidWE["date"] = pd.to_datetime(eidWE["date"])
eidWE["weekday"] = eidWE["date"].dt.dayofweek
eidWE["weekday_aux"] = np.where((eidWE["weekday"] == 4) & (eidWE["Leave"] == 1) & (eidWE["scheduled_days"] == 4), 1, np.nan)
eidWE["weekday_aux"] = eidWE.groupby(['EID', 'week_ending'])['weekday_aux'].fillna(method="bfill")

wsc_cond = [
    (eidWE["schedule_aux"] == 7) & (eidWE["scheduled_days"] == 6),
    (eidWE["schedule_aux"] == 7) & (eidWE["weekday_aux"] == 1) & ((eidWE["Schedule"] == 1) | (eidWE["weekday"] == 4)) & (eidWE["scheduled_days"] <= 4),
    (eidWE["schedule_aux"] == 7) & (eidWE["weekday_aux"].isnull()) & (eidWE["Schedule"] == 1) & (eidWE["scheduled_days"] == 5),
    (eidWE["schedule_aux"] == 7) & (eidWE["weekday_aux"].isnull()) & ((eidWE["Schedule"] == 1) | (eidWE["Leave"] == 1)) & (eidWE["scheduled_days"] == 4),
    (eidWE["schedule_aux"] == 7) & (eidWE["weekday_aux"].isnull()) & ((eidWE["Schedule"] == 1) | (eidWE["Leave"] == 1)) & (eidWE["scheduled_days"] == 3),
    (eidWE["schedule_aux"] < 7) & (eidWE["Schedule"] == 1)
]

wsc_res = [
    5/7,
    1,
    1,
    5/7,
    5/7,
    1
]

eidWE["WSC_new"] = np.select(wsc_cond, wsc_res, np.nan)
eidWE["WSC_new"] = eidWE["WSC_new"].fillna(0)

#eidWE["date"] = eidWE["date"].astype("string")
df4 = df4.merge(eidWE[["EID", "date", "WSC_new"]], how='left', on=["EID", "date"])

# drop unnecessary columns
df4 = df4.drop(columns=["Training FLOW",
                        "End date",
                        "enterprise id",
                        "employment end date",
                        "date_aux",
                        "enterprise_id",
                        "Change_Type",
                        "e_id",
                        "Old",
                        "New",
                        "Effective_Date",
                        "Until_Date",
                        "Business Unit",
                        "roster_date",
                        "count_days",
                        "Exit_date",
                        "count_wd",
                        "weekending",
                        "training_enddate"
                        ], errors='ignore')

#Shift Column
shift_cond = [
    df4["value"].str.contains("E", na=False),
    df4["value"].str.contains("N", na=False),
    df4["value"].str.contains("M(s)", na=False, regex=False),
    df4["value"].str.contains("M", na=False)
    ]

shift_res = [
    "Evening",
    "Night",
    "Middle",
    "Morning"
]

df4["shift"] = np.select(shift_cond, shift_res, "-")

# -- Date (datetime64) to string for faster export times 'unplanned_abs', 'planned_loa', 'planned_pto', 'scheduled_rta'
df4["date"] = df4["date"].astype(str)
df4["week_ending"] = df4["week_ending"].astype(str)
df4["WAVE"] = df4["WAVE"].astype(str)

df4 = df4[['EID', 'role_mmt', 'NAME', 'SRT ID', 'WAVE', 'employee_status_mmt', 'ROLL IN DATE', 'ROLL OFF DATE',
 'team_lead_mmt', 'ops_lead_mmt', 'market_mmt(cluster)', 'skills','PBU', 'SLOB_mmt', 'LOB_mmt', 'SHIFT BLOCK', 'date', 'value', 'shift', 'week_ending', 'Schedule',
 'WD', 'LOA', 'PTO', 'ABS', 'Leave', 'iw_highlevel_status', 'iw_attendance_status','status_final_AL', 'rta_status',
 'rta_status_code', 'srtf_total_hrs', 'srtf_completed_time', 'iw_actual_time', 'WH', 'WSC_new', 'billable']]

rename_cols = {
    "role_mmt":"ROLE",
    "employee_status_mmt":"STATUS",
    "team_lead_mmt":"SUPERVISOR CURRENT",
    "ops_lead_mmt":"OPS",
    "market_mmt(cluster)":"MARKET",
    "SLOB_mmt":"SLOB",
    "LOB_mmt":"LOB",
    "skills":"Flow Skills",
    "WSC_new":"WSC"
    }

df4 = df4.rename(columns=rename_cols)

# Checks
duplicate_check = df4.groupby(["EID", "date"], as_index=False).size()
duplicate_check = duplicate_check.query('size != 1')
print("Duplicates:")
print(duplicate_check.EID.unique())
print(df4["value"].unique())
PDOers = ["carlos.j.j.bolivar", "g.fialho.realista", "lucas.loureiro", "s.amaro.de.castro", "ines.b.raposo", "inma.g.gomez", "tiago.vidal"]
exit_check = df4.query('ROLE == "Analyst" and value == "-" and rta_status == "Work" and EID not in @PDOers')
print("Exit Check:")
print(exit_check.EID.unique())

# -- Creating temporary folder in local drive to export files into and copying files to share drive
# solution employed because exporting directly to share drive takes considerably more time
# than exporting to local disk and transfering to share drive
with tempfile.TemporaryDirectory() as tempdir:
    ExportPath = tempdir + '\\' + OutputName + '.csv'
    #df4.to_csv(ExportPath, index=False)
    mainsrc = ExportPath
    maindst = "//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/09. Daily Unpivot/"
    #shutil.copy(src=mainsrc, dst=maindst)
    BackupExportPath = tempdir + '\\' + BackupName + '.csv'
    df4.to_csv(BackupExportPath, index=False)
    backupsrc = BackupExportPath
    backupdst = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/13. Unpivot_MF Main/"
    shutil.copy(src=backupsrc, dst=backupdst)


Path = os.path.realpath(Path)
os.startfile(Path)
print(datetime.now())

#df4.to_csv("//projectportugaltest.file.core.windows.net/mstestfolder/Test CC/testing.csv")
#df4.to_csv('//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/02. IW Daily/IW Unp_test.csv')
