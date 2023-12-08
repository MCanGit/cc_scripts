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
StartingDate = "4/29/2023"
EndingDate = "1/12/2024"

File = "CO - Master_Schedule - 120823"

MFCodes = "CODES MF.WH"
CrossTraining = "Heatmap_Cross&Trainee"
ExitsList = "CO IW Absenteeism v.12.5"
MMT_Daily = 'Latest_MMT'
AdjustmentList = 'IW_AttendanceCompilation_2023'
# --

#ImportPath = "//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/08. Daily Master File/%s.xlsb"%File
#MFCodesPath = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/000. Database/14. Codes MF & WH/%s.xlsx"%MFCodes
#CrossTrainingPath = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/57. Heatmap/06. CrossTraining-Magic List/%s.xlsx"%CrossTraining
#ExitsListPath = "//lisfs1003/honey_badger$/Operations - Management/WFM/01. IW Report/CO/%s.xlsb"%ExitsList
#MMT_DailyPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/01. Parquet/%s.parquet'%MMT_Daily
#AdjustmentListPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/04. Adjustment List/%s.xlsx'%AdjustmentList
#Path = "//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/09. Daily Unpivot/"

# WFH Paths
ImportPath = "C:\\Users\\mario.canudo\\Desktop\\Unpivot WFH\\06. Daily Master File\\%s.xlsb"%File
MFCodesPath = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\03. Codes\CODES MF.WH.xlsx"
CrossTrainingPath = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\01. Magic List\Heatmap_Cross&Trainee.xlsx"
ExitsListPath = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\04. RTA\CO IW Absenteeism v.12.5 ICT.xlsb"
MMT_DailyPath = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\02. MMT\Parquet\Latest_MMT.parquet"
AdjustmentListPath = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\04. RTA\Adjustment List\IW_AttendanceCompilation_2023.xlsx"


OutputName = str('Unpivot ' + File[-6:])
#ExportPath = '//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/09. Daily Unpivot/%s.csv'%OutputName

BackupName = str('Backup Unpivot ' + ' ' + File[-6:])
#BackupExportPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/13. Unpivot_MF Main/01. Backup/%s.csv'%BackupName

df = pd.read_excel(ImportPath, sheet_name= "Master_Schedule",  header= 3)

# Remove #N/A (#N/A happen when someone is added to MF before being added to Roster)
df = df.query('NAME != "0x2a"')

# Drop LERT
#df = df.query('LOB != "Legal Operations"')

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

# -- Import Cross Skilling
cross_sheetname = "CrossSkilling"
crosstraining = pd.read_excel(CrossTrainingPath, sheet_name=cross_sheetname, usecols=["Business Unit", "Training FLOW", "EID", "End date"])
crosstraining = crosstraining.rename(columns={"EID":"EID_C"})
today = datetime.today() - timedelta(days=2)
today = today.strftime("%Y-%m-%d")
crosstraining = crosstraining[crosstraining["End date"].astype(str) > today]
crosstraining["End date_aux"] = crosstraining["End date"] + timedelta(days=1)
#crosstraining["cross_training"] = crosstraining["Business Unit"].map(str) + "_" + crosstraining["Training FLOW"].map(str)
#crosstraining["cross_training"] = crosstraining["cross_training"].str.replace('COMMUNITY_OPS', 'CO')
#crosstraining["cross_training"] = crosstraining["cross_training"].str.replace('CUSTOMER_SUPPORT', 'CS')

df4 = df4.merge(crosstraining, how='left', left_on=["EID", "date"], right_on=["EID_C", "End date_aux"])

df4["Business Unit"] = df4.groupby("EID")["Business Unit"].fillna(method="ffill")
df4["Training FLOW"] = df4.groupby("EID")["Training FLOW"].fillna(method="ffill")
df4["End date_aux"] = df4.groupby("EID")["End date_aux"].fillna(method="ffill")

# -- Import MMT Daily Roster
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

# Exit List - all codes are substituted for '-' after the employment end date
exitlist = pd.read_excel(ExitsListPath,
                         sheet_name="Exit List",
                         usecols=["Enterprise ID", "Employment End Date"]
                         )
exitlist = exitlist.rename(columns={"Enterprise ID":"enterprise_id_el", 
                                    "Employment End Date":"employment_end_date_el"})

exitlist = exitlist.query('employment_end_date_el  != "9/16/2023"')
exitlist["employment_end_date_el"] = exitlist["employment_end_date_el"].astype(int)

exitlist = exitlist.drop_duplicates(subset='enterprise_id_el', keep='first')
exitlist["employment_end_date_el"] = pd.to_datetime(exitlist["employment_end_date_el"], unit='D', origin='12-30-1899')
exitlist = exitlist.query('employment_end_date_el > @today')

df4 = df4.merge(exitlist, how='left', left_on=['EID', 'date'], right_on=['enterprise_id_el', 'employment_end_date_el'])
df4["employment_end_date_el"] = df4.groupby('EID')["employment_end_date_el"].fillna(method='ffill')

df4["value"] = np.where((df4["employment_end_date_el"].notnull()) &
                        (df4["date"] > df4["employment_end_date_el"]) &
                        ((df4["ROLL OFF DATE"] == 0) | (df4["ROLL OFF DATE"].isnull())),
                        '-', df4["value"])

df4["roll_off/reassignment"] = None
df4["roll_off/reassignment"] = np.where((df4["employment_end_date_el"].notnull()) &
                        (df4["date"] == df4["employment_end_date_el"]) &
                        ((df4["ROLL OFF DATE"] == 0) | (df4["ROLL OFF DATE"].isnull())),
                        'roll_off', df4["roll_off/reassignment"])

# Change V0 to "-"
df4["value"] = df4["value"].str.replace("V0", "-")

# DMR
df4["OpsDeploymentDate"] = df4.groupby('EID')['OpsDeploymentDate'].fillna(method="ffill")
df4["Graduation_Date"] = df4.groupby('EID')['Graduation_Date'].fillna(method="ffill")

dmr_cond = [
    df4["date"] < df4["OpsDeploymentDate"],
    (df4["date"] >= df4["OpsDeploymentDate"]) & (df4["date"] < df4["Graduation_Date"]),
    (df4["date"] >= df4["Graduation_Date"]),
    (df4["OpsDeploymentDate"].isnull()) & (df4["ROLL IN DATE"] < 44927),
    (df4["OpsDeploymentDate"].isnull()) & (df4["employee_status_mmt"] == "Active") &
    (df4["date"] > (pd.to_datetime(df4["ROLL IN DATE"].astype(int),  unit='d', origin='12/30/1899') + timedelta(days=19))),
    (df4["OpsDeploymentDate"].isnull()) & (df4["employee_status_mmt"] == "Active") &
    (df4["date"] <= (pd.to_datetime(df4["ROLL IN DATE"].astype(int),  unit='d', origin='12/30/1899') + timedelta(days=19)))
]

dmr_res = [
    'Trainee',
    'DMR',
    'Tenured',
    'Tenured',
    'Tenured',
    'Trainee'
    ]

df4["tenure"] = np.select(dmr_cond, dmr_res, None)
df4["tenure"] = df4.groupby('EID')['tenure'].fillna(method="ffill")

# secret exit list
secretExitList = pd.read_excel(CrossTrainingPath, sheet_name='Secret Exit List')

#secretExitList = secretExitList.query('exit_date > @today')
df4 = df4.merge(secretExitList[["e_id", "exit_date", "New"]], how='left', left_on=['EID', 'date'],
                 right_on=['e_id', 'exit_date'])

df4['exit_date'] = df4.groupby('EID')['exit_date'].fillna(method='ffill')
df4['New'] = df4.groupby('EID')['New'].fillna(method='ffill')
df4["value"] = np.where((df4["date"] > df4["exit_date"]) &
                        (df4["EID"].isin(secretExitList["e_id"])),
                        "-", df4["value"])

df4["roll_off/reassignment"] = np.where(df4["date"] == df4["exit_date"], 'roll_off', None)

df4 = df4.drop(columns=['e_id', 'New', 'exit_date'])

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
df4["ABS"] = np.where((df4["WD"] == 1) & (df4["rta_status"] == "Abs"), 1, 0)

# Leave
df4["Leave"] = np.where(df4["value"].isin(["L1", "L2"]), 1, 0)

# Roll in and off Dates as Date
df4["ROLL OFF DATE"] = df4["ROLL OFF DATE"].replace(0, 2)
df4["ROLL IN DATE"] = pd.to_datetime(df4["ROLL IN DATE"].astype(int),  unit='d', origin='12/30/1899').dt.strftime('%m/%d/%Y')
df4["ROLL OFF DATE"] = pd.to_datetime(df4["ROLL OFF DATE"].astype(int), unit='d', origin='12/30/1899').dt.strftime('%m/%d/%Y')
df4["value"] = df4["value"].fillna('-')

# Update roll off column
df4["roll_off/reassignment"] = np.where((df4["ROLL OFF DATE"] != 0) &
                                       (df4["ROLL OFF DATE"] == df4["date"]),
                                       'roll_off', df4["roll_off/reassignment"])


# Update status after roll-off date
df4["employee_status_mmt"] = np.where(((df4["employee_status_mmt"] == "Active") | (df4["employee_status_mmt"].isnull())) &
                         (df4["date"] > df4["ROLL OFF DATE"]) & (df4["ROLL OFF DATE"] != '01/01/1900'),
                         "Rolled-Off", df4["employee_status_mmt"])

# -- Change Trainee Role if it has WD Codes
trainees = df4.query('value in @mf_codes.Work and role_mmt == "Trainee"')
trainees = trainees.groupby(['EID'])['date'].min().to_frame('training_enddate').reset_index()
trainees = trainees.rename(columns={"EID":"EID_trainees"})

df4 = df4.merge(trainees, how='left', left_on=['EID', 'date'], right_on=['EID_trainees', 'training_enddate'])
df4["training_enddate"] = df4.groupby('EID')["training_enddate"].fillna(method="ffill")
df4["role_mmt"] = np.where((df4["role_mmt"] == "Trainee") &
                           (df4["date"] >= df4["training_enddate"]),
                             'Analyst', df4["role_mmt"])

# Change Billable status after turning Analyst
njoiners = df4.query('value in @mf_codes.Work and role_mmt == "Analyst" and billable == "non-billable"')
njoiners = njoiners.groupby(['EID'])['date'].min().to_frame('nonbill_enddate').reset_index()
njoiners = njoiners.rename(columns={"EID":"EID_njoiners"})

df4 = df4.merge(njoiners, how='left', left_on=['EID', 'date'], right_on=['EID_njoiners', 'nonbill_enddate'])
df4["nonbill_enddate"] = df4.groupby('EID')["nonbill_enddate"].fillna(method="ffill")
df4["billable"] = np.where((df4["role_mmt"] == "Analyst") &
                           (df4["employee_status_mmt"] == "Active") &
                           (df4["date"] >= df4["nonbill_enddate"]),
                             'billable', df4["billable"])

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

# Cross Skilling - SLOB is substituted with Cross Skilling Training Flow after the training end date
df4['PBU'] = np.where((df4["date"] >= df4["End date_aux"]) & (df4["End date_aux"].astype(str) > today), df4["Business Unit"], df4["PBU"])
df4["SLOB_mmt"] = np.where((df4["date"] >= df4["End date_aux"]) & (df4["End date_aux"] > today), df4["Training FLOW"], df4["SLOB_mmt"])
df4["SLOB_mmt"] = df4["SLOB_mmt"].str.replace('OBJECTS/IGPR', 'Objects/IGPR')
df4["SLOB_mmt"] = df4["SLOB_mmt"].str.replace('MESSENGER', 'Messenger')

df4["LOB_mmt"] = np.where((df4["PBU"] == "LERT") &
                          (df4["SLOB_mmt"] == "LERT"),
                          "LERT", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["SLOB_mmt"] == 'Messenger') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Content Moderation', df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["SLOB_mmt"] == 'Content Moderation') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Content Moderation', df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["SLOB_mmt"] == 'Objects/IGPR') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Content Moderation', df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["SLOB_mmt"] == 'MISREPRESENTATION') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Graph Integrity', df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["SLOB_mmt"] == 'Non-Rec') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Non-Rec', df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["SLOB_mmt"] == 'GTM') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - GTM', df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["SLOB_mmt"] == 'IDVAAS') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Graph Integrity', df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["SLOB_mmt"] == 'IDVAAS') & (df4['PBU'] == 'CUSTOMER_SUPPORT'), 'CO - Customer Support', df4["LOB_mmt"])

# -- Import Change Log and update
changelog = pd.read_excel(CrossTrainingPath, sheet_name='Change Log')

today = datetime.today() - timedelta(days=7)
today = today.strftime("%Y-%m-%d")
changelog = changelog[changelog['Effective_Date'].astype(str) >= today]

changelogPBU = changelog.query('Change_Type == "PBU"')

df4 = df4.merge(changelogPBU, how='left', left_on=['EID', 'date'], right_on=["e_id", "Effective_Date"])

df4["Effective_Date"] = df4.groupby('EID')["Effective_Date"].fillna(method="ffill")
df4["New"] = df4.groupby('EID')["New"].fillna(method="ffill")
df4["Change_Type"] = df4.groupby('EID')["Change_Type"].fillna(method="ffill")

df4["PBU"] = np.where((df4["Change_Type"] == "PBU") & (df4["date"] >= df4["Effective_Date"]),
                      df4["New"], df4["PBU"])

df4 = df4.drop(columns=["Change_Type", "Effective_Date", "New", "e_id"])

# Change Log SLOB
chSLOB = changelog.query('Change_Type == "SLOB"')
df4 = df4.merge(chSLOB, how='left', left_on=['EID', 'date'], right_on=["e_id", "Effective_Date"])

df4["Effective_Date"] = df4.groupby('EID')["Effective_Date"].fillna(method="ffill")
df4["New"] = df4.groupby('EID')["New"].fillna(method="ffill")
df4["Change_Type"] = df4.groupby('EID')["Change_Type"].fillna(method="ffill")

df4["SLOB_mmt"] = np.where((df4["Change_Type"] == "SLOB") & (df4["date"] >= df4["Effective_Date"]), df4["New"], df4["SLOB_mmt"])

df4["LOB_mmt"] = np.where((df4["PBU"] == "LERT") &
                          (df4["SLOB_mmt"] == "LERT"),
                          "LERT", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where(df4["PBU"] == "CUSTOMER_SUPPORT", "CO - Customer Support", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB_mmt"] == 'MISREPRESENTATION'),
                          "CO - Graph Integrity", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB_mmt"] == 'Messenger'),
                          "CO - Content Moderation", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB_mmt"] == 'Objects/IGPR'),
                          "CO - Content Moderation", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB_mmt"] == 'Content Moderation'),
                          "CO - Content Moderation", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB_mmt"] == 'GTM'),
                          "CO - GTM", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB_mmt"] == 'Non-Rec'),
                          "CO - Non-Rec", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["PBU"] == "COMMUNITY_OPS") & 
                          (df4["SLOB_mmt"] == "IDVAAS"),
                          "CO - Graph Integrity", df4["LOB_mmt"])


df4["SLOB_mmt"] = np.where(df4["LOB_mmt"] == "CO - Customer Support", "IDVAAS", df4["SLOB_mmt"])

#df4["SLOB_mmt"] = np.where(df4["LOB_mmt"] == "CO - Non-Rec", "Non-Rec", df4["SLOB_mmt"])
#df4["SLOB_mmt"] = np.where(df4["LOB_mmt"] == "Non-Rec", "Non-Rec", df4["SLOB_mmt"])
df4["LOB_mmt"] = np.where((df4["SLOB_mmt"] == 'Policy') & (df4["EID"].isin(df4["e_id"])),
                          "CO - PTQ", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where((df4["SLOB_mmt"] == 'Training') & (df4["EID"].isin(df4["e_id"])),
                          "CO - PTQ", df4["LOB_mmt"])


df4 = df4.drop(columns=["Change_Type", "Effective_Date", "New", "e_id"])

# Change Log Role
chRole = changelog.query('Change_Type == "Role"')
df4 = df4.merge(chRole, how='left', left_on=['EID', 'date'], right_on=["e_id", "Effective_Date"])

df4["Effective_Date"] = df4.groupby('EID')["Effective_Date"].fillna(method="ffill")
df4["New"] = df4.groupby('EID')["New"].fillna(method="ffill")
df4["Change_Type"] = df4.groupby('EID')["Change_Type"].fillna(method="ffill")

df4["role_mmt"] = np.where((df4["Change_Type"] == "Role") & (df4["date"] >= df4["Effective_Date"]), df4["New"], df4["role_mmt"])
df4 = df4.drop(columns=["Change_Type", "Effective_Date", "New", "e_id"])

# Change Log Market
chMarket = changelog.query('Change_Type == "Market"')
df4 = df4.merge(chMarket, how='left', left_on=['EID', 'date'], right_on=["e_id", "Effective_Date"])

df4["Effective_Date"] = df4.groupby('EID')["Effective_Date"].fillna(method="ffill")
df4["New"] = df4.groupby('EID')["New"].fillna(method="ffill")
df4["Change_Type"] = df4.groupby('EID')["Change_Type"].fillna(method="ffill")

df4["market_mmt(cluster)"] = np.where((df4["Change_Type"] == "Market") & (df4["date"] >= df4["Effective_Date"]), df4["New"], df4["market_mmt(cluster)"])
df4 = df4.drop(columns=["Change_Type", "Effective_Date", "New", "e_id"])

df4["LOB_mmt"] = np.where(df4["market_mmt(cluster)"] == "PDO_VACCINE_HESITANCY", "CO - Vaccine Hesitancy", df4["LOB_mmt"])
df4["LOB_mmt"] = np.where(df4["market_mmt(cluster)"] == "GTM_ITALIAN", "CO - GTM", df4["LOB_mmt"])
#df4["SLOB_mmt"] = np.where(df4["market_mmt(cluster)"] == "PDO_VACCINE_HESITANCY", 'Social Impact', df4["SLOB_mmt"])
df4["SLOB_mmt"] = np.where(df4["market_mmt(cluster)"] == "GTM_ITALIAN", 'GTM', df4["SLOB_mmt"])


# Role Change Column
dft = df4.query('role_mmt.notnull()')
dft["roleshift"] = dft.groupby(["EID"])["role_mmt"].transform('shift', -1).fillna(df4.role_mmt)
dft["role_change"] = np.where(
    dft["roleshift"] != dft["role_mmt"],
    dft["role_mmt"].map(str) + " " + "to" + " " + dft["roleshift"].map(str),
    np.nan
)
df4 = df4.merge(dft[["EID", "date", "role_change"]], on=["EID", "date"])

# PBU/SLOB Change Column
dfct = df4.query('PBU.notnull()')
dfct["pbu_shift"] = dfct.groupby(["EID"])["PBU"].transform('shift', -1).fillna(df4.PBU)

df4 = df4.merge(dfct[["EID", "date", "pbu_shift"]], on=["EID", "date"])

dfct = df4.query('SLOB_mmt.notnull()')
dfct["slob_shift"] = dfct.groupby(["EID"])["SLOB_mmt"].transform('shift', -1).fillna(df4.SLOB_mmt)

df4 = df4.merge(dfct[["EID", "date", "slob_shift"]], on=["EID", "date"])


dfct = df4[df4["market_mmt(cluster)"].notnull()]
dfct["market_shift"] = dfct.groupby(["EID"])["market_mmt(cluster)"].transform('shift', -1).fillna(df4["market_mmt(cluster)"])

df4 = df4.merge(dfct[["EID", "date", "market_shift"]], on=["EID", "date"])

df4["cross_change"] = np.where(
    (df4["PBU"] != df4["pbu_shift"]) | (df4["SLOB_mmt"] != df4["slob_shift"]) | (df4["market_mmt(cluster)"] != df4["market_shift"]) ,
    df4["PBU"] + "_" + df4["SLOB_mmt"] + "_" + df4["market_mmt(cluster)"] + " to " + df4["pbu_shift"] + "_" +  df4["slob_shift"] + "_" + df4["market_shift"], 
    np.nan
)

df4["cross_change"] = df4["cross_change"].str.replace("CUSTOMER_SUPPORT", "CS")
df4["cross_change"] = df4["cross_change"].str.replace("COMMUNITY_OPS", "CO")
df4["cross_change"] = df4["cross_change"].str.replace("MISREPRESENTATION", "MISREP")
df4["cross_change"] = df4["cross_change"].str.replace("Content Moderation", "CM")

df4["market_mmt(cluster)"] = df4["market_mmt(cluster)"].str.upper()

# WSC New
eidgroup = df4.groupby(['EID', 'week_ending'], as_index=False)[['Schedule', 'Leave']].sum()
eidgroup["schedule_aux"] = eidgroup["Schedule"] + eidgroup["Leave"]
eidgroup["scheduled_days"] = eidgroup["Schedule"]

eidWE = df4.groupby(['EID', 'week_ending', 'date'], as_index=False)[["Schedule", "Leave"]].sum()
eidWE = eidWE.merge(eidgroup[["EID", "week_ending", "schedule_aux", "scheduled_days"]], how='left', on=["EID", "week_ending"])

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

# Adhoc Change - CS -> CO from first November WE
df4["PBU"] = np.where((df4["date"] >= "2023-10-28") & 
                      (df4["PBU"] == "CUSTOMER_SUPPORT"),
                      "COMMUNITY_OPS", df4["PBU"])

df4["LOB_mmt"] = np.where((df4["date"] >= "2023-10-28") & 
                      (df4["LOB_mmt"] == "CO - Customer Support"),
                      "CO - Graph Integrity", df4["LOB_mmt"])

# -- Date (datetime64) to string for faster export times 'unplanned_abs', 'planned_loa', 'planned_pto', 'scheduled_rta'
df4["date"] = df4["date"].astype(str)
df4["week_ending"] = df4["week_ending"].astype(str)
df4["WAVE"] = df4["WAVE"].astype(str)
df4["SAP ID"] = df4["SAP ID"].astype(str)


df4 = df4[['EID', 'role_mmt', 'NAME', 'SRT ID', 'WAVE', 'employee_status_mmt', 'ROLL IN DATE', 'ROLL OFF DATE',
 'team_lead_mmt', 'ops_lead_mmt', 'market_mmt(cluster)', 'skills','PBU', 'SLOB_mmt', 'LOB_mmt', 'SupportRole',
 'SAP ID', 'SHIFT BLOCK', 'date', 'value', 'shift', 'week_ending', 'Schedule',
 'WD', 'LOA', 'PTO', 'ABS', 'Leave', 'iw_highlevel_status', 'iw_attendance_status','status_final_AL', 'rta_status',
 'rta_status_code', 'srtf_total_hrs', 'srtf_completed_time', 'iw_actual_time', 'WH', 'WSC_new', 'role_change', 'cross_change',
           'roll_off/reassignment', 'tenure', 'billable']]

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
PDOers = ["carlos.j.j.bolivar", "g.fialho.realista", "lucas.loureiro", "s.amaro.de.castro", "ines.b.raposo", "inma.g.gomez"]
exit_check = df4.query('ROLE == "Analyst" and value == "-" and rta_status == "Work" and EID not in @PDOers')
print("Exit Check:")
print(exit_check.EID.unique())

# -- Creating temporary folder in local drive to export files into and copying files to share drive
# solution employed because exporting directly to share drive takes considerably more time
# than exporting to local disk and transfering to share drive
with tempfile.TemporaryDirectory() as tempdir:
    ExportPath = tempdir + '\\' + OutputName + '.csv'
    df4.to_csv(ExportPath, index=False)
    mainsrc = ExportPath
    maindst = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\07. Daily Unpivot"
    shutil.copy(src=mainsrc, dst=maindst)
    BackupExportPath = tempdir + '\\' + BackupName + '.csv'
    #df4.to_csv(BackupExportPath, index=False)
    backupsrc = BackupExportPath
    backupdst = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/13. Unpivot_MF Main/"
    #shutil.copy(src=backupsrc, dst=backupdst)

dfp = df4
dfp["SRT ID"] = dfp["SRT ID"].astype(str)
dfp.columns = dfp.columns.str.replace(" ", "_")
dfp.columns = dfp.columns.str.replace("/", "_")
dfp["SRT_ID"] = dfp["SRT_ID"].str.rstrip('.0')
dfp = dfp.rename(columns={"date":"date_str"})
dfp["date_aux"] = pd.to_datetime(dfp["date_str"])
dfp["date"] = pd.to_datetime(dfp["date_str"], format=("%Y-%m-%d")).dt.strftime('%m-%d-%Y')

mfdate = File[-6:]
dfp["mfdate"] = File[-6:]
dfp["mfdate"] = pd.to_datetime(dfp["mfdate"], format='%m%d%y')

pdate = datetime.today().date()
dfp["creation_date"] = pdate
dfp["creation_date"] = pd.to_datetime(dfp["creation_date"])
dfp["ABS_aux"] = np.where((dfp["ABS"] == 0) & (dfp["iw_highlevel_status"].notnull()), 1, 0)

firstbill = dfp[["EID", "date_aux", "billable"]]
firstbill = firstbill.query('billable == "billable"')
firstbill = firstbill.groupby("EID", as_index=False)["date_aux"].min()
firstbill = firstbill.rename(columns={"date_aux":"first_bill_day", "EID":"EID_firstbill"})

dfp = dfp.merge(firstbill, how='left', left_on=["EID", "date_aux"], right_on=["EID_firstbill", "first_bill_day"])

ParquetPath = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\07. Daily Unpivot\Parquet\Unpivot.parquet"
dfp.to_parquet(ParquetPath)

DailyParquet = str('Unpivot MF ' + File[-6:] + ' ' + str(pdate))
DailyParquetPath = "c:/Users/mario.canudo/Desktop/Unpivot WFH/07. Daily Unpivot/Parquet/Daily Parquet/%s.parquet"%DailyParquet
#DailyParquetPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/13. Unpivot_MF Main/02. Parquet/Daily Parquet/%s.parquet'%DailyParquet
dfp.to_parquet(DailyParquetPath)

#Path = os.path.realpath(Path)
#os.startfile(Path)
print(datetime.now())

#df4.to_csv("//projectportugaltest.file.core.windows.net/mstestfolder/Test CC/testing.csv")
#df4.to_csv('//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/02. IW Daily/IW Unp_test.csv')
