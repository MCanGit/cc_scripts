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
StartingDate = "12/30/2023"
EndingDate = "1/10/2025"

File = "122623_CO - Master_Schedule - 2024"

MFCodes = "CODES MF.WH"
CrossTraining = "Heatmap_Cross&Trainee (2024)"
ExitsList = "CO IW Absenteeism v.12.5 ICT"
MMT_Daily = 'Latest_MMT'
AdjustmentList = 'IW_AttendanceCompilation_2023'
# --

ImportPath = "//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/08. Daily Master File/2024/%s.xlsb"%File
#ImportPath = "C:/Users/mario.canudo/Desktop/Unpivot 2024/%s.xlsb"%File

#MFCodesPath = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\03. Codes\CODES MF.WH.xlsx"
MFCodesPath = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/000. Database/14. Codes MF & WH/%s.xlsx"%MFCodes

CrossTrainingPath = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/11. Extras/11. Mário/.01/18. Unpivot 2024/%s.xlsx"%CrossTraining
#CrossTrainingPath = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\01. Magic List\Heatmap_Cross&Trainee.xlsx"

ExitsListPath = "//lisfs1003/honey_badger$/Operations - Management/WFM/01. IW Report/CO/%s.xlsb"%ExitsList
#xitsListPath = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\04. RTA\CO IW Absenteeism v.12.5 ICT.xlsb"

MMT_DailyPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/01. Parquet/%s.parquet'%MMT_Daily
AdjustmentListPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/04. Adjustment List/%s.xlsx'%AdjustmentList
#Path = "//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/09. Daily Unpivot/"

#CF_FCST_Path = r"C:\Users\mario.canudo\Desktop\Unpivot WFH\11. CF FCST\CF_FCST.xlsx"
CF_FCST_Path = r"Z:\Operations - Management\Lisbon Reporting\26. WFM (Reporting)\16. CF FCST\CF_FCST.xlsx"

pdate = datetime.today().date()

OutputName = str('Unpivot ' + File[-6:] + ' ' + str(pdate) + ' v2')
#ExportPath = '//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/09. Daily Unpivot/%s.csv'%OutputName

BackupName = str('Backup Unpivot ' + ' ' + File[-6:])
#BackupExportPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/13. Unpivot_MF Main/01. Backup/%s.csv'%BackupName

df = pd.read_excel(ImportPath, sheet_name= "Master_Schedule",  header= 3)

# Remove #N/A (#N/A happen when someone is added to MF before being added to Roster)
df = df.query('NAME != "0x2a"')



#Unpivot

# delete WW columns
df.drop(list(df.filter(regex='WW')), axis=1, inplace=True)
df.drop(["WD", "SL"], axis=1, inplace=True)
df.drop(df.columns[395  :], axis=1, inplace=True)

# Unpivot Date
df4 = df.melt(id_vars=["EID",
                      "ROLE",
                      "NAME",
                      "SRT ID",
                      "WAVE",
                      "STATUS",
                      "ROLL IN DATE",
                      "ROLL OFF DATE",
                      "SUPERVISOR CURRENT",
                      "OPS",
                      "MARKET",
                      "Flow Skills",
                      "SLOB",
                      "LOB",
                      "SupportRole",
                      "SAP ID",
                      "SHIFT BLOCK"
                      ],
             var_name="date"
             )
df4["date"] = df4["date"].astype(int)
df4["date"] = pd.to_datetime(df4["date"], unit='d', origin='12-30-1899').astype('string')

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
    mf_codes.code.str.endswith('ml'),
    mf_codes.code.str.endswith('3'),
    mf_codes.code.str.endswith('4'),
    mf_codes.code.str.endswith('5'),
    mf_codes.code.str.endswith('6'),
    mf_codes.code.str.endswith('9')
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

# -- Check against MMT Daily Roster


# Fill NULLs from MMT Roster


# Exit List - all codes are substituted for '-' after the employment end date
#exitlist = pd.read_excel(ExitsListPath,
#                         sheet_name="Unpivot_Exit List",
#                         usecols=["Enterprise ID", "Employment End Date"]
#                         )

#exitlist = exitlist.drop_duplicates(subset='Enterprise ID', keep='first')
##exitlist.columns = exitlist.columns.str.lower()
#exitlist["employment end date"] = pd.to_datetime(exitlist["employment end date"], unit='d', origin='12-30-1899')

#df4 = df4.merge(exitlist, how='left', left_on=['EID', 'date'], right_on=['enterprise id', 'employment end date'])
#df4["employment end date"] = df4.groupby('EID')["employment end date"].fillna(method='ffill')

#df4["value"] = np.where((df4["employment end date"].notnull()) &
##                        (df4["date"] > df4["employment end date"]) &
 #                      ((df4["ROLL OFF DATE"] == 0) | (df4["ROLL OFF DATE"].isnull())),
  #                      '-', df4["value"])

# Change V0 to "-"
df4["value"] = df4["value"].str.replace("V0", "-")


# secret exit list
secretExitList = pd.read_excel(CrossTrainingPath, sheet_name='Secret Exit List')
secretExitList = secretExitList.query('exit_date > @today')

df4 = df4.merge(secretExitList[["e_id", "exit_date"]], how='left', left_on=['EID', 'date'],
                right_on=['e_id', 'exit_date'])
df4['exit_date'] = df4.groupby('EID')['exit_date'].fillna(method='ffill')
s_exit = df4.query('exit_date.notnull()')

def secret_exit_list():
    df4["value"] = np.where((df4["date"] > df4["exit_date"]) &
                            (df4["EID"].isin(secretExitList["e_id"])),
                            "-", df4["value"])
    df4["roll_off/reassignment"] = np.where(df4["date"] == df4["exit_date"], 'roll_off', df4["roll_off/reassignment"])

if not s_exit.exit_date.index.empty: secret_exit_list()

df4 = df4.drop(columns=['e_id', 'exit_date'])



df4["value"] = df4["value"].str.replace('VOM2', 'OM2')


# -- CF FCST Absenteeism
current_day = datetime.today().date()
cf_backup = 'Backup ' + str(current_day) + ' MF ' + File[-6:]
cf_backup_output = "Z:/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/16. CF FCST/01. Backup/%s.xlsx"%cf_backup
cf_fcst = pd.read_excel(CF_FCST_Path)
cf_fcst.to_excel(cf_backup_output, index=False)
cf_fcst.columns = cf_fcst.columns.str.replace(" - ", "_")
cf_fcst.columns = cf_fcst.columns.str.replace(" ", "_")
cf_fcst.columns = cf_fcst.columns + "_cf"
cf_fcst = cf_fcst.query('Ticket_decision_cf == "Consider in Forecast"')
cf_split = cf_fcst["EID_cf"].str.split('@', expand=True)
cf_lenght = len(cf_split.columns)

def single_EID(cf_fcst):
    cf_fcst[["EID_cf", "EID_cf2"]] = cf_fcst["EID_cf"].str.split("@", expand=True)
    return cf_fcst

#cf_fcst[["EID_cf", "EID_cf2"]] = cf_fcst["EID_cf"].str.split("@", expand=True)

def multiple_EID(cf_fcst, cf_split):
    df_ID = cf_fcst["ID_cf"].to_frame()
    cf_split = df_ID.join(cf_split)
    cf_split = cf_split.rename(columns={0:"EID"})
    df_splitmelt = pd.melt(cf_split, id_vars=["ID_cf", "EID"])
    df_splitmelt = df_splitmelt.query('value.notnull()')
    df_splitmelt["aux1"] = df_splitmelt["value"].str[16:]
    df_splitmelt[["EID_1", "EID_2"]] = df_splitmelt.aux1.str.split('#', expand=True)
    df2 = df_splitmelt[["ID_cf", "EID_2"]]
    df2 = df2.rename(columns={"EID_2":"EID"})
    df3 = pd.concat([cf_split, df2], ignore_index=True)
    df3 = df3[["ID_cf", "EID"]]
    df3 = df3.drop_duplicates(["ID_cf", "EID"])
    df3 = df3.dropna()
    cf_fcst = cf_fcst.merge(df3, how='left', on='ID_cf')
    cf_fcst["EID_cf"] = cf_fcst["EID"]
    return cf_fcst

if cf_lenght > 3:
    cf_fcst = multiple_EID(cf_fcst, cf_split)
else:
    cf_fcst = single_EID(cf_fcst)


df4["ABS_FCST"] = np.nan

cf_abs = cf_fcst.query('Forecasted_Change_cf == "Absenteeism"')
df4 = df4.merge(cf_abs[["EID_cf", "Start_Date_cf", "End_Date_cf"]], how='left', left_on=['EID', 'date'], right_on=['EID_cf', 'Start_Date_cf'])

def fcst_absent():
    df4['Start_Date_cf'] = df4.groupby('EID')['Start_Date_cf'].fillna(method='ffill')
    df4['End_Date_cf'] = df4.groupby('EID')['End_Date_cf'].fillna(method='ffill')
    df4["ABS_FCST"] = np.where((df4["date"] >= df4["Start_Date_cf"]) & (df4["date"] <= df4["End_Date_cf"]) &
                            (df4["value"] != "L1") & (df4["value"] != "L2"), 1, df4["ABS_FCST"])

if not cf_abs.Start_Date_cf.index.empty: fcst_absent()

df4 = df4.drop(columns=["EID_cf", "Start_Date_cf", "End_Date_cf"])

# -- CF FCST LOA
cf_loa = cf_fcst.query('Forecasted_Change_cf == "LOA"')
df4 = df4.merge(cf_loa[["EID_cf", "Start_Date_cf", "End_Date_cf"]], how='left',  left_on=['EID', 'date'], right_on=['EID_cf', 'Start_Date_cf'])

def fcst_loa():
    df4['Start_Date_cf'] = df4.groupby('EID')['Start_Date_cf'].fillna(method='ffill')
    df4['End_Date_cf'] = df4.groupby('EID')['End_Date_cf'].fillna(method='ffill')
    df4["ABS_FCST"] = np.where((df4["date"] >= df4["Start_Date_cf"]) & (df4["date"] <= df4["End_Date_cf"]) &
                            (df4["value"] != "L1") & (df4["value"] != "L2"), 1, df4["ABS_FCST"])

if not cf_loa.Start_Date_cf.index.empty: fcst_loa()

df4 = df4.drop(columns=["EID_cf", "Start_Date_cf", "End_Date_cf"])

# -- CF FCST ROLL OFF
cf_rolloff = cf_fcst.query('Forecasted_Change_cf == "Managed Attrition"')
cf_rolloff = cf_rolloff.rename(columns={"Managed_attrition_DATE_FORECASTED_cf":"roll_off_cf"})
df4 = df4.merge(cf_rolloff[["EID_cf", "roll_off_cf"]], how='left', left_on=['EID', 'date'], right_on=['EID_cf', 'roll_off_cf'])

def fcst_rolloff():
    df4['roll_off_cf'] = df4.groupby('EID')['roll_off_cf'].fillna(method='ffill')
    df4["ABS_FCST"] = np.where((df4["date"] > df4["roll_off_cf"]) &
                               (df4["value"] != '-'), 1, df4["ABS_FCST"])

if not cf_rolloff.roll_off_cf.index.empty: fcst_rolloff()
df4 = df4.drop(columns=["EID_cf", "roll_off_cf"])


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
df4["ABS"] = np.where((df4["WD"] == 1) & (df4["Schedule"] < 1), 1, 0)
#df4["ABS"] = np.where((df4["WD"] == 1) & (df4["rta_status"] == "Abs"), 1, 0)
df4["ABS"] = np.where((df4["ABS_FCST"] == 1), 1, df4["ABS"])

# Leave
df4["Leave"] = np.where(df4["value"].isin(["L1", "L2"]), 1, 0)

# Roll in and off Dates as Date
df4["ROLL OFF DATE"] = df4["ROLL OFF DATE"].replace(0, 2)
df4["ROLL IN DATE"] = pd.to_datetime(df4["ROLL IN DATE"].astype(int),  unit='d', origin='12/30/1899').dt.strftime('%m/%d/%Y')
df4["ROLL OFF DATE"] = pd.to_datetime(df4["ROLL OFF DATE"].astype(int), unit='d', origin='12/30/1899').dt.strftime('%m/%d/%Y')
df4["value"] = df4["value"].fillna('-')

# -- Change Trainee Role if it has WD Codes
trainees = df4.query('value in @mf_codes.Work and ROLE == "Trainee"')
trainees = trainees.groupby(['EID'])['date'].min().to_frame('training_enddate').reset_index()
trainees = trainees.rename(columns={"EID":"EID_trainees"})

df4 = df4.merge(trainees, how='left', left_on=['EID', 'date'], right_on=['EID_trainees', 'training_enddate'])
df4["training_enddate"] = df4.groupby('EID')["training_enddate"].fillna(method="ffill")
df4["ROLE"] = np.where((df4["ROLE"] == "Trainee") &
                           (df4["date"] >= df4["training_enddate"]),
                             'Analyst', df4["ROLE"])

# Provisional 2024
df4["ROLE"] = np.where(df4["ROLE"] == 'Trainee', 'Analyst', df4["ROLE"])

# -- Create Business Unit column based on LOB
pbu_cond = [
    df4["LOB"] == 'CO - Customer Support',
    df4["LOB"].str.startswith('CO -', na=False),
    df4["LOB"] == 'Non-Rec',
    df4["LOB"] == 'CO - Non-Rec'
]

pbu_results = [
    'CUSTOMER_SUPPORT',
    'COMMUNITY_OPS',
    'COMMUNITY_OPS',
    'COMMUNITY_OPS'
]

df4['PBU'] = np.select(pbu_cond, pbu_results, df4["LOB"])

# Cross Skilling - SLOB is substituted with Cross Skilling Training Flow after the training end date
df4['PBU'] = np.where((df4["date"] >= df4["End date_aux"]) & (df4["End date_aux"].astype(str) > today), df4["Business Unit"], df4["PBU"])
df4["SLOB"] = np.where((df4["date"] >= df4["End date_aux"]) & (df4["End date_aux"] > today), df4["Training FLOW"], df4["SLOB"])
df4["SLOB"] = df4["SLOB"].str.replace('OBJECTS/IGPR', 'Objects/IGPR')
df4["SLOB"] = df4["SLOB"].str.replace('MESSENGER', 'Messenger')

df4["LOB"] = np.where((df4["PBU"] == "LERT") &
                          (df4["SLOB"] == "LERT"),
                          "LERT", df4["LOB"])
df4["LOB"] = np.where((df4["SLOB"] == 'Messenger') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Content Moderation', df4["LOB"])
df4["LOB"] = np.where((df4["SLOB"] == 'Content Moderation') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Content Moderation', df4["LOB"])
df4["LOB"] = np.where((df4["SLOB"] == 'Objects/IGPR') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Content Moderation', df4["LOB"])
df4["LOB"] = np.where((df4["SLOB"] == 'MISREPRESENTATION') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Graph Integrity', df4["LOB"])
df4["LOB"] = np.where((df4["SLOB"] == 'Non-Rec') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Non-Rec', df4["LOB"])
df4["LOB"] = np.where((df4["SLOB"] == 'GTM') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - GTM', df4["LOB"])
df4["LOB"] = np.where((df4["SLOB"] == 'IDVAAS') & (df4['PBU'] == 'COMMUNITY_OPS'), 'CO - Graph Integrity', df4["LOB"])
df4["LOB"] = np.where((df4["SLOB"] == 'IDVAAS') & (df4['PBU'] == 'CUSTOMER_SUPPORT'), 'CO - Customer Support', df4["LOB"])


# Change Log SLOB
changelog = pd.read_excel(CrossTrainingPath, sheet_name='Change Log')


chSLOB = changelog.query('Change_Type == "SLOB"')
df4 = df4.merge(chSLOB, how='left', left_on=['EID', 'date'], right_on=["e_id", "Effective_Date"])

df4["Effective_Date"] = df4.groupby('EID')["Effective_Date"].fillna(method="ffill")
df4["New"] = df4.groupby('EID')["New"].fillna(method="ffill")
df4["Change_Type"] = df4.groupby('EID')["Change_Type"].fillna(method="ffill")

df4["SLOB"] = np.where((df4["Change_Type"] == "SLOB") & (df4["date"] >= df4["Effective_Date"]), df4["New"], df4["SLOB"])

df4["LOB"] = np.where((df4["PBU"] == "LERT") &
                          (df4["SLOB"] == "LERT"),
                          "LERT", df4["LOB"])
df4["LOB"] = np.where(df4["PBU"] == "CUSTOMER_SUPPORT", "CO - Customer Support", df4["LOB"])
df4["LOB"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB"] == 'MISREPRESENTATION'),
                          "CO - Graph Integrity", df4["LOB"])
df4["LOB"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB"] == 'Messenger'),
                          "CO - Content Moderation", df4["LOB"])
df4["LOB"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB"] == 'Objects/IGPR'),
                          "CO - Content Moderation", df4["LOB"])
df4["LOB"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB"] == 'Content Moderation'),
                          "CO - Content Moderation", df4["LOB"])
df4["LOB"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB"] == 'GTM'),
                          "CO - GTM", df4["LOB"])
df4["LOB"] = np.where((df4["PBU"] == "COMMUNITY_OPS") &
                          (df4["SLOB"] == 'Non-Rec'),
                          "CO - Non-Rec", df4["LOB"])
df4["LOB"] = np.where((df4["PBU"] == "COMMUNITY_OPS") & 
                          (df4["SLOB"] == "IDVAAS"),
                          "CO - Graph Integrity", df4["LOB"])


df4["LOB"] = np.where(df4["LOB"] == "CO - Customer Support", "IDVAAS", df4["LOB"])

#df4["SLOB_mmt"] = np.where(df4["LOB_mmt"] == "CO - Non-Rec", "Non-Rec", df4["SLOB_mmt"])
#df4["SLOB_mmt"] = np.where(df4["LOB_mmt"] == "Non-Rec", "Non-Rec", df4["SLOB_mmt"])
df4["LOB"] = np.where((df4["SLOB"] == 'Policy') & (df4["EID"].isin(df4["e_id"])),
                          "CO - PTQ", df4["LOB"])
df4["LOB"] = np.where((df4["SLOB"] == 'Training') & (df4["EID"].isin(df4["e_id"])),
                          "CO - PTQ", df4["LOB"])


df4 = df4.drop(columns=["Change_Type", "Effective_Date", "New", "e_id"])





# Change Log SLOB


# Change Log Role


# Change Log Market

# Role Change Column

# LOB/SLOB Change Column


#df4["market_mmt(cluster)"] = df4["market_mmt(cluster)"].str.upper()

# WSC
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
df4 = df4.drop(columns=[
                        "Training FLOW",
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
                        "training_enddate",
                        "EID_C",
                        "End date_aux"
                        ], errors='ignore')


# -- Shift Column
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
df4["PBU"] = np.where((df4["PBU"] == "CUSTOMER_SUPPORT"),
                      "COMMUNITY_OPS", df4["PBU"])

df4["LOB"] = np.where((df4["LOB"] == "CO - Customer Support"),
                      "CO - Graph Integrity", df4["LOB"])

df4["LOB"] = np.where((df4["LOB"] == 'IDVAAS') &
                      (df4["SLOB"] == 'IDVAAS'),
                      'CO - Graph Integrity', df4["LOB"])

# -- Date (datetime64) to string for faster export times , 'roll_off/reassignment'
df4["date"] = df4["date"].astype(str)
df4["week_ending"] = df4["week_ending"].astype(str)
df4["WAVE"] = df4["WAVE"].astype(str)
df4["SAP ID"] = df4["SAP ID"].astype(str)


duplicate_check = df4.groupby(["EID", "date"], as_index=False).size()
duplicate_check = duplicate_check.query('size != 1')
print("Duplicates:")
print(duplicate_check.EID.unique())
print(df4["value"].unique())

# -- Creating temporary folder in local drive to export files into and copying files to share drive
# solution employed because exporting directly to share drive takes considerably more time
# than exporting to local disk and transfering to share drive


df4["iw_highlevel_status"] = np.nan
df4["iw_attendance_status"] = np.nan
df4["status_final_AL"] = np.nan
df4["rta_status"] = np.nan
df4["rta_status_code"] = np.nan
df4["unplanned_abs"] = np.nan
df4["planned_loa"] = np.nan
df4["planned_pto"] = np.nan
df4["scheduled_rta"] = np.nan
df4["srtf_total_hrs"] = np.nan
df4["role_change"] = np.nan
df4["cross_change"] = np.nan
df4["roll_off/reassignment"] = np.nan
df4["tenure"] = np.nan
df4["billable"] = np.nan

df4 = df4[['EID', 'ROLE', 'NAME', 'SRT ID', 'WAVE', 'STATUS', 'ROLL IN DATE', 'ROLL OFF DATE',
 'SUPERVISOR CURRENT', 'OPS', 'MARKET', 'Flow Skills','PBU', 'SLOB', 'LOB', 'SupportRole',
 'SAP ID', 'SHIFT BLOCK', 'date', 'value', 'shift', 'week_ending', 'Schedule',
 'WD', 'LOA', 'PTO', 'ABS', 'Leave', 'iw_highlevel_status', 'iw_attendance_status','status_final_AL', 'rta_status',
 'rta_status_code', 'unplanned_abs', 'planned_loa', 'planned_pto', 'scheduled_rta', 'srtf_total_hrs', 'WSC_new',
            'role_change', 'cross_change', 'roll_off/reassignment']]

rename_cols = {
    "WSC_new":"WSC"
    }

df4 = df4.rename(columns=rename_cols)


with tempfile.TemporaryDirectory() as tempdir:
    ExportPath = tempdir + '\\' + OutputName + '.csv'
    df4.to_csv(ExportPath, index=False)
    mainsrc = ExportPath
    maindst = "//lisfs1003/honey_badger$/Operations - Management/WFM/02. Database/08. Daily Master File/2024/"
    shutil.copy(src=mainsrc, dst=maindst)
    BackupExportPath = tempdir + '\\' + BackupName + '.csv'
    df4.to_csv(BackupExportPath, index=False)
    backupsrc = BackupExportPath
    backupdst = "//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/13. Unpivot_MF Main/"
    shutil.copy(src=backupsrc, dst=backupdst)

#Path = os.path.realpath(Path)
#os.startfile(Path)
print(datetime.now())

#dfp = df4
#dfp["SRT ID"] = dfp["SRT ID"].astype(str)
#dfp.columns = dfp.columns.str.replace(" ", "_")
#dfp.columns = dfp.columns.str.replace("/", "_")
#dfp["SRT_ID"] = dfp["SRT_ID"].str.rstrip('.0')
#dfp = dfp.rename(columns={"date":"date_str"})
#dfp["date_aux"] = pd.to_datetime(dfp["date_str"])
#dfp["date"] = pd.to_datetime(dfp["date_str"], format=("%Y-%m-%d")).dt.strftime('%m-%d-%Y')

#pdate = '2024-01-01'
#pdate = datetime.today().date()
#dfp["creation_date"] = pdate
#dfp["creation_date"] = pd.to_datetime(dfp["creation_date"], format=("%Y-%m-%d"))
#dfp["ABS_aux"] = np.where((dfp["ABS"] == 0) & (dfp["iw_highlevel_status"].notnull()), 1, 0)

#dfp.to_csv('//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/bill_test.csv')

#ParquetPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/13. Unpivot_MF Main/02. Parquet/Unpivot 2024.parquet'
#dfp.to_parquet(ParquetPath)

#DailyParquet = str('Unpivot MF ' + File[-6:] + ' ' + str(pdate))
#DailyParquetPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/13. Unpivot_MF Main/02. Parquet/Daily Parquet/%s.parquet'%DailyParquet
#dfp.to_parquet(DailyParquetPath)



#df4.to_csv("//projectportugaltest.file.core.windows.net/mstestfolder/Test CC/testing.csv")
#df4.to_csv(r"C:\Users\mario.canudo\Desktop\Unpivot 2024\Unpivot 2024.csv", index=False)
