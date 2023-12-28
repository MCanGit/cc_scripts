import pandas as pd
import numpy as np

# Manual Change required
QSA_A1 = 'IW Update Status - 1215 QSA & Analysts'
QSA_A2 = 'IW Update Status - 1222 QSA & Analysts2'
TL1 = 'IW Update Status - 1215 TL & Other Roles'
TL2 = 'IW Update Status - 1222 TLs & Other Roles2'
# --

MMT_Daily = 'MMT_IW 2023 Jan_Jun'
MMT_DailyOngoing = 'MMT_IW 2023 July-'
AdjustmentList = 'IW_AttendanceCompilation_2023'
ShrinkageReport = 'Shrinkage_Report v.14 ICT'

df_qsa_anal1 = "//lisfs1003/honey_badger$/Operations - Management/WFM/01. IW Report/01. Database IW/01. Attendance/CO/Closed Data/clean files/%s.xlsx"%QSA_A1
df_qsa_anal2 = "//lisfs1003/honey_badger$/Operations - Management/WFM/01. IW Report/01. Database IW/01. Attendance/CO/Closed Data/clean files/%s.xlsx"%QSA_A2
df_tl1 = "//lisfs1003/honey_badger$/Operations - Management/WFM/01. IW Report/01. Database IW/01. Attendance/CO/Closed Data/clean files/%s.xlsx"%TL1
df_tl2 = "//lisfs1003/honey_badger$/Operations - Management/WFM/01. IW Report/01. Database IW/01. Attendance/CO/Closed Data/clean files/%s.xlsx"%TL2

MMT_DailyPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/%s.csv'%MMT_Daily
MMT_DailyOngoingPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/%s.csv'%MMT_DailyOngoing
AdjustmentListPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/04. Adjustment List/%s.xlsx'%AdjustmentList
ShrinkageReportPath = "//lisfs1003/honey_badger$/Operations - Management/WFM/01. IW Report/CO/%s.xlsb"%ShrinkageReport

# -- Check against MMT Daily Roster
mmt_daily = pd.read_csv(MMT_DailyPath, usecols=['enterprise_id',
                                                'role',
                                                'employee_status',
                                                'skills',
                                                'cluster',
                                                'current_workflow',
                                                'workflow',
                                                'roster_date',
                                                'billable',
                                                'team_lead',
                                                'ops_lead_eid',
                                                'iw_highlevel_status',
                                                'iw_attendance_status',
                                                'iw_actual_time',
                                                'srtf_total_hrs',
                                                'srtf_completed_time',
                                                'hc',
                                                'srtf_adjustment_type',
                                                'date',
                                                'ts_total_hours',
                                                'ts_completed_time'
                                                ], encoding="utf-8-sig")

mmt_ongoing = pd.read_csv(MMT_DailyOngoingPath, usecols=['enterprise_id',
                                                'role',
                                                'employee_status',
                                                'skills',
                                                'cluster',
                                                'current_workflow',
                                                'workflow',
                                                'roster_date',
                                                'billable',
                                                'team_lead',
                                                'ops_lead_eid',
                                                'iw_highlevel_status',
                                                'iw_attendance_status',
                                                'iw_actual_time',
                                                'srtf_total_hrs',
                                                'srtf_completed_time',
                                                'hc',
                                                'srtf_adjustment_type',
                                                'date',
                                                'ts_total_hours',
                                                'ts_completed_time'
                                                ], encoding="utf-8-sig")

mmt = pd.concat([mmt_daily, mmt_ongoing])
mmt = mmt.rename(columns={'date':'srtf_date'})

slob_cond = [
    mmt["current_workflow"] == 'IGPR/Objects',
    mmt["current_workflow"] == 'Misrepresentation',
    mmt["current_workflow"] == 'IDRA',
    mmt["current_workflow"] == 'INA',
    mmt["current_workflow"] == 'IG Access',
    mmt["current_workflow"] == 'FNRP',
    mmt["current_workflow"] == 'IDReview',
    mmt["current_workflow"] == 'InstagramProfile',
    mmt["current_workflow"] == 'NamesReview',
    mmt["current_workflow"] == 'Object',
    mmt["current_workflow"] == 'PrivateImpersonation'
]

slob_results = [
    'Objects/IGPR',
    'MISREPRESENTATION',
    'IDVAAS',
    'IDVAAS',
    'IDVAAS',
    'IDVAAS',
    'IDVAAS',
    'MISREPRESENTATION',
    'IDVAAS',
    'Objects/IGPR',
    'MISREPRESENTATION'
]

mmt["roster_date"] = pd.to_datetime(mmt["roster_date"], format='%Y/%m/%d').dt.strftime('%m/%d/%Y')
mmt["current_workflow"] = np.select(slob_cond, slob_results, mmt["current_workflow"])

# Merge with IW Status Mapping for the RTA Status
#mmt["iw_attendance_status"] = mmt["iw_attendance_status"].str.replace(',', ' -')

#statusmap = pd.read_excel('//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/Final Status Mapping.xlsx')

#mmt = mmt.merge(statusmap, how='left', left_on='iw_attendance_status', right_on='Status final')

#rename_dict = {'Status':'rta_status',
#               'Status_code':'rta_status_code'}

#mmt = mmt.rename(columns=rename_dict)

# Adjust the SRTF Total Hrs to take adjustments into account and eliminate duplicates
adjustment_list = mmt.query('hc != 1').groupby(["enterprise_id", "roster_date"])[["srtf_total_hrs", "srtf_completed_time"]].sum().reset_index()
adjustment_list = adjustment_list.rename(columns={"srtf_total_hrs":"adjusted_total_hrs",
                                                "srtf_completed_time":"adjusted_completed_time"})
mmt = mmt.merge(adjustment_list, how='left', on=["enterprise_id", "roster_date"])
mmt["srtf_total_hrs"] = np.where(mmt["adjusted_total_hrs"].notnull(), mmt["adjusted_total_hrs"], mmt["srtf_total_hrs"])
mmt["srtf_completed_time"] = np.where(mmt["adjusted_completed_time"].notnull(), mmt["adjusted_completed_time"], mmt["srtf_completed_time"])
mmt = mmt.drop_duplicates(["enterprise_id", "roster_date", "srtf_date"])

# Check for Timestamp data if no SRT Finals data available
mmt["srtf_total_hrs"] = np.where(mmt["srtf_total_hrs"] == 0, np.nan, mmt["srtf_total_hrs"])
mmt["srtf_completed_time"] = np.where(mmt["srtf_completed_time"] == 0, np.nan, mmt["srtf_completed_time"])

mmt["srtf_total_hrs"] = np.where(mmt["srtf_total_hrs"].isnull(), mmt["ts_total_hours"], mmt["srtf_total_hrs"])
mmt["srtf_completed_time"] = np.where(mmt["srtf_completed_time"].isnull(), mmt["ts_completed_time"], mmt["srtf_completed_time"])

# Adjustment List
AL = pd.read_excel(AdjustmentListPath,
                     usecols=["Username", "Date", "Status Final"])

AL = AL.rename(columns={"Username":"enterprise_id", "Status Final":"status_final_AL", "Date":"roster_date"})
#AL["roster_date"] = AL["roster_date"].dt.strftime('%Y-%m-%d')
AL = AL.drop_duplicates(["enterprise_id", "roster_date"], keep='last')

AL["roster_date"] = pd.to_datetime(AL["roster_date"], format='%Y/%m/%d').dt.strftime('%m/%d/%Y')

AL["status_final_AL"] = AL["status_final_AL"].str.replace(",", " -")
AL["status_final_AL"] = np.where(AL["status_final_AL"].str.contains(r"1B - SICK LEAVE\n"), "1B - SICK LEAVE", AL["status_final_AL"])
AL["status_final_AL"] = AL["status_final_AL"].str.replace("12-", "12 -")

mmt = mmt.merge(AL, how='left', on=['enterprise_id', 'roster_date'])

# Adjustment List
df_qsa1 = pd.read_excel(df_qsa_anal1, usecols={'Username', 'Date', 'Status Final'}, sheet_name='List')
df_qsa2 = pd.read_excel(df_qsa_anal2, usecols={'Username', 'Date', 'Status Final'}, sheet_name='List')
df_tl1 = pd.read_excel(df_tl1, usecols={'Username', 'Date', 'Status Final'}, sheet_name='List')
df_tl2 = pd.read_excel(df_tl2, usecols={'Username', 'Date', 'Status Final'}, sheet_name='List')

df_qsa_all = df_qsa1.append([df_qsa2, df_tl1, df_tl2])

col_rename = {'Username':'enterprise_id',
              'Date':'roster_date',
              'Status Final':'Status_Final_adjust'}

df_qsa_all = df_qsa_all.rename(columns=col_rename)

df_qsa_all["roster_date"] = df_qsa_all["roster_date"].dt.strftime('%m/%d/%Y')

print(mmt.roster_date)
print(df_qsa_all.roster_date)
mmt = mmt.merge(df_qsa_all, how='left', on=['enterprise_id', 'roster_date'])

# Get last weeks of Status Final directly from Shrinkage file
#shrink = pd.read_excel(ShrinkageReportPath,
#                   sheet_name='Attendance',
#                   usecols=["Username", "Day", "Status final"])

#shrink = shrink.rename(columns={"Username":"enterprise_id", "Status final":"status_final_sh", "Day":"roster_date"})
#shrink["roster_date"] = pd.to_datetime(shrink["roster_date"], unit='d', origin='12-30-1899').dt.strftime('%m/%d/%Y')

#shrink["status_final_sh"] = shrink["status_final_sh"].astype('string')
#mmt = mmt.merge(shrink, how='left', on=['enterprise_id', 'roster_date'])

# Fill NULLs from Compiled
mmt["status_final_AL"] = np.where(mmt["status_final_AL"].isnull(), mmt["Status_Final_adjust"], mmt["status_final_AL"])
mmt["status_final_AL"] = np.where(mmt["status_final_AL"].isnull(), mmt["iw_attendance_status"], mmt["status_final_AL"])

# Merge with IW Status Mapping for the RTA Status
mmt["iw_attendance_status"] = mmt["iw_attendance_status"].str.replace(',', ' -')
mmt["status_final_AL"] = mmt["status_final_AL"].str.replace(",", " -")

statusmap = pd.read_excel('//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/Final Status Mapping.xlsx')

mmt = mmt.merge(statusmap, how='left', left_on='status_final_AL', right_on='Status final')

rename_dict = {'Status':'rta_status',
               'Status_code':'rta_status_code'}

mmt = mmt.rename(columns=rename_dict)

# WH Column
mmt["WH"] = np.where(mmt["iw_actual_time"] > 0.33, mmt["iw_actual_time"], mmt["WH Code"])
mmt["WH"] = mmt["WH"].astype('string')

# Merge with Roster_Lisbon for DMR Graduation Date
roster = pd.read_excel("//lisfs1003/honey_badger$/Operations - Management/WFM/Weekly Roster HC Reports/ROSTER_Lisbon.xlsm",
                   sheet_name="DMR",
                   usecols=["EID", "OpsDeploymentDate", "Graduation Date", "DMR Status"])

roster = roster.rename(columns={"Graduation Date":"Graduation_Date", "EID":"enterprise_id"})

roster["OpsDeploymentDate"] = roster["OpsDeploymentDate"].astype("string")
roster["OpsDeploymentDate"] = roster["OpsDeploymentDate"].replace("00:00:00", np.nan)
roster["OpsDeploymentDate"] = pd.to_datetime(roster["OpsDeploymentDate"], format='%Y-%m-%d')

roster["Graduation_Date"] = roster["Graduation_Date"].astype('string')
roster["Graduation_Date"] = roster["Graduation_Date"].replace("Rolled-off", np.nan)
roster["Graduation_Date"] = pd.to_datetime(roster["Graduation_Date"], format='%Y-%m-%d')
roster = roster.query('OpsDeploymentDate.notnull()')

mmt = mmt.merge(roster, how='left', on='enterprise_id')

# Check for duplicates
dup_check = mmt.groupby(["enterprise_id", "roster_date"], as_index=False).size()
dup_check = dup_check.query('size != 1')

# Export
FileName = pd.Timestamp.today().strftime("%Y-%m-%d")
ExportPath = '//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW//01. Parquet/01. Backup/MMT Daily %s.parquet'%FileName
#mmt.to_csv((r"C:\Users\mario.canudo\Desktop\test\testmmt.csv"))
mmt.to_parquet(ExportPath)
mmt.to_parquet('//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW//01. Parquet/Latest_MMT.parquet')
#print(mmt.info())
print("MMT_IW Duplicates:")
print(dup_check.enterprise_id.unique())
#mmt.to_csv('//lisfs1003/honey_badger$/Operations - Management/Lisbon Reporting/26. WFM (Reporting)/15. MMT_IW/test_mmt.csv')
#print(mmt.status_final_AL.unique())
