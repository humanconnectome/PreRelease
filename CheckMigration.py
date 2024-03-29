import datetime
import pycurl
import sys
import shutil
from openpyxl import load_workbook
import pandas as pd
#import download.box
from io import BytesIO
import os
from shutil import copyfile
import numpy as np

auth=pd.read_csv("migrationconfig.csv")
def getredcap7(studystr,restrictedcols=[]):
    studydata = pd.DataFrame()
    token=auth.loc[auth.studystr==studystr,'token'].reset_index().token[0]
    subj = 'subject_id'
    idvar = 'id'
    if studystr=="parent7":
        subj='parent_id'
    if studystr=="ssaga7":
        subj='hcpa_id'
        idvar='study_id'
    data = {
        'token': token,
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    buf = BytesIO()
    ch = pycurl.Curl()
    ch.setopt(ch.URL, 'https://redcap.wustl.edu/redcap/srvrs/prod_v3_1_0_001/redcap/api/')
    ch.setopt(ch.HTTPPOST, list(data.items()))
    ch.setopt(ch.WRITEDATA, buf)
    ch.perform()
    ch.close()
    htmlString = buf.getvalue().decode('UTF-8')
    buf.close()
    df = pd.read_json(htmlString)
    dflink = df.loc[~(df[subj] == '')][[subj, idvar]]
    new = dflink[subj].str.split("_", 1, expand=True)
    df = pd.merge(dflink.drop(columns=subj), df, how='right', on=idvar)
    for dropcol in restrictedcols:
        try:
            df=df.drop(columns=dropcol)
        except:
            pass
    print(df.shape)
    return df


def red13(studystr,restrictedcols=[]):
    """
    downloads all events and fields in a redcap database
    """
    studydata = pd.DataFrame()
    token=auth.loc[auth.studystr==studystr,'token'].reset_index().token[0]
    data = {
        'token': token,
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    buf = BytesIO()
    ch = pycurl.Curl()
    ch.setopt(ch.URL, 'https://redcap.wustl.edu/redcap/api/')
    ch.setopt(ch.HTTPPOST, list(data.items()))
    ch.setopt(ch.WRITEDATA, buf)
    ch.perform()
    ch.close()
    htmlString = buf.getvalue().decode('UTF-8')
    buf.close()
    df = pd.read_json(htmlString)
    for dropcol in restrictedcols:
        #try:
        df=df.drop(columns=dropcol)
        #except:
        #    pass
    print(df.shape)
    return df


def diffcols(study7,study13,name7,name13):
    sevenonly=[i for i in study7.columns if i not in study13.columns]
    print('column in',name7,'only:')
    for i in sevenonly:
        print(i)
    print('')
    thirteenonly=[i for i in study13.columns if i not in study7.columns]
    print('column in',name13,'only:')
    for i in thirteenonly:
        print(i)
    print('')
    print("Seven Rows:",study7.shape[0])
    print("Thirteen Rows:", study13.shape[0])

def diffvals(study7,study13,sortvars=[],varlist=[],floatforce=[]):
    for a in floatforce:
        try:
            study7[a]=pd.to_numeric(study7[a],errors='coerce')
            study7[a] = study7[a].round(1).apply(np.floor)
            study13[a] = pd.to_numeric(study13[a], errors='coerce')
            study13[a]=study13[a].round(1).apply(np.floor)
        except:
            pass
    for a in varlist:
        try:
            study7[a] = study7[a].astype(str).str.strip()
            study7[a] = study7[a].str.replace('\r', '').str.replace('\n', '')
            study13[a] = study13[a].astype(str).str.strip()
            study13[a] = study13[a].str.replace('\r', '').str.replace('\n', '')
        except:
            pass
        try:
            study7[a] = pd.to_numeric(study7[a],errors='coerce')
            study13[a] = pd.to_numeric(study13[a],errors='coerce')
        except:
            pass
    diff=pd.merge(study7[sortvars],study13[sortvars],on=sortvars,how='inner')
    if diff.empty ==False:
        study7=pd.merge(diff,study7,on=sortvars,how='inner')
    study7 = study7.sort_values(by=sortvars).reset_index().copy()
    study13 = study13.sort_values(by=sortvars).reset_index()
    study7=study7.set_index(sortvars)
    study13=study13.set_index(sortvars)
    return study7[varlist].compare(study13[varlist])#, align_axis=0)

def prepdiff(study7,study13,name,sortv=['id','redcap_event_name'],dropcols=['salisaliva_mestrual']):
    cols = list(set(study7.columns).intersection(set(study13.columns)))
    formcomp=[i for i in list(cols) if 'complete' in i]
    dropvars=formcomp+dropcols
    notesvars=[i for i in list(cols) if 'note' in i or 'other' in i or 'des' in i or 'satisf' in i and i not in sortv+dropvars]
    nonnotesvars=[i for i in list(cols) if i not in sortv+dropvars+notesvars and 'complete' not in i]
    diffnotes=diffvals(study7,study13,sortvars=sortv,varlist=notesvars[1:500],floatforce=[])
    diffnotes.to_csv('Migration_NotesDiffs_'+name+'_21Dec2023.csv')
    floaties=['age','age_v2','age_v3','age_v6','covidy_age','covid18_age','insulin','creatinine','estradiol','potassium','ast_sgot','chloride','cholesterol','ureanitrogen','cbq_effcon']
    diffs1=diffvals(study7,study13,sortvars=sortv,varlist=nonnotesvars[0:500],floatforce=floaties)
    diffs1.to_csv('Migration_Diffs_part1_'+name+'_21Dec2023.csv')
    diffs2=diffvals(study7,study13,sortvars=sortv,varlist=nonnotesvars[500:1000],floatforce=floaties)
    diffs2.to_csv('Migration_Diffs_part2_'+name+'_21Dec2023.csv')
    diffs3=diffvals(study7,study13,sortvars=sortv,varlist=nonnotesvars[1000:1500],floatforce=floaties)
    diffs3.to_csv('Migration_Diffs_part3_'+name+'_21Dec2023.csv')
    diffs4=diffvals(study7,study13,sortvars=sortv,varlist=nonnotesvars[1500:2000],floatforce=floaties)
    diffs4.to_csv('Migration_Diffs_part4_'+name+'_21Dec2023.csv')
    diffs5=diffvals(study7,study13,sortvars=sortv,varlist=nonnotesvars[2000:2500],floatforce=floaties)
    diffs5.to_csv('Migration_Diffs_part5_'+name+'_21Dec2023.csv')
    diffs6=diffvals(study7,study13,sortvars=sortv,varlist=nonnotesvars[2500:3000],floatforce=floaties)
    diffs6.to_csv('Migration_Diffs_part6_'+name+'_21Dec2023.csv')
    diffs7=diffvals(study7,study13,sortvars=sortv,varlist=nonnotesvars[3000:3500],floatforce=floaties)
    diffs7.to_csv('Migration_Diffs_part7_'+name+'_21Dec2023.csv')
    diffs8=diffvals(study7,study13,sortvars=sortv,varlist=nonnotesvars[3500:],floatforce=floaties)
    diffs8.to_csv('Migration_Diffs_part8_'+name+'_21Dec2023.csv')


def comparedictions(dlpath1,sev1,thirt1):
    seven1 = dlpath1 + sev1
    thirteen1 = dlpath1 + thirt1
    sevdict=pd.read_csv(seven1).drop(columns=['Field Label'])
    sevdict['Choices, Calculations, OR Slider Labels']=sevdict['Choices, Calculations, OR Slider Labels'].str.replace(" ","")
    sevdict['Field Annotation']=sevdict['Field Annotation'].str.replace(" ","")
    sevdict['Branching Logic (Show field only if...)']=sevdict['Branching Logic (Show field only if...)'].str.replace(" ","")
    thirt=pd.read_csv(thirteen1).drop(columns=['Field Label'])
    thirt['Choices, Calculations, OR Slider Labels'] = thirt['Choices, Calculations, OR Slider Labels'].str.replace(" ", "")
    thirt['Field Annotation'] = thirt['Field Annotation'].str.replace(" ", "")
    thirt['Branching Logic (Show field only if...)']=thirt['Branching Logic (Show field only if...)'].str.replace(" ","")
    incommon=pd.merge(thirt[['Variable / Field Name']],sevdict[['Variable / Field Name']], on='Variable / Field Name',how='outer',indicator=True)
    sevenonly=incommon.loc[incommon._merge=='right_only']
    thirteenonly = incommon.loc[incommon._merge == 'left_only']
    only=pd.concat([sevenonly,thirteenonly])
    incommon = incommon.loc[incommon._merge == 'both'].drop(columns=['_merge'])
    common = list(incommon['Variable / Field Name'])
    sevdict=sevdict.loc[sevdict['Variable / Field Name'].isin(common)].copy()
    thirt = thirt.loc[thirt['Variable / Field Name'].isin(common)].copy()
    thirt.reset_index(inplace=True)
    sevdict.reset_index(inplace=True)
    sevdict.columns=thirt.columns
    sevdict = sevdict.set_index('Variable / Field Name')
    thirt = thirt.set_index('Variable / Field Name')
    sevdict.index=thirt.index
    #sevdict.to_csv('test7.csv')
    #thirt.to_csv('test13.csv')
    compare=sevdict.compare(thirt)  # , align_axis=0)
    return compare, only
dlpath1='/Users/petralenzini/work/Behavioral/Lifespan/PreRelease/PreRelease/'

#HCA
hca7=getredcap7('hca7',restrictedcols=[])
hca13=red13('hca13',restrictedcols=[])
diffcols(hca7,hca13,'HCA7','HCA13')
droppedhca7=pd.merge(hca7,hca13[['id', 'redcap_event_name']], on=['id', 'redcap_event_name'], how='outer', indicator=True)
#check
droppedhca7.loc[droppedhca7._merge=='left_only']
droppedhca7.loc[droppedhca7._merge=='right_only']
#check values in spreadsheeets:
prepdiff(hca7,hca13,"HCA",sortv=['id','redcap_event_name'],dropcols=[])

#HCA dictionary
thirt1='HCPAV13Destination_DataDictionary_2024-01-02.csv'
sev1='HCPA_DataDictionary_2024-01-02.csv'
hcadiffs,only=comparedictions(dlpath1,sev1,thirt1)
hcadiffs.to_csv(dlpath1+"HCA_Dictionaries_v7vs13_inboth.csv")
only.to_csv(dlpath1+"HCA_Dictionaries_v7vsv13_onlyinone.csv")

############################################
#HCD Child
child7=getredcap7('child7',restrictedcols=[])
child13=red13('child13',restrictedcols=[])
diffcols(child7,child13,'CHILD7','CHILD13')
#check
droppedchild7=pd.merge(child7,child13[['id', 'redcap_event_name']], on=['id', 'redcap_event_name'], how='outer', indicator=True)
droppedchild7.loc[droppedchild7._merge=='left_only'].to_csv('Migration_Dropped_Empty_Child_21Dec2023.csv',index=False)
#check values in spreadsheeets:
#misrepcap variables are housekeeping variables.  They won't load.  Maybe because of the quadruple underscore?  Either way, they are
#not helpful or consistent and should be dropped
mrep=[mr for mr in list(child7.columns) if 'missrepcap' in mr]
prepdiff(child7,child13,"HCD-Child",sortv=['id','redcap_event_name'],dropcols=[]+mrep)
### Columns and Rows check PASS. Dropped rows are empty
#CHILD dictionary
thirt1='HCPDChildMigrate_DataDictionary_2024-01-02.csv'
sev1='HCPDChild_DataDictionary_2024-01-02.csv'
hcadiffs,only=comparedictions(dlpath1,sev1,thirt1)
hcadiffs.to_csv(dlpath1+"Child_Dictionaries_v7vs13_inboth.csv")
only.to_csv(dlpath1+"Child_Dictionaries_v7vsv13_onlyinone.csv")

##################################################
#PARENT
parent7=getredcap7('parent7',restrictedcols=[])
parent13=red13('parent13',restrictedcols=[])
diffcols(parent7,parent13,'PARENT7','PARENT13')
#check values in spreadsheeets:
prepdiff(parent7,parent13,"HCD-Parent",sortv=['id','redcap_event_name'],dropcols=[])
## Columns and Rows check PASS

#Parent dictionary
thirt1='HCPDParentMigrate_DataDictionary_2024-01-02.csv'
sev1='HCPDParent_DataDictionary_2024-01-02.csv'
hcadiffs,only=comparedictions(dlpath1,sev1,thirt1)
hcadiffs.to_csv(dlpath1+"Parent_Dictionaries_v7vs13_inboth.csv")
only.to_csv(dlpath1+"Parent_Dictionaries_v7vsv13_onlyinone.csv")

#TEEN
teen7=getredcap7('teen7',restrictedcols=[])
teen13=red13('teen13',restrictedcols=[])
diffcols(teen7,teen13,'TEEN7','TEEN13')
teen7.misscog___.value_counts(dropna=False)
prepdiff(teen7,teen13,"HCD-Teen",sortv=['id','redcap_event_name'],dropcols=[])
## Columns and Rows check PASS
thirt1='HCPD18Migrate_DataDictionary_2024-01-02.csv'
sev1='HCPD18_DataDictionary_2024-01-02.csv'
hcadiffs,only=comparedictions(dlpath1,sev1,thirt1)
hcadiffs.to_csv(dlpath1+"Teen_Dictionaries_v7vs13_inboth.csv")
only.to_csv(dlpath1+"Teen_Dictionaries_v7vsv13_onlyinone.csv")

ssaga7=getredcap7('ssaga7',restrictedcols=[])
ssaga13=red13('ssaga13',restrictedcols=[])
diffcols(ssaga7,ssaga13,'SSAGA7','SSAGA13')
prepdiff(ssaga7,ssaga13,"SSAGA",sortv=['study_id','redcap_event_name'],dropcols=[])
thirt1='SSAGAmigrate_DataDictionary_2024-01-02.csv'
sev1='SSAGA_DataDictionary_2024-01-02.csv'
hcadiffs,only=comparedictions(dlpath1,sev1,thirt1)
hcadiffs.to_csv(dlpath1+"SSAGA_Dictionaries_v7vs13_inboth.csv")
only.to_csv(dlpath1+"SSAGA_Dictionaries_v7vsv13_onlyinone.csv")


