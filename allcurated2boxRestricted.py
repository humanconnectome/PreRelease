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

import ccf
from ccf.box import LifespanBox




# This code walks through each of the external datatypes (except pedigrees), subsets to subjects/events in inventory
# (see Curated Inventory.ipynb in https://github.com/humanconnectome/PreRelease)
# and filters out variables with PHI
# KSADS additionally gets a makeover and downstream datatype for cases where MOOD batteries need to be merged into regular record
# PG-13 unfiltered data types (i.e. with dates, ages>90 and freetext that might mention 'John')
# can be found on the 'restricted' paths.
# G-rated (our best effort at scrubbing 26,605 varialbes) data and annotation can be found in Asnaps and Dsnps


verbose = True
snapshotdate = datetime.datetime.today().strftime('%Y-%m-%d')
snapshotdate
box_temp='./boxtemp' #location of local copy of curated data
box = LifespanBox(cache=box_temp)
redcapconfigfile="./.boxApp/redcapconfig.csv"
redcap9configfile="./.boxApp/redcap9config.csv"

Asnaps=126706803362
Dsnaps=126781658067
ArestrictSnaps=150224568988
DrestrictSnaps=150226955672

#REORGANIZED AROUND INVENTORY:
#this is the latest inventory

inventorypath='/home/petra/CCF_QC/PreRelease/'
#version='2022_01_19'
versionold='02_04_2022'
#version=snapshotdate
snapshotdate

inventoryA=pd.read_csv(inventorypath+'HCA_AllSources_' + versionold + '.csv')
inventoryD=pd.read_csv(inventorypath+'HCD_AllSources_' + versionold + '.csv')
goodidsD=list(inventoryD.subject.unique())
goodidsA=list(inventoryA.subject.unique())

#rename before upload for consistency with other files
'''Maybe {HCD,HCA}_<InstrumentName>_DataDictionary_YYYY_MM_DD.csv
{Inventory, KSADS, RedCap, RedCap_Child, RedCap_Teen, RedCap_Parent, NIH_Toolbox_Scores, NIH_Toolbox_Raw, Q_Interactive, PennCNP, Eprime, Apoe_Isoforms, Pedigrees}
'''

copyfile(inventorypath+'HCA_AllSourcesSlim_'+versionold+'.csv',inventorypath+'HCA_Inventory_'+snapshotdate+'.csv')
copyfile(inventorypath+'HCA_AllSources_'+versionold+'.csv',inventorypath+'HCA_Inventory_Restricted_'+snapshotdate+'.csv')
copyfile(inventorypath+'HCD_AllSourcesSlim_'+versionold+'.csv',inventorypath+'HCD_Inventory_'+snapshotdate+'.csv')
copyfile(inventorypath+'HCD_AllSources_'+versionold+'.csv',inventorypath+'HCD_Inventory_Restricted_'+snapshotdate+'.csv')

box.upload_file(inventorypath+'HCA_Inventory_'+snapshotdate+'.csv', Asnaps)
box.upload_file(inventorypath+'HCA_Inventory_Restricted_'+snapshotdate+'.csv', ArestrictSnaps)
box.upload_file(inventorypath+'HCD_Inventory_'+snapshotdate+'.csv', Dsnaps)
box.upload_file(inventorypath+'HCD_Inventory_Restricted_'+snapshotdate+'.csv', DrestrictSnaps)


##############################
#get list of legit HCD PINS
inventoryD.columns
a=pd.DataFrame(inventoryD.loc[(inventoryD.ParentPIN.isnull()==False) & (inventoryD.Curated_TLBX_Parent.isin(['YES','YES BUT'])),'ParentPIN'])
a=a.loc[~(a.ParentPIN=="")]
b=list(a.ParentPIN)
c=pd.DataFrame(inventoryD.loc[(inventoryD.PIN.isnull()==False) & (inventoryD.Curated_TLBX.isin(['YES','YES BUT'])),'PIN'])
c=c.loc[~(c.PIN=="")]
d=list(c.PIN)
goodPINSD=b+d
print(len(goodPINSD))
#get list of legit HCA PINS
c=pd.DataFrame(inventoryA.loc[(inventoryA.PIN.isnull()==False) & (inventoryA.Curated_TLBX.isin(['YES','YES BUT'])),'PIN'])
c=c.loc[~(c.PIN=="")]
d=list(c.PIN)
goodPINSA=d
print(len(goodPINSA))


#Restricted Variables:
mask_file=[887050736739]
def getlist(mask,sheet):
    restrictA=pd.read_excel(mask, sheet_name=sheet)
    restrictedA=list(restrictA.field_name)
    return restrictedA
box=LifespanBox(cache='./')

a=box.download_files(mask_file)

restrictedA=getlist(a[0],'HCA')
restrictedCh=getlist(a[0],'HCP-D Child')
restricted18=getlist(a[0],'HCD 18+')
restrictedParent=getlist(a[0],'HCD Parent')
restrictedQ=getlist(a[0],'Q')
restrictedK=getlist(a[0],'ksads')
restrictedS=getlist(a[0],'SSAGA')
restrictedTLBXS=getlist(a[0],'TLBX_Scores')
restrictedTLBXR=getlist(a[0],'TLBX_Raw')

ddict_file=[905784566785]
b=box.download_files(ddict_file)
b2=pd.read_excel(b[0], sheet_name='VariablesInMoodRecords')
moodvars=list(b2.varsInMood)


#note that some of the parameters in these macros not used anymore...wanted to leave them in in future versions of this code
# coudl figure out how to be more concise.

def getredcap7(studystr,curatedsnaps,restrictedsnaps,flaggedgold=pd.DataFrame(),restrictedcols=[]):
    """
    downloads all events and fields in a redcap database
    """
    studydata = pd.DataFrame()
    auth = pd.read_csv(redcapconfigfile)
    token=auth.loc[auth.study==studystr,'token'].reset_index().token[0]
    subj=auth.loc[auth.study==studystr,'field'].reset_index().field[0]
    idvar='id'
    if studystr=='ssaga':
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
    dflink['subject'] = new[0].str.strip()
    dflink['flagged'] = new[1].str.strip()
    dflink['study'] = studystr
    df = pd.merge(dflink.drop(columns=subj), df, how='right', on=idvar)
    flaggedids=df.loc[df.flagged.isnull()==False][['subject','flagged']]
    print(df.shape)
    df['redcap_event'] = df.replace({'redcap_event_name':
                                                 {'visit_1_arm_1': 'V1',
                                                  'follow_up_1_arm_1': 'F1',
                                                  'visit_arm_1':'V1',
                                                  'visit_2_arm_1': 'V2',
                                                  'visit_3_arm_1': 'V3',
                                                  'follow_up_2_arm_1': 'F2',
                                                  'follow_up_arm_1': 'F1',
                                                  'covid_arm_1': 'Covid',
                                                  'follow_up_3_arm_1': 'F3',
                                                  'covid_remote_arm_1': 'CR',
                                                  'covid_2_arm_1':'Covid2',
                                                  'actigraphy_arm_1': 'A',
                                                  }})['redcap_event_name']
    dfrestricted=df.copy() #[[idvar, 'subject', 'redcap_event_name']+restrictedcols] #send full set so not need merge
    for dropcol in restrictedcols:
        try:
            df=df.drop(columns=dropcol)
        except:
            pass
    print(df.shape)
    return flaggedids, df, dfrestricted

#these are misnomers.  please flip studystr and idstring when calling function
def getredcap10Q(studystr,curatedsnaps,goodies,idstring,restrictedcols=[]):
    """
    downloads all events and fields in a redcap database
    """
    studydata = pd.DataFrame()
    auth = pd.read_csv(redcap9configfile)
    print(auth)
    token=auth.loc[auth.study==studystr,'token'].reset_index().token[0]
    subj=auth.loc[auth.study==studystr,'field'].reset_index().field[0]
    print(token)
    print(subj)
    idvar='id'
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
    print(df.shape)
    if (studystr=='qint'):
        print('Dropping unusuable Q records')
        print(df.shape)
        df=df.loc[~(df.q_unusable=='1')]
        print(df.shape)
        df['subject']=df[subj]
        df['redcap_event']='V'+df.visit.astype('str')
        df.loc[df.redcap_event=='VCR','redcap_event']='CR'
        if(idstring=='HCD'):
            df=df.loc[df[subj].str.contains('HCD')].copy()
            df = df.loc[~(df.assessment.str.contains('RAVLT'))].copy()
            cols = [c for c in df.columns if c.lower()[:5] != 'ravlt']
            df = df[cols].copy()
        if(idstring=='HCA'):
            df=df.loc[df[subj].str.contains('HCA')]
            df = df.loc[df.assessment.str.contains('RAVLT')].copy()
            print(len(df.columns))
            cols = [c for c in df.columns if c.lower()[:4] != 'wais']
            cols = [c for c in cols if c[:4] != 'wisc']
            cols = [c for c in cols if c[:4] != 'wpps']
            print(len(cols))
            df = df[cols].copy()
    if (studystr == 'ksads'):
        print('Dropping unusuable K records')
        print(df.shape)
        df = df.loc[~(df.k_unusable == '1')]
        print(df.shape)
    print(df.shape)
    print('Dropping exclusions/DNRs/Withdrawns')
    #for sb in list(flaggedgold.subject):
    df=df.loc[(df[subj].str[:10].isin(goodies))].copy()
    df=df.loc[~(df[subj].str.contains('CC'))].copy()

    print(df.shape)
    if (studystr=='qint'):
        dfrestricted=df.copy() #[['id', 'subjectid', 'visit']+restrictedcols]
    if (studystr=='ksads'):
        dfrestricted=df.copy() #[['id', 'patientid', 'patienttype' ]+restrictedcols]
    for dropcol in restrictedcols:
        #try:
        df=df.drop(columns=dropcol)
        #except:
        #    pass
    print(df.shape)
    return df, dfrestricted

'''Maybe {HCD,HCA}_<InstrumentName>_DataDictionary_YYYY_MM_DD.csv
{Inventory, KSADS, RedCap, RedCap_Child, RedCap_Teen, RedCap_Parent, NIH_Toolbox_Scores, NIH_Toolbox_Raw, Q_Interactive, PennCNP, Eprime, Apoe_Isoforms, Pedigrees}
'''

############### KSADS ############### only upload data after mood merge
#needs to be ksads and not Ksads here because ksads is specific reference to the authorization file handle
kD,kDrestricted=getredcap10Q('ksads',Dsnaps,goodidsD,'HCD',restrictedcols=restrictedK)

#kD.to_csv(box_temp + '/' + studystr + idstring + '_' + snapshotdate + '.csv', index=False)
#kDrestricted.to_csv(box_temp + '/' + studystr + idstring + '_Restricted_' + snapshotdate + '.csv', index=False)
#don't upload these again.  Only upload the MOOD merged ones.
    #box.upload_file(box_temp+'/REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv', Dsnaps)
    #box.upload_file(box_temp+'/Restricted_REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv', DrestrictSnaps)

#kD is the smaller open access dataset
#kDrestricted is the larger restricted access dataset
whole=kDrestricted.copy()
#subject is the same as patientid, except stripped of Mood, where applicable...
# note that this only will work because data have already been cleaned of
#unusables (which have duplicates).

# Can't use regular old update() because only some of the columns have new information for some of the rows.
whole['subject']=whole['patientid'].str.upper().str.replace('MOOD','').str.replace(' ','').str.replace('-','_').str[:13]

#Mood batteries (T and P) for some subjects.  Need to get the T and P for regular correspondeces.
moodlist=whole.loc[whole.patientid.str.upper().str.contains('MOOD')]#[['patientid','patienttype','subject']]
print(moodlist.shape)

#subject records who don't have mood (group A)
#drop all mood records
A1=whole.loc[~(whole.patientid.str.upper().str.contains('MOOD'))].copy()
print(A1.shape)
#drop regular records for subjects who have moods records too to prevent duplication when they get rolled in later
#cant just drop.  Need to merge by patienttype
Asub=pd.merge(A1,moodlist[['patienttype','subject']],how='outer',on=['patienttype','subject'],indicator=True)
Asub._merge.value_counts()
#just keep the ones in left_only
A=Asub.loc[Asub._merge=='left_only'].drop(columns=['_merge']).copy()

print("A1 shape",A1.shape) #402 fewer than whole (dropped all mood records)
print("A shape",A.shape) #should be 400 fewer records than A1 - eg. the 400 with normie pairs (2 didn't have normies)
#A should have 802 fewer records than total.  Gonna add back two in b and 400 in C.  Final total should have 2528

#find subject who have only mood records e.g no regular records ;consider rename them as regular records withnote (group B)
B1=pd.merge(A1,moodlist[['patienttype','subject']],how='right',on=['patienttype','subject'],indicator=True).copy()
B2=B1.loc[B1._merge=='right_only'].copy()  #2
B=whole.loc[whole.subject.isin(list(B2.subject))].copy()#['HCD0092334_V1','HCD1229239_V1'])]
B.patientid=B.subject
B.additionalinfo='Data are usable but only include MOOD batteries'

#print(whole.loc[whole.subject=='HCD0092334_V1'][['patientid','dateofinterview','patienttype']])
#print(whole.loc[whole.subject=='HCD1229239_V1'][['patientid','dateofinterview','patienttype']])
#find subjects who have both regular and mood records (both) - separate from rest. (group C)
C=pd.merge(whole,moodlist[['patienttype','subject']],how='inner',on=['patienttype','subject']).copy()
C.shape  #802 - this is 400 subjects with both mood and regular plus two subjects with just mood.

#802+2 =804.  804/2=402 = same as moodlist shape. 8293 variables
#for Group C - separate into C1 and C2.
# C2 has moodies. C1 has normies.
C2=C.loc[C.patientid.str.upper().str.contains('MOOD')].copy()
#drop the records from B which need a rename, not a column overwrite
C2=C2.loc[~(C2.patientid.isin(['HCD0092334_V1_mood','HCD1229239_V1_mood']))].copy()

C1=C.loc[~(C.patientid.str.upper().str.contains('MOOD'))].copy()
print(C1.shape)
print(C2.shape)

#restrict C1 to moodvars,
# sort by subject and patientid and then reset both indexes so that the following works
#e.g.
moodvars
df1=C1.sort_values(by=['subject','patienttype']).reset_index() #regular records that need to be updated with mood varialbes
df1=df1.drop(columns=['index']).copy()
df2=C2.sort_values(by=['subject','patienttype']).reset_index() #mood records
#restrict df2 to only the moodvars
a=list(set(list(C2.columns)) & set(moodvars))
print("Number Moodvars:",len(moodvars))
print("intersection moodvars and reg",len(a)) #should be one fewers since C2 doesnt have 'Variable / Field Name'

df2=df2[a].copy()
dropmore=['patientid','patienttype','dateofinterview']
df2=df2.drop(columns=dropmore).copy()
#df1 and df2 have same number of rows, indentical indices, and are matched by patientid and patienttype so...
#replace the columns in df1 with corresponding columns in 2
print("C1 shape",C1.shape)
print("C2 shape=",C2.shape)
print("df1 shape=",df1.shape)
print("df2 shape=",df2.shape)
print("number of blanks before update:",(df1=='').sum(axis=0).sum())
print("number of blanks in C1 before update:",(C1=='').sum(axis=0).sum())

for col in df2.columns:
    try:
        col_pos = list(df1.columns).index(col)
        df1.drop(columns=[col], inplace=True)
        df1.insert(col_pos, col, df2[col])
    except ValueError:
        df1[col] = df2[col]

print("C1 shape",C1.shape)
print("C2 shape=",C2.shape)
print("df1 shape=",df1.shape)
print("df2 shape=",df2.shape)
print("number of blanks in df2 after update:",(df1=='').sum(axis=0).sum())

#check that the shape of the result is the same as C2=df1 and that the result has more information than prior to find a replace
#e.g. C1 shape (400, 8294) = new df1 shape.
#concatenate the pieces back together for restriction mask then upload
D=pd.concat([A,B,df1],axis=0)
print('Shape of Original whole',whole.shape)
print("number of blanks in whole before update:",(whole=='').sum(axis=0).sum())

print('Shape of D',D.shape)
D.loc[D.patientid.isin(['HCD0092334_V1','HCD1229239_V1'])][['subject','k_unusable_specify']]
print("number of blanks in D after update:",(D=='').sum(axis=0).sum())

D['PIN']=D.subject
new = D['PIN'].str.split("_", 1, expand=True)
D['redcap_event']=new[1]
D['subject']=new[0]
Dopen=D.copy()
Dopen=Dopen.drop(columns=restrictedK)

#Dopen.to_csv(box_temp+'/MoodMerge_REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv',index=False)
#D.to_csv(box_temp+'/MoodMerge_Restricted_REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv',index=False)
Dopen.to_csv(box_temp+'/HCD_KSADS'+'_'+snapshotdate+'.csv',index=False)
D.to_csv(box_temp+'/HCD_KSADS'+'_Restricted_'+snapshotdate+'.csv',index=False)
#2928 KSADS P,T, and P mood, Tmood batteries in inventory as of 11/19/2021
#2526 records after merging in the moods.

box.upload_file(box_temp+'/HCD_KSADS'+'_'+snapshotdate+'.csv', Dsnaps)
box.upload_file(box_temp+'/HCD_KSADS'+'_Restricted_'+snapshotdate+'.csv', DrestrictSnaps)
##########################################

'''Maybe {HCD,HCA}_<InstrumentName>_DataDictionary_YYYY_MM_DD.csv
{Inventory, KSADS, RedCap, RedCap_Child, RedCap_Teen, RedCap_Parent, NIH_Toolbox_Scores, NIH_Toolbox_Raw, Q_Interactive, PennCNP, Eprime, Apoe_Isoforms, Pedigrees}
'''

############### Qinteractive ###############
qA,qArestricted=getredcap10Q('qint',Asnaps,goodidsA,'HCA',restrictedcols=restrictedQ)
idstring='Q-Interactive'
studystr='HCA'

qA.drop(columns='unusable_specify').to_csv(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', index=False)
qArestricted.drop(columns='unusable_specify').to_csv(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', index=False)

box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', Asnaps)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', ArestrictSnaps)


qD,qDrestricted=getredcap10Q('qint',Dsnaps,goodidsD,'HCD',restrictedcols=restrictedQ)
idstring='Q-Interactive'
studystr='HCD'
qD.drop(columns='unusable_specify').to_csv(box_temp + '/' + studystr + '_'+ idstring + '_' + snapshotdate + '.csv', index=False)
qDrestricted.drop(columns='unusable_specify').to_csv(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', index=False)

box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', Dsnaps)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', DrestrictSnaps)

#Note: as of 11/18 there were 5 extra HCA CR records in data but not inventory due to missing CR survey (and event date)
#this is just QC to confirm 5 extra records in HCA and none in HCD
test=pd.merge(inventoryA[['subject','redcap_event']],qArestricted,left_on=['subject','redcap_event'],right_on=['subject','redcap_event'],how='outer',indicator=True)
test.loc[test._merge=='right_only' ][['subject','redcap_event']]
#qint.shape
test2=pd.merge(inventoryD[['subject','redcap_event']],qDrestricted,left_on=['subject','redcap_event'],right_on=['subject','redcap_event'],how='outer',indicator=True)
test2.loc[test2._merge=='right_only' ][['subject','redcap_event']]
#end QC



############### HCA database + define list of excluded subjects  ##########################################
#create the hca dataframes for export - note that export functionality has been disabled in function definition.
flaggedhcpa, df, dfrestricted=getredcap7('hcpa',Asnaps,ArestrictSnaps,flaggedgold=pd.DataFrame(),restrictedcols=restrictedA)
#now merge with inventory to get rid of empty events and make doubly sure there are no excluded subjects
#some subjects did Covid1 but not covid2 and vice versa.  Both are the 'covid' 'redcap_event_name'.
test=inventoryA.drop_duplicates(subset=['subject','REDCap_id','redcap_event_name'])

inventdf=pd.merge(test[['REDCap_id','redcap_event_name']],df, left_on=['REDCap_id','redcap_event_name'],right_on=['id','redcap_event_name'],how='left')
inventdf=inventdf.drop(columns='REDCap_id')
print(df.shape)
print(inventoryA.shape)
print(inventdf.shape)

#restricted
inventdfrestricted=pd.merge(test[['REDCap_id','redcap_event_name']],dfrestricted, left_on=['REDCap_id','redcap_event_name'],right_on=['id','redcap_event_name'],how='left')
inventdfrestricted=inventdfrestricted.drop(columns='REDCap_id')
print(df.shape)
print(test.shape)
print(inventdfrestricted.shape)

'''Maybe {HCD,HCA}_<InstrumentName>_DataDictionary_YYYY_MM_DD.csv
{Inventory, KSADS, RedCap, RedCap_Child, RedCap_Teen, RedCap_Parent, NIH_Toolbox_Scores, NIH_Toolbox_Raw, Q_Interactive, PennCNP, Eprime, Apoe_Isoforms, Pedigrees}
'''

restrictedsnaps=ArestrictSnaps
studystr='HCA'
idstring='RedCap'
inventdf.to_csv(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', index=False)
inventdfrestricted.to_csv(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv',index=False)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', Asnaps)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', ArestrictSnaps)

############################################################
flaggedssaga, dfss, dfssres=getredcap7('ssaga',Asnaps,ArestrictSnaps,flaggedgold=pd.DataFrame(),restrictedcols=restrictedS)

link=dfss.loc[dfss.hcpa_id.isnull()==False][['hcpa_id','study_id']]
link=link.loc[~(link.hcpa_id=="")]
print(link.shape)
print(inventoryA.shape)
test=inventoryA.loc[~(inventoryA.sub_event=='7.Covid1')] #remove source of dups irrelevant to ssaga (it was relevent to HCPA records so couldnt do this there)
print(test.shape)
test=test.drop_duplicates(subset=['subject','REDCap_id','redcap_event_name'])
print(test.shape)
test=test.loc[test.Curated_SSAGA.isin(['YES','YES BUT'])]#1789 on 11/19/21
test.shape
test=pd.merge(test,link,left_on='subject',right_on='hcpa_id',how='left')#[['study_id','redcap_event_name']]
print(test.shape) #these are the subevent=covid1 people with SSAGA and visit data
test2=test[['study_id','redcap_event_name','redcap_event','subject']]
test2
#add four who aren't in inventory because have ssaga but no v2 in main REDCap- the rest of the discrepancies (1831 vs 1793) are empty ssagas or withdrawns
extrasubjects=dfss.loc[(dfss.study_id.isin(["9", "12", "9532-280", "9533-257"])) & (dfss.redcap_event_name=='visit_2_arm_1')][['study_id','redcap_event_name']]
test3=pd.concat([test2,extrasubjects])
print(test3.shape)

inventss=pd.merge(test3.drop(columns=['redcap_event', 'subject']),dfss, left_on=['study_id','redcap_event_name'],right_on=['study_id','redcap_event_name'],how='left')
inventss=inventss.drop(columns='study_id')
print(dfss.shape) #if not 1831 - see note above and find the change
print(inventss.shape)

inventssres=pd.merge(test3.drop(columns=['redcap_event', 'subject']),dfssres, left_on=['study_id','redcap_event_name'],right_on=['study_id','redcap_event_name'],how='left')
inventssres=inventssres.drop(columns='study_id')
print(dfssres.shape)
print(inventssres.shape)

curatedsnaps=Asnaps
restrictedsnaps=ArestrictSnaps

studystr='HCA'
idstring='SSAGA'

inventss.to_csv(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', index=False)
inventssres.to_csv(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv',index=False)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', Asnaps)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', ArestrictSnaps)




#   HCD child   ####################################### #############################################
flaggedhcpd, dfc, dfcrestricted=getredcap7('hcpdchild',Dsnaps,DrestrictSnaps,flaggedgold=pd.DataFrame(),restrictedcols=restrictedCh)

testD=inventoryD.loc[~(inventoryD.DB_Source.isin(['teen','parent_only']))][['REDCap_id','redcap_event_name']]
testD=testD.drop_duplicates()

inventdfc=pd.merge(testD[['REDCap_id','redcap_event_name']],dfc, left_on=['REDCap_id','redcap_event_name'],right_on=['id','redcap_event_name'],how='left')
inventdfc=inventdfc.drop(columns='REDCap_id')
print(dfc.shape)
print(testD.shape)

len(flaggedhcpd)

#print(inventdfc.shape)
#for i in list(flaggedhcpd.subject):
#    print(i)
#    if i in inventdfc.subject:
#        print("CHECK")

#restricted
inventdfcr=pd.merge(testD[['REDCap_id','redcap_event_name']],dfcrestricted, left_on=['REDCap_id','redcap_event_name'],right_on=['id','redcap_event_name'],how='left')
inventdfcr=inventdfcr.drop(columns='REDCap_id')
print(dfcrestricted.shape)
print(testD.shape)
print(inventdfcr.shape)
###you are here###
idstring='RedCap-Child'
studystr='HCD'

inventdfc.to_csv(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', index=False)
inventdfcr.to_csv(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv',index=False)
inventdfcr[['subject','redcap_event']]
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', Dsnaps)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', DrestrictSnaps)

#Asnaps=126706803362
#Dsnaps=126781658067
#ArestrictSnaps=150224568988
#DrestrictSnaps=150226955672

####### HCD 18  ####################################################################################
flagged18, df18, df18restricted=getredcap7('hcpd18',Dsnaps,DrestrictSnaps,flaggedgold=pd.DataFrame(),restrictedcols=restricted18)

testD18=inventoryD.loc[(inventoryD.DB_Source.isin(['teen']))][['REDCap_id','redcap_event_name']]
testD18=testD18.drop_duplicates()

#temp=pd.merge(testD18[['REDCap_id','redcap_event_name']],df18, left_on=['REDCap_id','redcap_event_name'],right_on=['id','redcap_event_name'],how='outer',indicator=True)


inventd18=pd.merge(testD18[['REDCap_id','redcap_event_name']],df18, left_on=['REDCap_id','redcap_event_name'],right_on=['id','redcap_event_name'],how='left')
inventd18=inventd18.drop(columns='REDCap_id')
print(df18.shape)
print(testD18.shape)
print(inventd18.shape)
#for i in list(flaggedhcpd.subject):
#    print(i)
#    if i in inventdfc.subject:
#        print("CHECK")

#restricted
inventdfcr18=pd.merge(testD18[['REDCap_id','redcap_event_name']],df18restricted, left_on=['REDCap_id','redcap_event_name'],right_on=['id','redcap_event_name'],how='left')
inventdfcr18=inventdfcr18.drop(columns='REDCap_id')
print(df18restricted.shape)
print(testD18.shape)
print(inventdfcr18.shape)


idstring='RedCap-Teen'
studystr='HCD'


inventd18.to_csv(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', index=False)
inventdfcr18.to_csv(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv',index=False)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', Dsnaps)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', DrestrictSnaps)


######### HCD parents ###################################################################################
flaggedparent, dfparent, dfparentrest=getredcap7('hcpdparent',Dsnaps,DrestrictSnaps,flaggedgold=pd.DataFrame(),restrictedcols=restrictedParent)
#need to not restrict the two-parent cases
#both dfparent and dfparentrest have 'subject' equal to parent_at_V1.  This is not useful.
#change subject to child subject
dfparent=dfparent.rename(columns={'subject':'parent_at_V1'})
dfparentrest=dfparentrest.rename(columns={'subject':'parent_at_V1'})

parentD=inventoryD.loc[~(inventoryD.DB_Source.isin(['teen','child_only']))][['REDCap_id_parent','redcap_event_name','subject']]
inventp=pd.merge(parentD[['REDCap_id_parent','redcap_event_name','subject']],dfparent, left_on=['REDCap_id_parent','redcap_event_name'],right_on=['id','redcap_event_name'],how='left')
inventp=inventp.drop(columns='REDCap_id_parent')
print(inventp.shape)
#listgoodies=list(inventp.id.unique()) #these redcap ids are of parents of legit children

#need a few extra ids for the specialty cases
#dfparent.loc[dfparent.parent_id=='HCD3062037'][['child_id','parent_id','id']]
#dfparent.loc[dfparent.parent_id=='HCD4351251'][['child_id','parent_id','id']]
#dfparent.loc[dfparent.parent_id=='HCD5555474'][['child_id','parent_id','id']]
extraids=['6105-302','6106-255','6106-159'] #one has two events
extraparents=dfparent.loc[dfparent.id.isin(extraids)]
'''     parent_at_V1        id
1778   HCD3062037  6105-302
2287   HCD5555474  6106-159
2522   HCD4351251  6106-255
2523   HCD4351251  6106-255
'''
#put them together
parents=pd.concat([inventp,extraparents])
print(parents.shape)

#restricted
inventpr=pd.merge(parentD[['REDCap_id_parent','redcap_event_name','subject']],dfparentrest, left_on=['REDCap_id_parent','redcap_event_name'],right_on=['id','redcap_event_name'],how='left')
inventpr=inventpr.drop(columns='REDCap_id_parent')
print(inventpr.shape)

extraparentsr=dfparentrest.loc[dfparentrest.id.isin(extraids)]
#now get the accidentally uninventoried covid_arm_1, but only for the goodies.
#extra_armr=dfparentrest.loc[(dfparentrest.redcap_event_name=='covid_arm_1') & (dfparentrest.id.isin(extragoodies))]

#put them together
parentsr=pd.concat([inventpr,extraparentsr])
print(parentsr.shape)


curatedsnaps=Dsnaps
restrictedsnaps=DrestrictSnaps
idstring='RedCap-Parent'
studystr='HCD'

parents.to_csv(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', index=False)
parentsr.to_csv(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', index=False)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', Dsnaps)
box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', DrestrictSnaps)


##############################


eprime=box.downloadFile(495490047901)
eprimed=pd.read_csv(eprime,header=0)
eprimed=eprimed.loc[(eprimed.subject.isin(goodidsD))].copy()
eprimed=eprimed.loc[eprimed.exclude==0].copy()
eprimed.to_csv(box_temp+'/HCD_Eprime_'+snapshotdate+'.csv',index=False)
box.upload_file(box_temp+'/HCD_Eprime_'+snapshotdate+'.csv',Dsnaps)

##############################
penncnp=box.downloadFile(452784840845)
#penn=pd.read_csv(box_temp+'/'+penncnp.get().name,header=0,encoding = "ISO-8859-1")
penn=pd.read_csv(penncnp,header=0,encoding = "ISO-8859-1")

print(penn.shape)
penn=penn.loc[~(penn.p_unusable==1)]
penn=penn.loc[penn.CC.isnull()==True]
penn=penn.drop(columns=['age'])


penn=penn.loc[(penn.subid.isin(goodidsD+goodidsA))].copy()
penn['subject']=penn.subid
penn['redcap_event']=penn.assessment

print(penn.shape)
print(penn.columns)

penn.loc[penn.subid.str.contains('HCA')].to_csv(box_temp+'/HCA_PennCNP_'+snapshotdate+'.csv',index=False)
penn.loc[penn.subid.str.contains('HCD')].to_csv(box_temp+'/HCD_PennCNP_'+snapshotdate+'.csv',index=False)
box.upload_file(box_temp+'/HCA_PennCNP_'+snapshotdate+'.csv',Asnaps)
box.upload_file(box_temp+'/HCD_PennCNP_'+snapshotdate+'.csv',Dsnaps)

#box.download_files(mask_file)
########### Toolbox #################################
goodPINS=goodPINSA+goodPINSD

#Toolbox drop date fields 'DateFinished',
#visit is renamed redcap_event for consistency across datatypes in the function
scorecolumns=['subject','visit','PIN', 'DeviceID', 'Assessment Name', 'Inst',
       'RawScore', 'Theta', 'TScore', 'SE', 'ItmCnt',
       'Column1', 'Column2', 'Column3', 'Column4', 'Column5', 'Language',
       'Computed Score', 'Uncorrected Standard Score',
       'Age-Corrected Standard Score', 'National Percentile (age adjusted)',
       'Fully-Corrected T-score', 'Uncorrected Standard Scores Dominant',
       'Age-Corrected Standard Scores Dominant',
       'National Percentile (age adjusted) Dominant',
       'Fully-Corrected T-scores Dominant',
       'Uncorrected Standard Scores Non-Dominant',
       'Age-Corrected Standard Scores Non-Dominant',
       'National Percentile (age adjusted) Non-Dominant',
       'Fully-Corrected T-scores Non-Dominant', 'Dominant Score',
       'Non-Dominant Score', 'Raw Score Right Ear', 'Threshold Right Ear',
       'Raw Score Left Ear', 'Threshold Left Ear',
       'Static Visual Acuity logMAR', 'Static Visual Acuity Snellen',
       'InstrumentBreakoff', 'InstrumentStatus2', 'InstrumentRCReason',
       'InstrumentRCReasonOther', 'App Version', 'iPad Version',
       'Firmware Version']

#'DateCreated', 'InstStarted', 'InstEnded',
datacolumns=['subject','visit','PIN', 'DeviceID', 'Assessment Name',
       'InstOrdr', 'InstSctn', 'ItmOrdr', 'Inst', 'Locale', 'ItemID',
       'Response', 'Score', 'Theta', 'TScore', 'SE', 'DataType', 'Position',
       'ResponseTime',
       'App Version', 'iPad Version', 'Firmware Version']


#no restricted counterparts for NIHTOOLBOX...leaving code in incase want to deal
def tlbxtrans2(fileid,curatedsnaps,restrictsnaps,goodPINS,typed):
    fname=box.downloadFile(fileid)
    print(typed,fileid)
    #df=pd.read_csv(box_temp+'/'+fname.get().name,header=0)
    df = pd.read_csv(fname, header=0)
    print(len(df.PIN.unique()))
    print('Downloaded Shape',df.shape)
    df=df.loc[df.PIN.isin(goodPINS)].copy()
    print('Shape after exclusions',df.shape)
    print(len(df.PIN.unique()))
    #dfr=df.copy()
    if typed=='Scores':
        df=df[scorecolumns]
        #cols=scorecolumns + restrictedTLBXS
        #dfr=dfr[cols]
    if typed=='Raw':
        df=df[datacolumns]
    df=df.rename(columns={'visit':'redcap_event'})
        #colr = datacolumns + restrictedTLBXR
        #dfr=dfr[colr]
    #df.to_csv(box_temp+'/Filtered_'+snapshotdate+fname.get().name,index=False)
    #dfr.to_csv(box_temp+'/Restricted_Filtered_'+snapshotdate+fname.get().name,index=False)
    print('final shape',df.shape)
    #print('final restricted shape',dfr.shape)
    #box.upload_file(box_temp+'/Filtered_'+snapshotdate+fname.get().name,curatedsnaps)
    #box.upload_file(box_temp+'/Restricted_Filtered_'+snapshotdate+fname.get().name,restrictsnaps)
    return df #, dfr

#get curated fileids
exportpath="./"
curated=pd.read_csv(exportpath+"CuratedToolboxBoxFiles.csv")

HS=curated.loc[(curated.study=='HCD') & (curated.site=='MGH/Harvard') & (curated.type=='Scores')].reset_index().fileid[0]
HR=curated.loc[(curated.study=='HCD') & (curated.site=='MGH/Harvard') & (curated.type=='Raw')].reset_index().fileid[0]
MGS=curated.loc[(curated.study=='HCA') & (curated.site=='MGH/Harvard') & (curated.type=='Scores')].reset_index().fileid[0]
MGR=curated.loc[(curated.study=='HCA') & (curated.site=='MGH/Harvard') & (curated.type=='Raw')].reset_index().fileid[0]
WUAS=curated.loc[(curated.study=='HCA') & (curated.site=='WashU') & (curated.type=='Scores')].reset_index().fileid[0]
WUAR=curated.loc[(curated.study=='HCA') & (curated.site=='WashU') & (curated.type=='Raw')].reset_index().fileid[0]
WUDS=curated.loc[(curated.study=='HCD') & (curated.site=='WashU') & (curated.type=='Scores')].reset_index().fileid[0]
WUDR=curated.loc[(curated.study=='HCD') & (curated.site=='WashU') & (curated.type=='Raw')].reset_index().fileid[0]
UMAS=curated.loc[(curated.study=='HCA') & (curated.site=='UMinn') & (curated.type=='Scores')].reset_index().fileid[0]
UMAR=curated.loc[(curated.study=='HCA') & (curated.site=='UMinn') & (curated.type=='Raw')].reset_index().fileid[0]
UMDS=curated.loc[(curated.study=='HCD') & (curated.site=='UMinn') & (curated.type=='Scores')].reset_index().fileid[0]
UMDR=curated.loc[(curated.study=='HCD') & (curated.site=='UMinn') & (curated.type=='Raw')].reset_index().fileid[0]

UCAS=curated.loc[(curated.study=='HCA') & (curated.site=='UCLA') & (curated.type=='Scores')].reset_index().fileid[0]
UCAR=curated.loc[(curated.study=='HCA') & (curated.site=='UCLA') & (curated.type=='Raw')].reset_index().fileid[0]
UCDS=curated.loc[(curated.study=='HCD') & (curated.site=='UCLA') & (curated.type=='Scores')].reset_index().fileid[0]
UCDR=curated.loc[(curated.study=='HCD') & (curated.site=='UCLA') & (curated.type=='Raw')].reset_index().fileid[0]

#FROM INVENTORY:
#TLBXD IDS to mask (most are already masked by virtue of exclusions, but some parents of excluded or DNR children
#print("TLBX IDS that dont exist in REDCap: create trello card to investigate and possibly drop from curated")
#tlbxwierdos=pd.merge(HCAdf3,TLBXA,how='right',left_on=['subject'],right_on=['subject'],indicator='TLBX_wierdos')
#print(tlbxwierdos.loc[tlbxwierdos.TLBX_wierdos=='right_only'][['subject']])
#from site lists and sandy list;
#droplist=['HCD0661448','HCD0971261','HCD1027530','HCD1616953','HCD1703039','HCD1727558','HCD2113528','HCD2384761','HCD2557463','HCD3367766','HCD3563665']

Asnaps=126706803362
Dsnaps=126781658067
ArestrictSnaps=150224568988
DrestrictSnaps=150226955672
#fileid,curatedsnaps,restrictsnaps,goodPINS,typed
#All the HCD
hs=tlbxtrans2(HS,Dsnaps,DrestrictSnaps,goodPINS,'Scores') #HarvardScores
rawhs=tlbxtrans2(HR,Dsnaps,DrestrictSnaps,goodPINS,'Raw') #HarvardRaw

wus=tlbxtrans2(WUDS,Dsnaps,DrestrictSnaps,goodPINS,'Scores') #WashUD scores
rwus=tlbxtrans2(WUDR,Dsnaps,DrestrictSnaps,goodPINS,'Raw') #WashUD raw

ums=tlbxtrans2(UMDS,Dsnaps,DrestrictSnaps,goodPINS,'Scores') #umn D scores
rums=tlbxtrans2(UMDR,Dsnaps,DrestrictSnaps,goodPINS,'Raw') #umn D raw

ucs=tlbxtrans2(UCDS,Dsnaps,DrestrictSnaps,goodPINS,'Scores') #UCLA D Scored
rucs=tlbxtrans2(UCDR,Dsnaps,DrestrictSnaps,goodPINS,'Raw') #UCLA D raw

DS=pd.concat([hs,wus,ums,ucs])
for i in [hs,wus,ums,ucs]:
    print(i.shape)
RawDS=pd.concat([rawhs,rwus,rums,rucs])
for i in [rawhs,rwus,rums,rucs]:
    print(i.shape)

print(DS.shape)
print(RawDS.shape)


DS.to_csv(box_temp+'/HCD_NIH-Toolbox-Scores_'+snapshotdate+'.csv',index=False)
RawDS.to_csv(box_temp+'/HCD_NIH-Toolbox-Raw_'+snapshotdate+'.csv',index=False)
box.upload_file(box_temp+'/HCD_NIH-Toolbox-Scores_'+snapshotdate+'.csv',Dsnaps)
box.upload_file(box_temp+'/HCD_NIH-Toolbox-Raw_'+snapshotdate+'.csv',Dsnaps)



amgs=tlbxtrans2(MGS,Asnaps,ArestrictSnaps,goodPINS,'Scores') #MGHScores
amgr=tlbxtrans2(MGR,Asnaps,ArestrictSnaps,goodPINS,'Raw') #MGHRaw

awuas=tlbxtrans2(WUAS,Asnaps,ArestrictSnaps,goodPINS,'Scores') #WashUA scores
awuar=tlbxtrans2(WUAR,Asnaps,ArestrictSnaps,goodPINS,'Raw') #WashUA raw

aumns=tlbxtrans2(UMAS,Asnaps,ArestrictSnaps,goodPINS,'Scores') #umn A scores
aumnr=tlbxtrans2(UMAR,Asnaps,ArestrictSnaps,goodPINS,'Raw') #umn A raw

aucs=tlbxtrans2(UCAS,Asnaps,ArestrictSnaps,goodPINS,'Scores') #UCLA A scored
aucr=tlbxtrans2(UCAR,Asnaps,ArestrictSnaps,goodPINS,'Raw') #UCLA A raw


AS=pd.concat([amgs,awuas,aumns,aucs])
for i in [amgs,awuas,aumns,aucs]:
    print(i.shape)
RawAS=pd.concat([amgr,awuar,aumnr,aucr])
for i in [amgr,awuar,aumnr,aucr]:
    print(i.shape)

print(AS.shape)
print(RawAS.shape)
AS.to_csv(box_temp+'/HCA_NIH_Toolbox_Scores_'+snapshotdate+'.csv',index=False)
RawAS.to_csv(box_temp+'/HCA_NIH_Toolbox_Raw_'+snapshotdate+'.csv',index=False)
box.upload_file(box_temp+'/HCA_NIH_Toolbox_Scores_'+snapshotdate+'.csv',Asnaps)
box.upload_file(box_temp+'/HCA_NIH_Toolbox_Raw_'+snapshotdate+'.csv',Asnaps)

#inventory has been independently checked to make sure that no exclusions/withdrawn subjects squeaked in


