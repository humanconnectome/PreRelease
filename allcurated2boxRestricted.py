import os, datetime
import csv
import pycurl
import sys
import shutil
from openpyxl import load_workbook
import pandas as pd
import download.box
from io import BytesIO
import numpy as np

from download.box import LifespanBox


# This code walks through each of the external datatypes (except pedigrees), subsets to subjects/events in inventory
# (see Curated Inventory.ipynb in https://github.com/humanconnectome/PreRelease)
# and filters out variables with PHI
# PG-13 unfiltered data types (i.e. with dates, ages>90 and freetext that might mention 'John')
# can be found on the 'restricted' paths.
# G-rated (our best effort at scrubbing 26,605 varialbes) data and annotation can be found in Asnaps and Dsnps


verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
box_temp='/home/petra/UbWinSharedSpace1/boxtemp' #location of local copy of curated data
box = LifespanBox(cache=box_temp)
redcapconfigfile="/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/.boxApp/redcapconfig.csv"
redcap9configfile="/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/.boxApp/redcap9config.csv"

Asnaps=126706803362
Dsnaps=126781658067
ArestrictSnaps=150224568988
DrestrictSnaps=150226955672

#REORGANIZED AROUND INVENTORY:
#this is the latest inventory

inventorypath='/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ccfQC/ccf-behavioral/src/'
inventoryA=pd.read_csv(inventorypath+'HCA_AllSources_'+snapshotdate+'.csv')
inventoryD=pd.read_csv(inventorypath+'HCD_AllSources_'+snapshotdate+'.csv')
goodidsD=list(inventoryD.subject.unique())
goodidsA=list(inventoryA.subject.unique())

box.upload_file(inventorypath+'HCA_AllSourcesSlim_'+snapshotdate+'.csv', Asnaps)
box.upload_file(inventorypath+'HCA_AllSources_'+snapshotdate+'.csv', ArestrictSnaps)
box.upload_file(inventorypath+'HCD_AllSourcesSlim_'+snapshotdate+'.csv', Dsnaps)
box.upload_file(inventorypath+'HCD_AllSources_'+snapshotdate+'.csv', DrestrictSnaps)



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
restrictA=pd.read_excel("/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ccfQC/ccf-behavioral/src/HCP REDCap Recommended Restricted17Nov2021.xlsx", sheet_name='HCA')
restrictedA=list(restrictA.field_name)
restrictCh=pd.read_excel("/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ccfQC/ccf-behavioral/src/HCP REDCap Recommended Restricted17Nov2021.xlsx", sheet_name='HCP-D Child')
restrictedCh=list(restrictCh.field_name)
restrict18=pd.read_excel("/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ccfQC/ccf-behavioral/src/HCP REDCap Recommended Restricted17Nov2021.xlsx", sheet_name='HCD 18+')
restricted18=list(restrict18.field_name)
restrictParent=pd.read_excel("/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ccfQC/ccf-behavioral/src/HCP REDCap Recommended Restricted17Nov2021.xlsx", sheet_name='HCD Parent')
restrictedParent=list(restrictParent.field_name)
restrictQ=pd.read_excel("/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ccfQC/ccf-behavioral/src/HCP REDCap Recommended Restricted17Nov2021.xlsx", sheet_name='Q')
restrictedQ=list(restrictQ.field_name)
restrictK=pd.read_excel("/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ccfQC/ccf-behavioral/src/HCP REDCap Recommended Restricted17Nov2021.xlsx", sheet_name='ksads')
restrictedK=list(restrictK.field_name)
restrictS=pd.read_excel("/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ccfQC/ccf-behavioral/src/HCP REDCap Recommended Restricted17Nov2021.xlsx", sheet_name='SSAGA')
restrictedS=list(restrictS.field_name)

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
    dfrestricted=df.copy() #[[idvar, 'subject', 'redcap_event_name']+restrictedcols] #send full set so not need merge
    for dropcol in restrictedcols:
        try:
            df=df.drop(columns=dropcol)
        except:
            pass
    print(df.shape)
    return flaggedids, df, dfrestricted


def getredcap10Q(studystr,curatedsnaps,goodies,idstring,restrictedcols=[]):
    """
    downloads all events and fields in a redcap database
    """
    studydata = pd.DataFrame()
    auth = pd.read_csv(redcap9configfile)
    token=auth.loc[auth.study==studystr,'token'].reset_index().token[0]
    subj=auth.loc[auth.study==studystr,'field'].reset_index().field[0]
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
    df.to_csv(box_temp+'/REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv',index=False)
    dfrestricted.to_csv(box_temp+'/Restricted_REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv',index=False)
    return df, dfrestricted



############### KSADS ###############
kD,kDrestricted=getredcap10Q('ksads',Dsnaps,goodidsD,'HCD',restrictedcols=restrictedK)
idstring='HCD'
studystr='ksads'
box.upload_file(box_temp+'/REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv', Dsnaps)
box.upload_file(box_temp+'/Restricted_REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv', DrestrictSnaps)
#2828 KSADS P,T, and P mood, Tmood batteries in inventory as of 11/19/2021

############### Qinteractive ###############
qA,qArestricted=getredcap10Q('qint',Asnaps,goodidsA,'HCA',restrictedcols=restrictedQ)
idstring='HCA'
studystr='qint'
box.upload_file(box_temp+'/REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv', Asnaps)
box.upload_file(box_temp+'/Restricted_REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv', ArestrictSnaps)


qD,qDrestricted=getredcap10Q('qint',Dsnaps,goodidsD,'HCD',restrictedcols=restrictedQ)
idstring='HCD'
studystr='qint'
box.upload_file(box_temp+'/REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv', Dsnaps)
box.upload_file(box_temp+'/Restricted_REDCap_'+idstring+studystr+'_'+snapshotdate+'.csv', DrestrictSnaps)

#Note: as of 11/18 there were 5 extra HCA CR records in data but not inventory due to missing CR survey (and event date)
#this is just QC to confirm 5 extra records in HCA and none in HCD
qDrestricted['redcap_event']='V'+qDrestricted.visit
qArestricted['redcap_event']='V'+qArestricted.visit
qArestricted['redcap_event']=qArestricted.redcap_event.str.replace('VCR','CR')

qArestricted.redcap_event=qArestricted.redcap_event.str.strip()
qDrestricted.redcap_event=qDrestricted.redcap_event.str.strip()

test=pd.merge(inventoryA[['subject','redcap_event']],qArestricted,left_on=['subject','redcap_event'],right_on=['subjectid','redcap_event'],how='outer',indicator=True)
test.loc[test._merge=='right_only' ][['subject','visit','id','redcap_event']]
#qint.shape
test2=pd.merge(inventoryD[['subject','redcap_event']],qDrestricted,left_on=['subject','redcap_event'],right_on=['subjectid','redcap_event'],how='outer',indicator=True)
test2.loc[test2._merge=='right_only' ][['subject','visit','id','redcap_event']]
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

restrictedsnaps=ArestrictSnaps
studystr='hcpa'
inventdf.to_csv(box_temp+'/REDCap_'+studystr+'_'+snapshotdate+'.csv',index=False)
inventdfrestricted.to_csv(box_temp+'/Restricted_REDCap_'+studystr+'_'+snapshotdate+'.csv',index=False)
box.upload_file(box_temp+'/REDCap_'+studystr+'_'+snapshotdate+'.csv', Asnaps)
box.upload_file(box_temp+'/Restricted_REDCap_'+studystr+'_'+snapshotdate+'.csv', ArestrictSnaps)

############################################################
flaggedssaga, dfss, dfssres=getredcap7('ssaga',Asnaps,ArestrictSnaps,flaggedgold=pd.DataFrame(),restrictedcols=restrictedS)

link=dfss.loc[dfss.hcpa_id.isnull()==False][['hcpa_id','study_id']]
link=link.loc[~(link.hcpa_id=="")]
print(inventoryA.shape)
test=inventoryA.loc[~(inventoryA.sub_event=='7.Covid1')] #remove source of dups irrelevant to ssaga (it was relevent to HCPA records so couldnt do this there)
print(test.shape)
test=test.drop_duplicates(subset=['subject','REDCap_id','redcap_event_name'])
print(test.shape)
test=test.loc[test.Curated_SSAGA.isin(['YES','YES BUT'])]#1789 on 11/19/21
test=pd.merge(test,link,left_on='subject',right_on='hcpa_id',how='left')#[['study_id','redcap_event_name']]
print(test.shape) #these are the subevent=covid1 people with SSAGA and visit data
test2=test[['study_id','redcap_event_name']]

#add four who aren't in inventory because have ssaga but no v2 - the rest of the discrepancies (1831 vs 1793) are empty ssagas or withdrawns
extrasubjects=dfss.loc[(dfss.study_id.isin(["9", "12", "9532-280", "9533-257"])) & (dfss.redcap_event_name=='visit_2_arm_1')][['study_id','redcap_event_name']]
test3=pd.concat([test2,extrasubjects])
print(test3.shape)

inventss=pd.merge(test3,dfss, left_on=['study_id','redcap_event_name'],right_on=['study_id','redcap_event_name'],how='left')
inventss=inventss.drop(columns='study_id')
print(dfss.shape) #if not 1831 - see note above and find the change
print(inventss.shape)

inventssres=pd.merge(test3,dfssres, left_on=['study_id','redcap_event_name'],right_on=['study_id','redcap_event_name'],how='left')
inventssres=inventssres.drop(columns='study_id')
print(dfssres.shape)
print(inventssres.shape)

curatedsnaps=Asnaps
restrictedsnaps=ArestrictSnaps
studystr='ssaga'

inventss.to_csv(box_temp+'/REDCap_'+studystr+'_'+snapshotdate+'.csv',index=False)
inventssres.to_csv(box_temp+'/Restricted_REDCap_'+studystr+'_'+snapshotdate+'.csv',index=False)
box.upload_file(box_temp+'/REDCap_'+studystr+'_'+snapshotdate+'.csv', Asnaps)
box.upload_file(box_temp+'/Restricted_REDCap_'+studystr+'_'+snapshotdate+'.csv', ArestrictSnaps)




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


studystr='hcpdchild'

inventdfc.to_csv(box_temp+'/REDCap_'+studystr+'_'+snapshotdate+'.csv',index=False)
inventdfcr.to_csv(box_temp+'/Restricted_REDCap_'+studystr+'_'+snapshotdate+'.csv',index=False)
box.upload_file(box_temp+'/REDCap_'+studystr+'_'+snapshotdate+'.csv', Dsnaps)
box.upload_file(box_temp+'/Restricted_REDCap_'+studystr+'_'+snapshotdate+'.csv', DrestrictSnaps)

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

studystr='hcpd18'

inventd18.to_csv(box_temp+'/REDCap_'+studystr+'_'+snapshotdate+'.csv',index=False)
inventdfcr18.to_csv(box_temp+'/Restricted_REDCap_'+studystr+'_'+snapshotdate+'.csv',index=False)

box.upload_file(box_temp+'/REDCap_'+studystr+'_'+snapshotdate+'.csv', Dsnaps)
box.upload_file(box_temp+'/Restricted_REDCap_'+studystr+'_'+snapshotdate+'.csv', DrestrictSnaps)


######### HCD parents ###################################################################################
flaggedparent, dfparent, dfparentrest=getredcap7('hcpdparent',Dsnaps,DrestrictSnaps,flaggedgold=pd.DataFrame(),restrictedcols=restrictedParent)
#need to not restrict the two-parent cases

parentD=inventoryD.loc[~(inventoryD.DB_Source.isin(['teen','child_only']))][['REDCap_id_parent','redcap_event_name']]
inventp=pd.merge(parentD[['REDCap_id_parent','redcap_event_name']],dfparent, left_on=['REDCap_id_parent','redcap_event_name'],right_on=['id','redcap_event_name'],how='left')
inventp=inventp.drop(columns='REDCap_id_parent')
print(inventp.shape)
#listgoodies=list(inventp.id.unique()) #these redcap ids are of parents of legit children

#need a few extra ids for the specialty cases
#dfparent.loc[dfparent.parent_id=='HCD3062037'][['child_id','parent_id','id']]
#dfparent.loc[dfparent.parent_id=='HCD4351251'][['child_id','parent_id','id']]
#dfparent.loc[dfparent.parent_id=='HCD5555474'][['child_id','parent_id','id']]
extraids=['6105-302','6106-255','6106-159'] #one has two events
extraparents=dfparent.loc[dfparent.id.isin(extraids)]

#put them together
parents=pd.concat([inventp,extraparents])
print(parents.shape)


#restricted
inventpr=pd.merge(parentD[['REDCap_id_parent','redcap_event_name']],dfparentrest, left_on=['REDCap_id_parent','redcap_event_name'],right_on=['id','redcap_event_name'],how='left')
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
studystr='hcpdparent'

parents.to_csv(box_temp+'/REDCap_'+studystr+'_'+snapshotdate+'.csv',index=False)
parentsr.to_csv(box_temp+'/Restricted_REDCap_'+studystr+'_'+snapshotdate+'.csv',index=False)
box.upload_file(box_temp+'/REDCap_'+studystr+'_'+snapshotdate+'.csv', Dsnaps)
box.upload_file(box_temp+'/Restricted_REDCap_'+studystr+'_'+snapshotdate+'.csv', DrestrictSnaps)


##############################


eprime=box.download_file(495490047901)
eprimed=pd.read_csv(box_temp+'/'+eprime.get().name,header=0)
eprimed=eprimed.loc[(eprimed.subject.isin(goodidsD))].copy()
eprimed=eprimed.loc[eprimed.exclude==0].copy()
eprimed.to_csv(box_temp+'/Eprime_DelayDiscounting_'+snapshotdate+'.csv',index=False)
box.upload_file(box_temp+'/Eprime_DelayDiscounting_'+snapshotdate+'.csv',Dsnaps)

##############################


penncnp=box.download_file(452784840845)
penn=pd.read_csv(box_temp+'/'+penncnp.get().name,header=0,encoding = "ISO-8859-1")
print(penn.shape)
penn=penn.loc[~(penn.p_unusable==1)]
penn=penn.loc[penn.CC.isnull()==True]
penn=penn.drop(columns=['age'])


penn=penn.loc[(penn.subid.isin(goodidsD+goodidsA))].copy()
print(penn.shape)

penn.loc[penn.subid.str.contains('HCA')].to_csv(box_temp+'/HCAonly_AllSites_PENNCNP_'+snapshotdate+'.csv',index=False)
penn.loc[penn.subid.str.contains('HCD')].to_csv(box_temp+'/HCDonly_AllSites_PENNCNP_'+snapshotdate+'.csv',index=False)
box.upload_file(box_temp+'/HCAonly_AllSites_PENNCNP_'+snapshotdate+'.csv',Asnaps)
box.upload_file(box_temp+'/HCDonly_AllSites_PENNCNP_'+snapshotdate+'.csv',Dsnaps)


########### Toolbox #################################
goodPINS=goodPINSA+goodPINSD

#Toolbox drop date fields 'DateFinished',
scorecolumns=['PIN', 'DeviceID', 'Assessment Name', 'Inst',
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
       'Firmware Version','subject','visit']

#'DateCreated', 'InstStarted', 'InstEnded',
datacolumns=['PIN', 'DeviceID', 'Assessment Name',
       'InstOrdr', 'InstSctn', 'ItmOrdr', 'Inst', 'Locale', 'ItemID',
       'Response', 'Score', 'Theta', 'TScore', 'SE', 'DataType', 'Position',
       'ResponseTime',
       'App Version', 'iPad Version', 'Firmware Version','subject','visit']



def tlbxtrans2(fileid,curatedsnaps,restrictsnaps,goodPINS,typed):
    fname=box.download_file(fileid)
    df=pd.read_csv(box_temp+'/'+fname.get().name,header=0)
    print(len(df.PIN.unique()))
    print(df.shape)
    df=df.loc[df.PIN.isin(goodPINS)].copy()
    print(df.shape)
    print(len(df.PIN.unique()))
    dfr=df.copy()
    if typed=='Scores':
        df=df[scorecolumns]
    if typed=='Raw':
        df=df[datacolumns]
    df.to_csv(box_temp+'/Filtered_'+snapshotdate+fname.get().name,index=False)
    dfr.to_csv(box_temp+'/Restricted_Filtered_'+snapshotdate+fname.get().name,index=False)
    print(df.shape)
    print(dfr.shape)
    box.upload_file(box_temp+'/Filtered_'+snapshotdate+fname.get().name,curatedsnaps)
    box.upload_file(box_temp+'/Restricted_Filtered_'+snapshotdate+fname.get().name,restrictsnaps)



#get curated fileids
exportpath="/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ccfQC/ccf-behavioral/src/"
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
tlbxtrans2(HS,Dsnaps,DrestrictSnaps,goodPINS,'Scores') #HarvardScores
tlbxtrans2(HR,Dsnaps,DrestrictSnaps,goodPINS,'Raw') #HarvardRaw

tlbxtrans2(WUDS,Dsnaps,DrestrictSnaps,goodPINS,'Scores') #WashUD scores
tlbxtrans2(WUDR,Dsnaps,DrestrictSnaps,goodPINS,'Raw') #WashUD raw

tlbxtrans2(UMDS,Dsnaps,DrestrictSnaps,goodPINS,'Scores') #umn D scores
tlbxtrans2(UMDR,Dsnaps,DrestrictSnaps,goodPINS,'Raw') #umn D raw

tlbxtrans2(UCDS,Dsnaps,DrestrictSnaps,goodPINS,'Scores') #UCLA D Scored
tlbxtrans2(UCDR,Dsnaps,DrestrictSnaps,goodPINS,'Raw') #UCLA D raw

tlbxtrans2(MGS,Asnaps,ArestrictSnaps,goodPINS,'Scores') #MGHScores
tlbxtrans2(MGR,Asnaps,ArestrictSnaps,goodPINS,'Raw') #MGHRaw

tlbxtrans2(WUAS,Asnaps,ArestrictSnaps,goodPINS,'Scores') #WashUA scores
tlbxtrans2(WUAR,Asnaps,ArestrictSnaps,goodPINS,'Raw') #WashUA raw

tlbxtrans2(UMAS,Asnaps,ArestrictSnaps,goodPINS,'Scores') #umn A scores
tlbxtrans2(UMAR,Asnaps,ArestrictSnaps,goodPINS,'Raw') #umn A raw

tlbxtrans2(UCAS,Asnaps,ArestrictSnaps,goodPINS,'Scores') #UCLA A scored
tlbxtrans2(UCAR,Asnaps,ArestrictSnaps,goodPINS,'Raw') #UCLA A raw

#inventory has been independently checked to make sure that no exclusions/withdrawn subjects squeaked in






 #  box.upload_file(box_temp+'/'+fname+'_data_'+snapshotdate+'.csv',fnumber)


