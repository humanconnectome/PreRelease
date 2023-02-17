import pandas as pd
import numpy as np
import sys
import datetime

snapshotdate = datetime.datetime.today().strftime('%Y-%m-%d')


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 10000)
pd.set_option('display.max_colwidth',500)

prepath="/home/petra/Behavioral/Lifespan/PreRelease/"
sandyA="HCA Excluded Participants_2021May05 (1).xlsx"
sandyD="HCD Excluded Participants_2021Mar01 (1).xlsx"
IRB="IRB exclusions"
DNR="Data release exclusions"
Complain="Subject Complaints"

petra="Lifespan_REDCap_Based_Exclusions_From_PreRelease_NO_DUPS.csv"
inventorypath='/home/petra/Behavioral/Lifespan/PreRelease/PreRelease/'
#versionold='11_22_2022'
versionold='02_17_2023'

erinA='HCA-exclusions-EkR.xlsx'
sheetA='Sheet2'
erinD='HCD-T1+age.xlsx'
SheetD='Excluded'

ErinA=pd.read_excel(prepath+erinA, sheet_name=sheetA)#[['HCA ID','Reason for Exclusion','Additional Information ']]
ErinA=ErinA.rename(columns={'EkR excluded list from scan QC':'subject','Sandy_repeatinfo':'Erin_exclusion'})[['subject','Erin_exclusion']]
ErinD=pd.read_excel(prepath+erinD, sheet_name=SheetD)
ErinD['subject']=ErinD.Session.str.split('_',expand=True)[0]
ErinD=ErinD.drop_duplicates(subset='subject',keep='first')[['subject','Exclusion reason']].copy()
ErinD=ErinD.rename(columns={'Exclusion reason':'Erin_exclusion'})

Erin=pd.concat([ErinA,ErinD],axis=0)#
Erin['redcap_event']='V1'
Erin['ErinUnusable']=1

IRBA=pd.read_excel(prepath+sandyA, sheet_name=IRB, header=4)[['HCA ID','Reason for Exclusion','Additional Information ','Amt. of Data Collected','Data']]
IRBA=IRBA.rename(columns={"HCA ID":'subject','Reason for Exclusion':'Sandy_IRB_exclusion','Additional Information ':'Sandy_IRB_moreinfo','Amt. of Data Collected':'AmtI'})
DNRA=pd.read_excel(prepath+sandyA, sheet_name=DNR, header=3)[['HCA ID','Reason for Exclusion','Additional Information ','Amt. of Data Collected']]
DNRA=DNRA.rename(columns={"HCA ID":'subject','Reason for Exclusion':'Sandy_DNR_exclusion','Additional Information ':'Sandy_DNR_moreinfo','Amt. of Data Collected':'AmtDNR'})

SandyA=pd.merge(IRBA,DNRA,on='subject',how='outer')
SandyA=SandyA.drop_duplicates(subset='subject')

IRBD=pd.read_excel(prepath+sandyD, sheet_name=IRB, header=4)[['HCD ID','Reason for Exclusion','Additional Information ','Amt. of Data Collected','Data ']]
IRBD=IRBD.rename(columns={"HCD ID":'subject','Reason for Exclusion':'Sandy_IRB_exclusion','Additional Information ':'Sandy_IRB_moreinfo','Data ':'Data','Amt. of Data Collected':'AmtI'})
DNRD=pd.read_excel(prepath+sandyD, sheet_name=DNR, header=3)[['HCD ID','Reason for Exclusion','Additional Information ','Amt. of Data Collected']]
DNRD=DNRD.rename(columns={"HCD ID":'subject','Reason for Exclusion':'Sandy_DNR_exclusion','Additional Information ':'Sandy_DNR_moreinfo','Amt. of Data Collected':'AmtDNR'})

SandyD=pd.merge(IRBD,DNRD,on='subject',how='outer')
SandyD=SandyD.drop_duplicates(subset='subject')

Sandy=pd.concat([SandyA,SandyD],axis=0,sort=True)
Sandy['SandyUnusable']=1
Sandy['redcap_event']='V1'
Sandy.loc[Sandy.subject.str.contains('V2'),'redcap_event']='V2'
Sandy.loc[Sandy.subject=='HCA9461182 (V2)','subject']='HCA9461182'
Sandy['SandyExclusion']=Sandy.Sandy_IRB_exclusion.astype(str) +'...'+ Sandy.Sandy_IRB_moreinfo.astype(str) +'...'+ Sandy.Sandy_DNR_exclusion.astype(str) +'...'+Sandy.Sandy_DNR_moreinfo.astype(str)
Sandy['SandyWhatData']=Sandy.AmtI.astype(str)+'...'+ Sandy.AmtDNR.astype(str)

petralist=pd.read_csv(prepath+petra)
petralist['subject']=petralist['subject id'].str.split('_',expand=True)[0]
petralist.loc[petralist['subject id'].str.upper().str.contains('CC'),'subject']=petralist['subject id']
petralist['redcap_event']='V1'
petralist.loc[petralist.subject.isin(['HCD0664656_CC','HCD1106728_CC','HCD0541034_CC']),'redcap_event']='V3'
#add two back because gonna be released (never came back after covid, and had enough usable data)
petralist=petralist.loc[~(petralist.subject.isin(['HCD0123824_CC','HCD2059043_CC']))].copy()
#this guy is redundant
petralist.loc[petralist.subject.str.contains('HCD2442446')]
petralist=petralist.loc[~(petralist.subject.isin(['HCD2442446_CC']))].copy()

petralist['PetraUnusable']=1

inventoryA=pd.read_csv(inventorypath+'HCA_AllSources_' + versionold + '.csv')[['subject','IntraDB','DB_Source','redcap_event','event_age']]
inventoryA=inventoryA.rename(columns={'IntraDB':'Petra_IntraDB_STG','DB_Source':'RedcapDB','event_age':'redcap_age'})
inventoryD=pd.read_csv(inventorypath+'HCD_AllSources_' + versionold + '.csv')[['subject','IntraDB','DB_Source','redcap_event','event_age']]
inventoryD=inventoryD.rename(columns={'IntraDB':'Petra_IntraDB_STG','DB_Source':'RedcapDB','event_age':'redcap_age'})
Petra=pd.concat([inventoryA,inventoryD],axis=0)
Petra=Petra.loc[Petra.redcap_event.isin(['V1','V2','V3'])]
Petra['PreReleaseInventory']=1
Petra=Petra.drop_duplicates(subset=['redcap_event','subject']).copy()
petralist=petralist.drop_duplicates(subset=['redcap_event','subject']).copy()
petralist=petralist.rename(columns={'subject id':'Petra_exclusion'})

Petra2=pd.merge(Petra, petralist, on=['subject','redcap_event'], how='outer')
PetraSandy=pd.merge(Petra2,Sandy,on=['subject','redcap_event'],how='outer')
PetraSandyErin=pd.merge(PetraSandy,Erin,on=['subject','redcap_event'],how='outer')
PetraSandyErin.loc[PetraSandyErin.redcap_age.isnull()==True,'redcap_age']=PetraSandyErin.age
PetraSandyErin.loc[PetraSandyErin.PetraUnusable.isnull()==True,'PetraUnusable']=0
PetraSandyErin.loc[PetraSandyErin.SandyUnusable.isnull()==True,'SandyUnusable']=0
PetraSandyErin.loc[PetraSandyErin.ErinUnusable.isnull()==True,'ErinUnusable']=0

PetraSandyErin['AnyUnusable']=PetraSandyErin.ErinUnusable + PetraSandyErin.SandyUnusable + PetraSandyErin.PetraUnusable
PetraSandyErin.redcap_age=PetraSandyErin.redcap_age.round(1)
PetraSandyErin.loc[PetraSandyErin.PetraUnusable==0,'PetraUnusable']=np.nan
PetraSandyErin.loc[PetraSandyErin.SandyUnusable==0,'SandyUnusable']=np.nan
PetraSandyErin.loc[PetraSandyErin.ErinUnusable==0,'ErinUnusable']=np.nan
PetraSandyErin.loc[PetraSandyErin.AnyUnusable==0,'AnyUnusable']=np.nan
PetraSandyErin['SuggestRelease3.0']=np.nan
PetraSandyErin.loc[PetraSandyErin.Petra_IntraDB_STG.str.contains('STG')==True,'SuggestRelease3.0']=1
PetraSandyErin.loc[PetraSandyErin.Petra_IntraDB_STG.str.contains('Behavioral Only')==True,'SuggestRelease3.0']=1
PetraSandyErin=PetraSandyErin.rename(columns={'Data':'SandyWhatHappened2Data'})

#COVID SPECIAL STUFF
data={
    0: {'subject':'HCD0099752','redcap_event': 'V1','Covid_Comeback':'X1 X2 B After','ITK_URL':'https://intradb.humanconnectome.org/app/action/DisplayItemAction/search_element/xnat%3AsubjectData/search_field/xnat%3AsubjectData.ID/search_value/HCPIntradb11_S00656/project/CCF_HCD_ITK'},
    1: {'subject':'HCD2604749','redcap_event': 'V1','Covid_Comeback':'X1 B After','ITK_URL':'https://intradb.humanconnectome.org/app/action/DisplayItemAction/search_element/xnat%3AsubjectData/search_field/xnat%3AsubjectData.ID/search_value/HCPIntradb11_S00634/popup/false/project/CCF_HCD_ITK'},
    2: {'subject':'HCD0123824','redcap_event': 'V1','Covid_Comeback':'A Before','ITK_URL':'https://intradb.humanconnectome.org/app/action/DisplayItemAction/search_element/xnat%3AsubjectData/search_field/xnat%3AsubjectData.ID/search_value/HCPIntradb11_S00690/popup/false/project/CCF_HCD_ITK'},
    3: {'subject':'HCD2059043','redcap_event': 'V1','Covid_Comeback':'A Before','ITK_URL':'https://intradb.humanconnectome.org/app/action/DisplayItemAction/search_element/xnat%3AsubjectData/search_field/xnat%3AsubjectData.ID/search_value/HCPIntradb11_S00734/popup/false/project/CCF_HCD_ITK'},
    4: {'subject':'HCD2648365','redcap_event': 'V1','Covid_Comeback':'X1 B After','ITK_URL':'https://intradb.humanconnectome.org/app/action/DisplayItemAction/search_element/xnat%3AsubjectData/search_field/xnat%3AsubjectData.ID/search_value/HCPIntradb11_S00686/popup/false/project/CCF_HCD_ITK'},
    5: {'subject':'HCD0664656','redcap_event': 'V3','Covid_Comeback':'X1 B After','ITK_URL':'https://intradb.humanconnectome.org/app/action/DisplayItemAction/search_element/xnat%3AsubjectData/search_field/xnat%3AsubjectData.ID/search_value/HCPIntradb_S05144/popup/false/project/CCF_HCD_ITK'},
    6: {'subject':'HCD1106728','redcap_event': 'V3','Covid_Comeback':'X1 B After','ITK_URL':'https://intradb.humanconnectome.org/app/action/DisplayItemAction/search_element/xnat%3AsubjectData/search_field/xnat%3AsubjectData.ID/search_value/HCPIntradb_S05793/popup/false/project/CCF_HCD_ITK'},
    7: {'subject':'HCD0541034','redcap_event': 'V3','Covid_Comeback':'X1 B After','ITK_URL':'https://intradb.humanconnectome.org/app/action/DisplayItemAction/search_element/xnat%3AsubjectData/search_field/xnat%3AsubjectData.ID/search_value/HCPIntradb_S06582/popup/false/project/CCF_HCD_ITK'},
    8: {'subject':'HCD2442446','redcap_event': 'V1','Covid_Comeback':'A Before, but Excluded'},
    9: {'subject':'HCA7626380','redcap_event': 'V1','Covid_Comeback':'X1 B After','ITK_URL':'https://intradb.humanconnectome.org/app/action/DisplayItemAction/search_element/xnat%3AsubjectData/search_field/xnat%3AsubjectData.ID/search_value/HCPIntradb11_S00656/project/CCF_HCD_ITK'}
}
lis = []

for key,val in data.items():
    lis.append(val)
Unmergable2 = pd.DataFrame(lis)
PetraSandyErin=PetraSandyErin.merge(Unmergable2,on=['subject','redcap_event'],how='left')

#guy with cysts - don't exclude afterall
##this is the guy that should be excluded.  was dnr at V1.  probably got a v2 by accident.
##PetraSandyErin.loc[PetraSandyErin.subject=='HCA7297488','Petra_IntraDB_STG'] = 'CCF_HCA_STG'
#Unmergable1=pd.DataFrame([{'subject':'HCA7297488','redcap_event':'V2','Petra_IntraDB_STG':'CCF_HCA_STG','AnyUnusable':1,'PetraUnusable':'1','Petra_exclusion':'HCA7297488_DO NOT RELEASE'}])
#PetraSandyErin=pd.concat([PetraSandyErin,Unmergable1],axis=0)
#PetraSandyErin.loc[PetraSandyErin.subject=='HCA7297488','ITK_URL'] = 'https://intradb.humanconnectome.org/app/action/DisplayItemAction/search_element/xnat%3AsubjectData/search_field/xnat%3AsubjectData.ID/search_value/HCPIntradb_S04139/popup/false/project/CCF_HCA_ITK'

PetraSandyErin['Study']=''
PetraSandyErin.loc[PetraSandyErin.subject.str.contains('HCA'),'Study']='HCA'
PetraSandyErin.loc[PetraSandyErin.subject.str.contains('HCD'),'Study']='HCD'
PetraSandyErin.loc[PetraSandyErin.Study=='','Study']='PCMP'

PetraSandyErin[['subject','redcap_event','SuggestRelease3.0', 'Petra_IntraDB_STG', 'PreReleaseInventory', 'AnyUnusable','Covid_Comeback',
                'Erin_exclusion','SandyExclusion','Petra_exclusion','SandyUnusable','SandyWhatData','SandyWhatHappened2Data','ErinUnusable','PetraUnusable',
        'redcap_age','ITK_URL']].to_csv(prepath+'Lifespan_Universe_Inventory_Unusables_'+snapshotdate+'.csv',index=False)


# Saving the reference of the standard output
original_stdout = sys.stdout
with open('N_Breakdown_Lifespan_Universe_'+snapshotdate+'.txt','w') as f:
    sys.stdout = f
    print('**********************************************')
    print('Lifespan Universe')
    print(PetraSandyErin.Study.value_counts(dropna=False))

    print('**********************************************')
    print('Lifespan Usables.  1=one of Sandy/Petra/Erin flagged for unusable.  3=all agree')
    print(PetraSandyErin.AnyUnusable.value_counts(dropna=False))

    print('**********************************************')
    print('SANITY CHECK')
    print(pd.crosstab(PetraSandyErin.Petra_IntraDB_STG,PetraSandyErin.Study))
    print(pd.crosstab(PetraSandyErin.Petra_IntraDB_STG,PetraSandyErin.Study))

    print('**********************************************')
    print('1st Breakdown: Exclude all subjects in Erin/Petra/Sandy Lists , exclude PCMP')
    print('and EXCLUDE subjects with Behavioral Data Only:')
    #for i in ['HCA','HCD']:
    print('******************************************')
    #print(i,':')
    #subset=PetraSandyErin.loc[(PetraSandyErin.Study==i) & (~(PetraSandyErin.Petra_IntraDB_STG == "Behavioral Only"))]
    subset = PetraSandyErin.loc[(PetraSandyErin['SuggestRelease3.0'] == 1) & (~(PetraSandyErin.Petra_IntraDB_STG == "Behavioral Only"))]
    #print(pd.crosstab(subset['SuggestRelease3.0'], subset.redcap_event))#.to_csv('.csv',index=True)
    print(pd.crosstab(subset['Study'], subset.redcap_event))  # .to_csv('.csv',index=True)

    print('**********************************************')
    print('2nd Breakdown (suggested release): Exclude all subjects in Erin/Petra/Sandy Lists , exclude PCMP')
    print('and INCLUDE subjects with Behavioral Data Only:')
    subset2=PetraSandyErin.loc[(PetraSandyErin['SuggestRelease3.0']==1) ]
    print(pd.crosstab(subset2['Study'], subset2.redcap_event))#.to_csv('.csv',index=True)

    print('**********************************************')
    print('Behavioral Only Breakdown')
    subset3=PetraSandyErin.loc[PetraSandyErin.Petra_IntraDB_STG == "Behavioral Only"]
    print(pd.crosstab(PetraSandyErin.Study,subset3.redcap_event))


    #URLs for the interesting guys
    print('**********************************************')
    print("URLs for the interesting guys")
    print(PetraSandyErin.loc[PetraSandyErin.ITK_URL.isnull()==False][['subject','redcap_event','AnyUnusable','Covid_Comeback','ITK_URL']])
    sys.stdout = original_stdout




#'Sandy_DNR_exclusion', 'Sandy_DNR_moreinfo',
#'Sandy_IRB_exclusion', 'Sandy_IRB_moreinfo',

