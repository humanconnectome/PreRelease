import pandas as pd

prepath="/home/petra/Behavioral/Lifespan/PreRelease/"
sandyA="HCA Excluded Participants_2021May05 (1).xlsx"
sandyD="HCD Excluded Participants_2021Mar01 (1).xlsx"
IRB="IRB exclusions"
DNR="Data release exclusions"
Complain="Subject Complaints"

petra="Lifespan_REDCap_Based_Exclusions_From_PreRelease_NO_DUPS.csv"
inventorypath='/home/petra/Behavioral/Lifespan/PreRelease/PreRelease/'
versionold='11_01_2022'

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
import numpy as np
PetraSandyErin.loc[PetraSandyErin.PetraUnusable==0,'PetraUnusable']=np.nan
PetraSandyErin.loc[PetraSandyErin.SandyUnusable==0,'SandyUnusable']=np.nan
PetraSandyErin.loc[PetraSandyErin.ErinUnusable==0,'ErinUnusable']=np.nan
PetraSandyErin.loc[PetraSandyErin.AnyUnusable==0,'AnyUnusable']=np.nan
PetraSandyErin['SuggestRelease3.0']=np.nan
PetraSandyErin.loc[PetraSandyErin.Petra_IntraDB_STG.str.contains('STG')==True,'SuggestRelease3.0']=1
PetraSandyErin=PetraSandyErin.rename(columns={'Data':'SandyWhatHappened2Data'})
PetraSandyErin[['subject','redcap_event','redcap_age','PreReleaseInventory', 'Petra_IntraDB_STG', 'SuggestRelease3.0','AnyUnusable','ErinUnusable',
                'PetraUnusable','SandyUnusable','SandyWhatData','SandyWhatHappened2Data','SandyExclusion','Petra_exclusion',
        'Erin_exclusion']].to_csv(prepath+'InventoryUnusables_21Nov2022.csv',index=False)

#'Sandy_DNR_exclusion', 'Sandy_DNR_moreinfo',
#'Sandy_IRB_exclusion', 'Sandy_IRB_moreinfo',

