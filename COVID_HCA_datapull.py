# PROGRAM TO WORK WITH DOWNLADED COLLECTION OF FILES CONTAINING HCA COVID-RELATED DATA
# I.e...SUBSET AND ORGANIZE FOR ANALYSIS
# SEE PPT OVERVIEW in PreRelease Box folder, Data Dictionaries, Supporting Document pages
import pandas as pd


#Download the files from Box by hand
#specify the full path to these download files (ending in a "/")
DL_directory="/home/petra/Desktop/HCA_PreReleaseBehavioral-selected/"  #Note that WINDOWS uses \ instead of / and a ew other things for folders

#read the Inventory file
inventory=pd.read_csv(DL_directory+"HCA_Inventory_2022-02-04.csv",low_memory=False)
inventory.columns
#find subjects who two visits before CR, one visit before CR, and one visit on each side of CR.  Determine temporal breakdown of these visits.
subs=inventory.loc[inventory.redcap_event.isin(['V1','V2','CR'])].sort_values(['subject','daysfromtime0'])[['subject','daysfromtime0','redcap_event']]

len(subs.subject.unique()) #1215 subjects total
a=subs.loc[subs.redcap_event=='V1'] #1215 have V1
b=subs.loc[subs.redcap_event=='V2'] #611 have V1
c=subs.loc[subs.redcap_event=='CR'] #498 have V1
ab=pd.merge(a[['subject','daysfromtime0']],b[['subject','daysfromtime0']], on='subject',how='left').rename(columns={'daysfromtime0_y':'daysfromtime0_V2'}).drop(columns=['daysfromtime0_x'])
abc=pd.merge(ab,c[['subject','daysfromtime0']],on='subject',how='left').rename(columns={'daysfromtime0':'daysfromtime0_CR'})
abc.head()


subwmeventsOR=abc.loc[(abc.daysfromtime0_V2>0) | (abc.daysfromtime0_CR>0)] #806
subwmeventsAND=abc.loc[(abc.daysfromtime0_V2>0) & (abc.daysfromtime0_CR>0)] #299
subwmeventsBOTH=abc.loc[(abc.daysfromtime0_V2>abc.daysfromtime0_CR)] #9
#for report:
subwmeventsAND['difftime']=subwmeventsAND.daysfromtime0_CR - subwmeventsAND.daysfromtime0_V2
summary=subwmeventsAND.describe(percentiles=[.05,.10,.25,.5,.75,.9,.95])
summary.daysfromtime0_CR=summary.daysfromtime0_CR.round(0)
summary.daysfromtime0_V2=summary.daysfromtime0_V2.round(0)
summary.difftime=summary.difftime.round(0)


#read the REDCap file
REDCap=pd.read_csv(DL_directory+"HCA_RedCap_2022-03-04.csv",low_memory=False)

#read the TLBX file
TLBX=pd.read_csv(DL_directory+"HCA_NIH-Toolbox-Scores_2022-01-28.csv",low_memory=False)

#these are the column names in TLBX
TLBX.columns

#THESE ARE THE INSTRUMENTS FROM WHICH YOU CAN CHOOSE (note that all the rearranging is basically so I can copy and paste it into a slide)
T=pd.DataFrame(list(TLBX.Inst.unique()))
T.columns=['Inst']
list(T.Inst)

#THESE ARE THE INSTRUMENTS THAT WERE COLLECTED FOR CR
#and the number of subjects data collected for each instrument.
#Numbers are small because CR data was shared with V2 and V1.  E.g close to 500 did the CR, but only half of those have most of the batteries below.
#I suspect (but do not know) that really small counts are likely because of wrong instruments being loaded by RAs in the app
for i in list(TLBX.loc[TLBX.redcap_event=='CR','Inst'].unique()):
    print(TLBX.loc[(TLBX.redcap_event=='CR') & (TLBX.Inst==i)].shape,i)

#you can increase sample sizes by pulling the data from these guys:
#use the V2 event data as CR for these subjects:
subjV2list=list(inventory.loc[inventory.Curated_TLBX=='SEE V2'].subject)
#use the V1 event data as CR for these subjects:
subjV1list=list(inventory.loc[inventory.Curated_TLBX=='SEE V1'].subject)

#define a function to get a specific instrument and it's relevent columns from the TLBX scored data
#use the 'Toolbox Scores That Should Be Present in Data.xlsx' spreadsheet in Supporting docs (in Box) for variable name subsets
def getTLBXinst(dset,instrument,varlist,event,subjlist=[]):
    subset=dset.loc[(dset.Inst==instrument) & (dset.redcap_event==event)][varlist]
    if subjlist==[]:
        return subset
    if subjlist !=[]:
        return subset.loc[subset.subject.isin(subjlist)]

a=getTLBXinst(TLBX,'NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1',['PIN','subject','redcap_event','RawScore'],'CR',subjlist=[])
b=getTLBXinst(TLBX,'NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1',['PIN','subject','redcap_event','RawScore'],'V2',subjlist=subjV2list)

#send the combo for longitudinal analysis against the V1 data, for example
combinedT=pd.concat([a,b])
combinedT.to_csv(DL_directory+'ListSortingCR.csv',index=False)



#get the subset of the Redcap Data Dictionary that is relevant fo COVID data
#first get all of them
REDCapCols=pd.read_csv(DL_directory+"HCA_RedCap_DataDictionary_2022-01-28.csv")
#now subset to the ones that are in the covid forms
REDCapCovidvars=REDCapCols.loc[((REDCapCols['Form Name'].str.upper().str.contains('COVID')) | (REDCapCols['Variable / Field Name'].str.contains('rt_moca'))) & (~(REDCapCols['Form Name'].str.upper().str.contains('REGISTER')))]
REDCapCovidvars['Section Header'].unique()


#subset further to list variable names that are not actually in the data because they are housekeeping vars to flag missingness and/or restricted date variables
inlist=[i for i in list(REDCapCovidvars['Variable / Field Name']) if i in REDCap.columns]
#add back list of 'checkbox' fields.  why?  you just dropped everythin not in the actual data -- checkbox fields are something like 'variable1' in the data dictionary but expand to 'variable1___1' and 'variable1___2' in the data
unexpanded=['covid1','covid3','covid5','covid8','bt_covid_1','bt_covid3','bt_covid5','bt_covid8','bt_covid_1','bt_covid3','bt_covid5','bt_covid8']
#combine these two lists to create a data dictionary for this subset
REDCapCovidvars=REDCapCovidvars.loc[REDCapCovidvars['Variable / Field Name'].isin(inlist+unexpanded)][['Branching Logic (Show field only if...)','Variable / Field Name','Form Name','Field Label','Choices, Calculations, OR Slider Labels']].copy()
REDCapCovidvars.to_csv(DL_directory+'CovidDataDictionaryREDCap.csv',index=False)

#now create a list of variables to keep in the data itself
#add back in fields whose names are expanded when they are exported -> i.e. multiple choice fields in data *dictionary* expand out to variable name + '___' + option in *data*
inlist2=['covid1___','covid3___','covid5___','covid8___']
inlist2exp=[]
for i in inlist2:
    spike_cols = [col for col in REDCap.columns if i in col]
    inlist2exp=inlist2exp+spike_cols
#export data subset for the covid events and variables
REDCapCovid=REDCap.loc[REDCap.redcap_event.isin(['Covid','CR'])][['subject','redcap_event']+inlist+inlist2exp]
REDCapCovid.to_csv(DL_directory+'CovidDataREDCap.csv',index=False)

#### SOME BASIC STATS about COVID tests.  2 times in the Covid event.  1 time in the Covid Remote event.
#subjects with data by event
REDCapCovid.value_counts('redcap_event',dropna=False)

#Of the Covid (first survey sent out in March) ... breakdown of who was tested and test results
REDCapCovid.loc[REDCapCovid.redcap_event=='Covid'].covid4_covid.value_counts(dropna=False)
REDCapCovid.loc[REDCapCovid.redcap_event=='Covid'].covid4_covid_pos.value_counts(dropna=False)

#Of the Covid (2nd survey sent out in ~June) ... breakdown of who was tested and test results
REDCapCovid.loc[REDCapCovid.redcap_event=='Covid'].bt_covid4_bt_covid.value_counts(dropna=False)
REDCapCovid.loc[REDCapCovid.redcap_event=='Covid'].bt_covid4_bt_covid_pos.value_counts(dropna=False)

#of the full Covid-Remote data collection event cohort ... breakdown of who was tested and test results
REDCapCovid.loc[REDCapCovid.redcap_event=='CR'].rt_covid4_bt_covid.value_counts(dropna=False)
REDCapCovid.loc[REDCapCovid.redcap_event=='CR'].rt_covid4_bt_covid_pos.value_counts(dropna=False)


############################################################
#now get the TOOLBOX data

