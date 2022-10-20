# PROGRAM TO WORK WITH DOWNLADED COLLECTION OF FILES CONTAINING HCA COVID-RELATED DATA
# I.e...SUBSET AND ORGANIZE FOR ANALYSIS
# SEE PPT OVERVIEW in PreRelease Box folder, Data Dictionaries, Supporting Document pages
import pandas as pd


#Download the files from Box by hand
#specify the full path to these download files (ending in a "/")
DL_directory="/home/petra/Desktop/HCA_PreReleaseBehavioral-selected/"  #Note that WINDOWS uses \ instead of / and a ew other things for folders

#read the REDCap file
REDCap=pd.read_csv(DL_directory+"HCA_RedCap_2022-03-04.csv",low_memory=False)

#get the subset of the Redcap Data Dictionary that is relevant fo COVID data
#first get all of them
REDCapCols=pd.read_csv(DL_directory+"HCA_RedCap_DataDictionary_2022-01-28.csv")
#now subset to the ones that are in the covid forms
REDCapCovidvars=REDCapCols.loc[((REDCapCols['Form Name'].str.upper().str.contains('COVID')) | (REDCapCols['Variable / Field Name'].str.contains('rt_moca'))) & (~(REDCapCols['Form Name'].str.upper().str.contains('REGISTER')))]

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


