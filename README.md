# PreReleaseFolder
- The code in this inventory documents decisions about harmonizing events across data sources.
- EG it reduces the universe of behavioral data study design schema into that which can be linked by subject id and
an event code (V1, V2, V3, F1, F2, CR, etc).  It links parents with children, captures  expectations wrt parent-about-self
Toolbox data, maps out aliases for Double Winners, and hard codes deviations from expectations (visit summary information
currently captured in notes or trello tickets, for which there is no other reliable indicator of missingness in REDCap).

THIS NOTEBOOK IS IN PROGRESS and IS NOT PRETTY. It is intended to be sliced and diced for your particular use case, not run as a standalone code in your environment. Follow this repository for updates on Curated* codes in particular.
These indicators of datatype presence or absense will be undergoing the most changes over the course of curation.


![BehaveDiagram](/images/PreRelease.jpg)