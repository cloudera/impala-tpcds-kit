These templates are from [TPC-DS v2.11.0](http://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request.asp?bm_type=TPC-DS&bm_vers=2.11.0&mode=CURRENT-ONLY). 

The following query variants are used:

- query5a
- query10a
- query18a
- query22a
- query27a
- query35a
- query36a
- query67a
- query70a
- query77a
- query80a
- query86a

The following MQM are used:

- Use of vendor-specific syntax of date expressions and date casting. (MQM f.1, f.8)
    - query5a
    - query12 
    - query16 
    - query20 
    - query21 
    - query32 
    - query37 
    - query40 
    - query72 
    - query77a
    - query80a
    - query82 
    - query92 
    - query94 
    - query95 
    - query98
- Use of nested table expression alias. (MQM e.4)
    - query2
- Use of column aliases. (MQM e.5)
    - query5a
    - query77a
    - query80a
    - query90
- Use of vendor-specific syntax of concatenation operator. (MQM c.3)
    - query5a
    - query66
    - query80a
    - query84

Additional bug fixes:

- query22a - removal of erroneous `group by`
- query77a - missing comma in `cr` CTE

Unsupported queries (9):

- query8 (INTERSECT)
- query9 (subquery in SELECT)
- query14 (INTERSECT)
- query23 (subquery in HAVING)
- query24 (subquery in HAVING)
- query38 (INTERSECT)
- query44 (subquery in HAVING)
- query45 (subquery in OR)
- query87 (EXCEPT)
