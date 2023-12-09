Steps to Use The Profiling Automation

Dependinces 
python3 
pip / pip3
make sure service account is provisioned with read access to the required tables/views 


1. pip install -r requirements.txt 
2. 
    Export GOOGLE_APPLICATION_CREDENTIALS=service account path
    service account path should be absolute path .

3. python3 profiling.py <prefix>

<prefix> should be complete table prefix including project and dataset eg: tc-sc-bi-bigdata-dfl-qa.sandbox_dfl_qa.btd_scha_profilling_test_


output will be excel file named profiling.xlsx in source code directory 
if you need to change the output path goto 143 line profiling.py

logs will be maintained in profiling.log in the source code directory 
if you need to change the log path goto line 2 base_logger.py

