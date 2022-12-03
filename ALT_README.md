1. Creat `osci/config/files/local.yml` from `osci/config/files/default.yml`
    1. Change base_path to `./.data`
    2. Input `github/token`
2. Use python<=3.8
3. Modify requirements to 
```
azure-core==1.7.0
azure-storage-blob==12.3.2
azure-storage-common==2.1.0
azure-storage-nspkg==3.1.0
google-cloud-bigquery==1.25.0
pyyaml==5.4
pyarrow == 0.17.1
pandas == 1.0.5
requests==2.22.0
click==7.1.2
six==1.13.0
XlsxWriter==1.2.3
Jinja2==2.11.3
deepmerge==0.1.1
numpy==1.22.0
python-dateutil==2.8.1

markupsafe==2.0.1
```
4. Have JDK8
5. (Optional) change logging level to WARNING in `osci-cli.py`