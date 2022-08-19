### Description
This folder works independently from Sherlock. It runs in Python to visualize results from Egads and Prophet pipelines.

### Directory layout
    ├── csv_files                         # Folder containing CSV files
    │   ├── abc.csv                           # Original time series CSV files       
    │   └── abc_full.csv                      # Time series files with Egads/Prophet expected values
    ├── convert_df_datetime.ipynb         # Python Notebook that converts datetime in "yyyy-mm-dd HH:mm:ss.SSS" format to unix timestamp 
    ├── plot_egads_prophet_anomaly.ipynb  # Python Notebook that plots "abc_full.csv" data
    ├── plot_cpu4_fullcpu4_full.ipynb     # Python Notebook that plots exclusively for "cpu4_full.csv" data
    ├── .gitignore                        # gitignore file
    └── README.md                         # readme

### Getting Started
```shell
pip3 install numpy pandas matplotlib tqdm jupyterlab
```

### Use provided CSV files to generate plots
* Launch JupyterLab with ```jupyter-lab```
* Open ```plot_egads_prophet_anomaly.ipynb```, replace filename with desired file, and run the notebook
* Alternatively, open ```plot_cpu4_full.ipynb```, and run the notebook

### CSV file formats
```plot_egads_prophet_anomaly.ipynb``` requires CSV files to adhere to below format for plotting. Example would be ```/csv_files/egads_sample_input_full.csv```.

| date                 | original                  | prophetExpected                   | prophetAnomaly | egadsExpected                   | egadsAnomaly |
|----------------------|---------------------------|-----------------------------------|----------------|---------------------------------|--------------|
| unix epoch timestamp | original time series data | expected time series from Prophet | yes/no         | expected time series from Egads | yes/no       |

```plot_cpu4_full.ipynb``` requires CSV files to adhere to below format for plotting. Example would be ```/csv_files/cpu4_full.csv```.

| date                 | original                  | prophetExpected                   | prophetAnomaly | egadsExpected                   | egadsAnomaly | label               |
|----------------------|---------------------------|-----------------------------------|----------------|---------------------------------|--------------|---------------------|
| unix epoch timestamp | original time series data | expected time series from Prophet | yes/no         | expected time series from Egads | yes/no       | 0/1 (correct label) |
