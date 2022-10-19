import pandas as pd
import plotly.express as px
from sklearn.preprocessing import *
import numpy as np
import os
from functools import reduce
import datetime

def merge_sheets(file_path):
    sheets = list(pd.read_excel(file_path,None).keys())
    ls = []
    for sheet in sheets:
        df = pd.read_excel(file_path,sheet)
        values_col_name = sheet.split("_")[0]
        
        df[values_col_name] = df["Data osservazione,Valore"].apply(lambda x: x.split(",")[-1])
        df[values_col_name] = df[values_col_name].astype(float)
    
        df["Data"] = df["Data osservazione,Valore"].apply(lambda x: x.split(",")[0])
        df["Data"] = df["Data"].apply(lambda x: x.split(".")[0])
        df["Data"] = pd.to_datetime(df["Data"])
        df["Day_of_Week"] = df["Data"].apply(lambda x: x.strftime('%A'))
        df["Month_Name"] = df["Data"].apply(lambda x: x.strftime('%B'))
        
        df = df.drop(columns="Data osservazione,Valore").copy()
        df = df.sort_values(by="Data").reset_index(drop=True).set_index("Data")
        df["Location"] = ((file_path.split("/")[-1]).split(".")[0]).split("_")[-1]
        ls.append(df)
        
    return reduce(pd.DataFrame.combine_first, ls)

def merge_all_datas(main_folder_path):
    data_names = os.listdir(main_folder_path)
    ls = []
    for path in data_names:
        file_path = main_folder_path + f"/{path}"
        ls.append(merge_sheets(file_path))
    return pd.concat(ls)

def get_rolling_mean(df,location_col,location_param,col,num,center):
    df_rolling = df.copy()
    df_rolling = df_rolling.loc[df_rolling[location_col] == location_param].copy()
    for cols in col:
        df_rolling[f"Rolling_{cols}"] = df_rolling[cols].rolling(int(num),center=center).mean()
    return df_rolling

if __name__ == "__main__":
    merge_all_datas(str(os.getcwd()) + "/Dati").to_csv(os.getcwd())
    

    