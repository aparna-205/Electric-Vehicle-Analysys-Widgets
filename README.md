```import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
df=pd.read_csv(r"C:\Users\InGo Electric\Downloads\0109v1 - Charge 5A.CAN",sep=";")
df.drop(columns="Unnamed: 12",inplace=True)
df.fillna(0,inplace=True)
df1=df[df['ID'].str.contains('0x101')]
df1.replace(['0x',''], ['',''], regex=True,inplace=True)
df1["ACK/NACK Responce"]=df1['D4'].astype(int).apply(lambda x:"Success" if x>60 else "Failure")
df2=df[df['ID'].str.contains('0x102')]
df2['102'] = df2['D1'] + df2['D0']
dfbin=df2["status"].apply(int, base=16).apply(bin)
dff=dfbin.replace(["0b",""],["",""],regex=True)
dff2=dff.to_csv(r"C:\Users\InGo Electric\Downloads\bin.csv")
dfc=pd.read_csv(r"C:\Users\InGo Electric\Downloads\bin.csv")
dfc["number of cells"]=dfc["cell1"]+dfc["cell2"]+dfc["cell3"]+dfc["cell4"]+dfc["cell5"]+dfc["cell6"]+dfc["cell7"]+dfc["cell8"]+dfc["cell9"]+dfc["cell10"]+dfc["cell11"]+dfc["cell12"]+dfc["cell13"]
given_set={1}
df['new'] = df.isin(given_set).sum(1)
given_set={0}
df['working cells'] = df.isin(given_set).sum(1)
df1=df.drop(['new'], axis=1)
df2=df1.drop(['number of cells'], axis=1)
given_set={1}
df2['cells gone'] = df2.isin(given_set).sum(1)
df2["Text"]= "cells working"
df2["Working_Cells"]=df2["working cells"].astype(str)+" "+df2["Text"]
df3=df2.drop(['Text'],axis=1)
df3["Text"]= "cells gone"
df3["cells_gone"]=df3["cells gone"].astype(str)+" "+df3["Text"]
df3=df3.drop(['Text'],axis=1)
df3=df3.drop(['cells gone'],axis=1)
import pandas as pd
df=pd.read_csv(r"C:\Users\InGo Electric\Downloads\bin.csv")
def res(cell1):
    if cell1==1:
        return "Detected"
    elif cell1==0:
        return "not Detected"
df["OCDL"]=df["cell1"].apply(res)
def res(cell2):
    if cell2==1:
        return "Detected"
    elif cell2==0:
        return "not Detected"
df["OTF"]=df["cell2"].apply(res)
def res(cell3):
    if cell3==1:
        return "Detected"
    elif cell3==0:
        return "not Detected"
df["AFE alert"]=df["cell3"].apply(res)
def res(cell4):
    if cell4==1:
        return "Detected"
    elif cell4==0:
        return "not Detected"
df["UTD"]=df["cell4"].apply(res)
def res(cell5):
    if cell5==1:
        return "Detected"
    elif cell5==0:
        return "not Detected"
df["UTC"]=df["cell5"].apply(res)
def res(cell6):
    if cell6==1:
        return "Detected"
    elif cell6==0:
        return "not Detected"
df["OTD"]=df["cell6"].apply(res)
def res(cell7):
    if cell7==1:
        return "Detected"
    elif cell7==0:
        return "not Detected"
df["OTC"]=df["cell7"].apply(res)
def res(cell8):
    if cell8==1:
      return "Detected"
    elif cell8==0:
      return "not Detected"
df["ASCDL"]=df["cell8"].apply(res)
def res(cell9):
    if cell9==1:
        return "Detected"
    elif cell9==0:
        return "NotDetected"
df["ASCD"]=df["cell9"].apply(res)
def res(cell10):
    if cell10==1:
        return "Detected"
    elif cell10==0:
        return "NotDetected"
df["AOLDL"]=df["cell10"].apply(res)
def res(cell11):
    if cell11==1:
        return "Detected"
    elif cell11==0:
        return "NotDetected"
df["AOLD"]=df["cell11"].apply(res)
def res(cell12):
    if cell12==1:
        return "Detecetd"
    elif cell12==0:
        return "NotDetected"
df["OCD"]=df["cell12"].apply(res)
def res(cell13):
    if cell13==1:
        return "Detecetd"
    elif cell13==0:
        return "NotDetected"
df["OCC"]=df["cell13"].apply(res)
df['Voltage'] = df['D2'] + df['D1']
df["Voltage"]=df["Voltage"].apply(lambda x:str(int(str(x),16)))
df['Voltage'] = df['Voltage'].astype(float) *0.001
df['Current'] = df['D4'] + df['D3']
df["Curernt"]=df["Current"].apply(lambda x:str(int(str(x),16)))
df['Current'] = df['Current'].astype(float) *0.001
df['D5'] = df['D5'].map(lambda x: chr(int(x, 16)))
#alternatives
df['D5'] = df['D5'].apply(lambda x: chr(int(x, 16)))
df['D5'] = [chr(int(x, 16)) for x in df['D5']]
def res(D5):
    if D5=="C":
        return "Charging"
    elif D5=="D":
        return "Dis Charging"
    elif D5=="N":
        return "Not Discharging"
df["Current Response"]=df["D5"].apply(res)
df7['107'] = df7['D1'] + df5['D0']
df7["Temperature"]=df7["107"].apply(lambda x:str(int(str(x),16)))
df8['Battery Capacity'] = df8['Battery Capacity'].astype(float)*0.001
df8["Battery Health"]=df8["D2"]
df8["Battery Health"]=df8["Battery Health"].apply(lambda x:str(int(str(x),16)))
df8["SOC"]=df8["D3"]
df8["SOC"]=df8["SOC"].apply(lambda x:str(int(str(x),16)))
df9['V1'] = df9['D1'] + df9['D0']
df9['V2'] = df9['D3'] + df9['D2']
df9['V3'] = df9['D5'] + df9['D4']
df9['V4'] = df9['D7'] + df9['D6']
df9["V1"]=df9["V1"].apply(lambda x:str(int(str(x),16)))
df9["V2"]=df9["V2"].apply(lambda x:str(int(str(x),16)))
df9["V3"]=df9["V3"].apply(lambda x:str(int(str(x),16)))
df9["V4"]=df9["V4"].apply(lambda x:str(int(str(x),16)))
df10["V1"]=df10["D1"]+df10["D0"]
df10["V2"]=df10["D3"]+df10["D2"]
df10["V3"]=df10["D5"]+df10["D4"]
df10["V4"]=df10["D7"]+df10["D6"]
df10["V1"]=df10["V1"].apply(lambda x:str(int(str(x),16)))
df10["V2"]=df10["V2"].apply(lambda x:str(int(str(x),16)))
df10["V3"]=df10["V3"].apply(lambda x:str(int(str(x),16)))
df10["V4"]=df10["V4"].apply(lambda x:str(int(str(x),16)))
df9['V1'] = df9['V1'].astype(float) *0.001
df9['V2'] = df9['V2'].astype(float) *0.001
df9['V3'] = df9['V3'].astype(float) *0.001
df9['V4'] = df9['V4'].astype(float) *0.001
df10['V1'] = df10['V1'].astype(float) *0.001
df10['V2'] = df10['V2'].astype(float) *0.001
df10['V3'] = df10['V3'].astype(float) *0.001
df10['V4'] = df10['V4'].astype(float) *0.001
df11["V1"]=df11["D1"]+df11["D0"]
df11["V2"]=df11["D3"]+df11["D2"]
df11["V3"]=df11["D5"]+df11["D4"]
df11["V4"]=df11["D7"]+df11["D6"]
df11["V1"]=df11["V1"].apply(lambda x:str(int(str(x),16)))
df11["V2"]=df11["V2"].apply(lambda x:str(int(str(x),16)))
df11["V3"]=df11["V3"].apply(lambda x:str(int(str(x),16)))
df11["V4"]=df11["V4"].apply(lambda x:str(int(str(x),16)))
df11['V1'] = df11['V1'].astype(float) *0.001
df11['V2'] = df11['V2'].astype(float) *0.001
df11['V3'] = df11['V3'].astype(float) *0.001
df11['V4'] = df11['V4'].astype(float) *0.001
df11["V1"]=df11["D1"]+df11["D0"]
df11["V2"]=df11["D3"]+df11["D2"]
df11["V3"]=df11["D5"]+df11["D4"]
df11["V4"]=df11["D7"]+df11["D6"]
df11["V1"]=df11["V1"].apply(lambda x:str(int(str(x),16)))
df11["V2"]=df11["V2"].apply(lambda x:str(int(str(x),16)))
df11["V3"]=df11["V3"].apply(lambda x:str(int(str(x),16)))
df11["V4"]=df11["V4"].apply(lambda x:str(int(str(x),16)))
df11['V1'] = df11['V1'].astype(float) *0.001
df11['V2'] = df11['V2'].astype(float) *0.001
df11['V3'] = df11['V3'].astype(float) *0.001
df11['V4'] = df11['V4'].astype(float) *0.001
df12['Voltage'] = df12['D1'] + df12['D0']
df12["Voltage"]=df12["Voltage"].apply(lambda x:str(int(str(x),16)))
df12['Voltage'] = df12['Voltage'].astype(float) *0.001
df=df[df['ID'].str.contains('0x115')]
def res(D0):
    if D0==80:
        return "Success"
    elif D0==60:
        return "Failure"
df["Ack Responce"]=df["D0"].apply(res)
df['Voltage'] = df['D2'] + df['D1']
df["Voltage"]=df["Voltage"].apply(lambda x:str(int(str(x),16)))
df['Voltage'] = df['Voltage'].astype(float) *0.001
df['Current'] = df['D4'] + df['D3']
df["Curernt"]=df["Current"].apply(lambda x:str(int(str(x),16)))
df['Current'] = df['Current'].astype(float) *0.001
df['D5'] = df['D5'].map(lambda x: chr(int(x, 16)))
#alternatives
df['D5'] = df['D5'].apply(lambda x: chr(int(x, 16)))
df['D5'] = [chr(int(x, 16)) for x in df['D5']]
def res(D5):
    if D5=="C":
        return "Charging"
    elif D5=="D":
        return "Dis Charging"
    elif D5=="N":
        return "Not Discharging"
df["Current Response"]=df["D5"].apply(res)
df["D6"]=df["D6"].apply(lambda x:str(int(str(x),16)))
df["D7"]=df["D7"].apply(lambda x:str(int(str(x),16)))
df=df[df['ID'].str.contains('0x116')]
df['Cell'] = df['D1'] + df['D0']
df.Cell Status.apply(int, base=16).apply(bin)
df=df.replace(["0b",""],["",""],regex=True)
def res(cell):
    if Cell==0:
        return "Cell is Good"
    elif Cell==1:
        return "Failure Of the cell"
df["Cell Status"]=df["Cell"].apply(res)
df['Fault Condition'] = df['D2'] + df['D3']
df["Fault Condition"]=df["Fault Condition"].apply(lambda x:str(int(str(x),16)))
df['Time in hour'] = df['D4']
df['Time in minuts'] = df['D5']
df['Battery Capacity'] = df['D7'] + df['D6']
df["Battery Capacity"]=df["Battery Capacity"].apply(lambda x:str(int(str(x),16)))```
