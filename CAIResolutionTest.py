### This script is to

## 1. Aggregate 1min cumulative accessibility increment data to 2min, 3min, 5min and 10min for resolution tests

## 2. Identify the maximum CAI index, 10th, 50th and 90th CAI index for each unit of cumulative accessibility increment

## The output is used for the travel time estimation models

import csv
import os
import math
import psycopg2 as pg
import numpy as np


schema="tt_matrix"
mode="transit"

conn = pg.connect("dbname=aodb-fhwa user=nexusadmin")
cur = conn.cursor()


print('Part 1: Aggregate 1min CAI data')

cai_1min="bg_donut_acc_c000_auto_2015"
cai_1min_table="{}.{}".format(schema, cai_1min)

aggreUnit=[1,2,3,5,10]

def create_table(aggreUnit):
    aggreTableName="bg_donut_acc_{}min".format(aggreUnit)
    aggreTable="{}.{}".format(schema, aggreTableName)
    cur.execute("select exists (select 1 from information_schema.tables where table_schema='{}' and table_name='{}')".format(schema, aggreTableName))
    if cur.fetchone()[0]==False:
        cur.execute("select b_geoid into {} from {} order by b_geoid asc".format(aggreTable, cai_1min_table))
        conn.commit()
    

def CAITableUpdate(aggreUnit):
    aggreTableName="bg_donut_acc_{}min".format(aggreUnit)
    aggreTable="{}.{}".format(schema, aggreTableName)
    roundNum=60/aggreUnit
    i=1
    while i<=roundNum:
        columnName="donut_{}min".format(i*aggreUnit)
        cur.execute("select exists (select 1 from information_schema.columns where table_name='{}' and column_name='{}')".format(aggreTableName, columnName))
        if cur.fetchone()[0]==False:
            cur.execute("alter table {} add {} bigint".format(aggreTable, columnName))
            conn.commit()
        j=1
        unitGroup=[]
        while j<=60:
            if math.ceil(j/aggreUnit)==i:
                unitGroup.append(j)
            j+=1
        code=""
        for unit in unitGroup:
            unitName="donut_c000_{}min+".format(unit)
            code+=unitName
        code=code[:-1]
        #print(code)
        cur.execute("update {} set {}=x.aggreDonut from (select b_geoid, {} as aggreDonut from {}) x where x.b_geoid={}.b_geoid".format(aggreTable, columnName, code, cai_1min_table, aggreTable))
        conn.commit()        
        i+=1
    

for unit in aggreUnit:
    print("Aggreagating for Unit={}min".format(unit))
    create_table(unit)
    CAITableUpdate(unit)

print("Part 2: Identify CAI index")

def alterTableAddColumn(tableName,columnName):
    table="{}.{}".format(schema, tableName)
    cur.execute("select exists (select 1 from information_schema.columns where table_name='{}' and column_name='{}')".format(tableName, columnName))
    a=cur.fetchone()[0]
    if a!=True:
        cur.execute("alter table {} add {} bigint".format(table, columnName))
        conn.commit()
    

def CAIIndexAlterTable(tableName, percentOpt):    
    if percentOpt==100:
        columnName="maxindex"
        alterTableAddColumn(tableName,columnName)
    else:
        columnName1="p{}thup".format(percentOpt)
        columnName2="p{}thdown".format(percentOpt)
        alterTableAddColumn(tableName,columnName1)
        alterTableAddColumn(tableName,columnName2)


def CAIIndexCal(percentOpt,dataSource):#dataSource refers to each line of the aggreagated CAI tables
    maxValue=0
    maxIndex=0
    buildArray=[]
    for i, access in enumerate(dataSource):
        #print(access)
        if i!=0:
            if access>maxValue:                
                maxValue=access
                maxIndex=i*percentOpt
            buildArray.append(access)
    if percentOpt==100:
        CAIOutputUp=maxIndex
        CAIOutputDown=maxIndex
    else:
        accessArray=np.array(buildArray)
        pth=np.percentile(accessArray,percentOpt)
        pthDifUp=99999999
        pthDifDown=99999999
        pthIndexUp=0
        pthIndexDown=0

        for i, access in enumerate(dataSource):
            if i!=0:
                if i>maxIndex:
                    if abs(access-pth)<pthDifUp:
                        pthDifUp=abs(access-pth)
                        pthIndexUp=i*percentOpt
                else:
                    if abs(access-pth)<pthDifDown:
                        pthDifDown=abs(access-pth)
                        pthIndexDown=i*percentOpt
        CAIOutputDown=pthIndexDown
        CAIOutputUp=pthIndexUp
    return CAIOutputUp, CAIOutputDown
    
def CAIIndexUpdateTable(geoID, percentOpt,CAIOutputUp,CAIOutputDown,tableName):
    table="{}.{}".format(schema, tableName)
    if percentOpt==100:
        columnName="maxindex"
        cur.execute("update {} set {}={} where b_geoid={}".format(table, columnName, CAIOutputUp, geoID))
        conn.commit()
    else:
        columnName1="p{}thup".format(percentOpt)
        columnName2="p{}thdown".format(percentOpt)
        cur.execute("update {} set {}={} where b_geoid={}".format(table, columnName1, CAIOutputUp, geoID))
        cur.execute("update {} set {}={} where b_geoid={}".format(table, columnName2, CAIOutputDown, geoID))
        conn.commit()



for unit in aggreUnit:
    print("Working on Unit={}min".format(unit))
    roundNum=60/unit
    aggreTableName="bg_donut_acc_{}min".format(unit)
    aggreTable="{}.{}".format(schema, aggreTableName)
    cur.execute("select * from {} order by b_geoid asc".format(aggreTable))
    data=cur.fetchall()
    percentOpts=[10,50,90,100]
    for percentOpt in percentOpts:
        CAIIndexAlterTable(aggreTableName, percentOpt)
        for num, value in enumerate(data):
            geoID=value[0]
            dataSource=[]
            for i, access in enumerate(value):
                if i<=roundNum:
                    dataSource.append(access)
            #print(geoID)
            CAIOutputUp, CAIOutputDown=CAIIndexCal(percentOpt,dataSource)            
            CAIIndexUpdateTable(geoID, percentOpt, CAIOutputUp, CAIOutputDown, aggreTableName)
        
        
        

    



print("---------------------Calculating Finished------------------------")


