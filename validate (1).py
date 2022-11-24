import ingest as ig
import numpy as np
import pandas as pd 
from datetime import datetime
import random



def cost(df):
    '''Pulls out the cost row from each dataframe'''
    df = df.select_dtypes('float')
    df = df.loc[0]
    for a in df:
        if (len(list(str(a))))>=6:
            fig = a
           
    return fig


def ids_list(df, las_col):
    '''Pulls a distinct list of las ids from a df.'''
    las_ids = df[las_col].values
    las_ids = tuple(np.unique(las_ids))
    
    return las_ids


def startDates(las_id,df):
    '''Pulls out event start dates for each client.'''
    las_col = ig.las_col(df)
    dataframe = df[df[las_col]==las_id]
    if 'services_Event Start Date' in dataframe.columns:
        start_col = ['services_Event Start Date','services_Event Outcome']
        
    elif 'assessments_Event Start Date' in dataframe.columns:
        start_col = ['assessments_Event Start Date','assessments_Event Outcome']
        
    else:
        start_col = ['requests_Event Start Date','requests_Event Outcome']
   
    return dataframe[start_col]

def startColumns(las_id,df):
    las_col = ig.las_col(df)
    if 'services_Event Start Date' in df.columns:
        start_col = 'services_Event Start Date'

    elif 'assessments_Event Start Date' in df.columns:
        start_col = 'assessments_Event Start Date'

    else:
        start_col = 'requests_Event Start Date'

    return start_col
            
    
    
    
def datestan(date):
    '''Converts all dates into datetime'''
    standard = pd.to_datetime(date) 
    return standard

          
def cost_compare(ids,df1,df2):
    '''Outputs the costs figures for each dataframe for comparison.'''
    
    value1 = 0 
    value2 = 0 
    list1 = list()
    list2 = list()
    base = dates(ids,df1)
    branch = dates(ids,df2)
    
    list1.append(base)
    list2.append(branch)
    matches = date_match(list1, list2)
    for items in matches:
        value1, value2 = compute(ids,items,df1,df2)

    return value1,value2


def compute(ids,matched,df,df2):
    '''Computes the average costs between 2 dataframes for use in the cost_compare function above.'''
    #start date columns
    start_col_1 = [a for a in df.columns if a.endswith('Start Date')][0]
    start_col_2 = [a for a in df2.columns if a.endswith('Start Date')][0]
    #las id columns
    las_col_1 = ig.las_col(df)
    las_col_2 = ig.las_col(df2)
    #cost rows
    cost_1 = cost(df)
    cost_2 = cost(df2)
    #cost columns
    cost_col_1 = df.apply(lambda row:row[row==cost_1].index,axis=1)[0][0]
    cost_col_2 = df2.apply(lambda row:row[row==cost_2].index,axis=1)[0][0]
    #dataframes
    one = df[(df[start_col_1]==matched)&(df[las_col_1]==ids)]
    two = df2[(df2[start_col_2]==matched)&(df2[las_col_2]==ids)]
    #values 
    value1 = one[cost_col_1].values.mean()
    value2 = two[cost_col_2].values.mean()
    
    return value1,value2  


def validator(las_ids,df,df2):
    '''Pulls out instances where the absolute cost differences are not zero. Its for error detection'''
    deviation = list()
    ids = list()
    ours = list()
    theirs = list()
    for items in las_ids:
        try:
            a,b = cost_compare(items,df,df2)
            if abs(a-b)!=0:
                deviation.append(abs(a-b))
                ids.append(items)
                ours.append(a)
                theirs.append(b)
        except:
            continue
    
    return pd.DataFrame(list(zip(ids,ours,theirs,deviation)),columns=['LAS ID','Ours','Theirs','Difference'])


def viewer(ids,*df):
    '''Slices both dataframes for specific las ids.'''
    dictionary = {}
    for count,items in enumerate(df):
        col = ig.las_col(items)
        output = items[items[col]==ids]
        dictionary[count] = output
    return dictionary


def generator(idx,num):
    '''For use in QA, generates a random list of LAS IDs to assess closer.'''
    LAS = list()
    for num in range(num):
        las = random.choice(idx)
        LAS.append(las)
    return LAS


class bsdetector:
    '''Bullshit detector which counts the number of rows for each variable between 2 difference dataframes'''
    '''Returns a new dataframe.'''
    def __init__(self,variable,df1,df2,col1,col2):
        ids = list()
        counts = list()
        second_counts = list()
        frame = pd.DataFrame()
        for a in variable:
            count = len(df1[df1[df1.columns[col1]]==a])
            second_count = len(df2[df2[df2.columns[col2]]==a])
            ids.append(a)
            counts.append(count)
            second_counts.append(second_count)
        self.ids = ids
        self.counts = counts
        self.second_counts = second_counts
        self.frame = pd.DataFrame(list(zip(ids,self.counts,self.second_counts)))
    
    def variation(self):
        '''Creates a new dataframe of aboslute difference in the variable you want'''
        self.frame['VAR'] = abs(self.frame[1] - self.frame[2])
        differences = self.frame[self.frame['VAR']!=0]
        return differences   