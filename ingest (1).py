import pandas as pd
import pyodbc as odbc
from datetime import datetime as dt


def pull(**queries):
    ''' Main function that takes in query and outputs the dictionary'''
    dictionary = dictionarise(**queries)            
    jobs = tasks(dictionary)
    frame = main_frame(jobs, dictionary)
    
    return frame
 

def dictionarise(**data):
    ''' Builds a dictionary using events as keys 
    and a dataframe of associated query results as values. '''
    dictionary = dict()
    for event, query in data.items():
        data = column_rename(event, SQLDB(database = event.rsplit('_',1)[0]).open_query_close((query)))
        dictionary[event] = data
        
    return dictionary


def tasks(dictionary):
    '''Counts each event in the dictionary and creates a queue of jobs
    This queue determines the order of processing the data associated with each event.'''
    jobs = Queue()
    for task in dictionary.keys():
        jobs.enqueue(task)
    print('%s job(s) in queue.'%(jobs.size()))
    
    return jobs


def column_rename(event,inflow):
    '''Renames columns to names set by keywords provided by user
    Helps with visually identifying datasets to each event and provides a prefix
    for querying columns by event. '''
    for a in range(len(inflow.columns)):
        inflow.rename({inflow.columns[a]:str(event.split('_')[1])+'_'+inflow.columns[a]}, axis=1, inplace=True)
        
    return inflow


def las_id(df):
    '''identifies las id for the 0th row in a dataframe
    For use in the las_col that matches this id to a column in the dataset.'''
    df_copy = df.copy()
    df_copy.reset_index(drop=True)
    las = [a for a in df_copy.loc[0] if str(a).isnumeric() and len(str(a))>=7][0]  
  
    return las


def las_col(df):
    '''Identifies the relevant las column in the data 
    to ensure both datasets contain an las column for basic matching'''
    idx = las_id(df)
    df_copy = df.copy()
    df_copy.reset_index(drop=True)
    first = df_copy.loc[0].to_frame().reset_index()
    column = first[first[0]==idx]['index'].values[0]
    
    return column 


def main_frame(jobs,library):
    '''Creates a list of dataframes for all tasks in the queue.'''
    mainframes = []
    for num in range(jobs.size()):
        df = job_unload(jobs,library)
        mainframes.append(df)
        
    return mainframes


def job_unload(jobs, library):
    '''Makes a dataframe for each load i.e. event. '''
    load, key = empty_queue(jobs, library)
    df = library[load]
    return df


def empty_queue(jobs, library):
    '''Empties the job queue individually for each dataframe_load. '''
    if not jobs.is_empty():
        current_load = jobs.dequeue()
        file = library[current_load]
        las_name = las_col(file)
    else:
        print('No jobs to empty.')
            
    return current_load, las_name   


class Queue:
    def __init__(self):
        self.items = []
    
    def is_empty(self):
        return self.items == []
    
    def enqueue(self,item):
        self.items.insert(0,item)
    
    def size(self):
        return len(self.items)
    
    def dequeue(self):
        return self.items.pop()

class SQLDB:
    def __init__(self, 
                 server = 'SEDV-PRDLLR01',
                 trusted_connection = 'yes',
                 driver = "{ODBC Driver 17 for SQL Server}",
                 database = "HDM"
                ):
        self.server = server
        self.trusted_connection = trusted_connection
        self.driver = driver
        self.database = database
    
        self.connection_string = "Driver=%s;Server=%s;Database=%s;Trusted_Connection=%s;"%(self.driver, self.server, self.database, self.trusted_connection)
    
    def open_query_close(self,q):
        conn = odbc.connect(self.connection_string)
        result = pd.read_sql(q,conn)
        conn.close()
        
        return result