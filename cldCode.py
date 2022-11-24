import pandas as pd
import ingest as ig
import validate as val

class clData:
    def __init__(self):
        return None

    def column_dictionary(self,alist):
        d = {}
        for a,b in enumerate(alist):
            d[a] = list(b.columns)
        return d

    def column_picker(self,alist,blist):
        index = 0 
        while index < len(alist):
            index2 = 0 
            found = False
            while index2 < len(blist) and not found:
                if alist[index] == blist[index2]:
                    found = True 
                else: 
                    index2 += 1
            if found:
                blist[index2] = None 
            index +=1
        for a in blist:
            if a is not None:
                alist.append(a)
        return alist  
    
    def column_create(self, alist):
        x = self.column_dictionary(alist)
        for a in range(len(x)-1):
            xx = self.column_picker(x[0], x[a+1])
        return xx
    
    def data_create(self,alist):
        columns = self.column_create(alist)
        empty = pd.DataFrame(columns=columns)
        for items in alist:
            empty = empty.append(items)
        return empty

def to_str(df):
    df2 = df.copy().reset_index(drop=True)
    df2['Person Unique Identifier'] = df2['Person Unique Identifier'].apply(lambda x : str(x))
    return df2

def magnify(las_id,df):
    '''Breaks dataframe into individual client records'''
    event_start = ''
    las_col = ig.las_col(df)
    events = df[df[las_col]==las_id]
    for a in events.columns:
        if a.endswith('Primary Support Reason'):
            psr = a   
    return events[psr]

def psr_test(frame):
    failed = []
    idx = val.ids_list(frame,'Person Unique Identifier')
    for client in idx:
        client_record = magnify(client,frame)
        count = len(set(client_record.values))
        if count > 1:
            failed.append(client)
    return failed

def psr_fix(df):
    alist = psr_test(df)
    if len(alist) > 0:
        for client in alist:
            try:
                date = df[df['Person Unique Identifier']==client]['Event Start Date'].max()
                record = df[(df['Person Unique Identifier']==client)&(df['Event Start Date']==date)]\
                [['Primary Support Reason','Event Start Date','Event End Date','Accommodation Status','Employment Status']]
                date2 = record['Event End Date'].max()
                if len(record)>1:
                    if isinstance(date2, pd._libs.tslibs.nattype.NaTType) == False:
                        psr = record[(record['Event End Date']==date2)][['Primary Support Reason']].values[0]
                        acc = record[(record['Event End Date']==date2)][['Accommodation Status']].values[0]
                        emp = record[(record['Event End Date']==date2)][['Employment Status']].values[0]
                else:
                    psr = record['Primary Support Reason'].values[0]
                    acc = record['Accommodation Status'].values[0]
                    emp = record['Employment Status'].values[0]
                        
  
                filtered = df[df['Person Unique Identifier'] == client]
                for a in filtered.index:
                    df.loc[a, 'Primary Support Reason'] = psr
                    df.loc[a, 'Accommodation Status'] = acc
                    df.loc[a, 'Employment Status'] = emp
            except Exception as e:
                print(e,client)
                break

    return df