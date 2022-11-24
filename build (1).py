import pandas as pd
import numpy as np 
import ingest as ig
import validate as val
import datetime as dt
from collection import Queue
pd.options.mode.chained_assignment = None  # default='warn'
import clean

def magnify(las_id, df, colname):
    '''Breaks dataframe into individual client records'''
    event_start = ''
    las_col = ig.las_col(df)
    events = df[df[las_col]==las_id]
    for a in events.columns:
        if a.endswith(colname):
            event_start = a  
    return events[event_start]

def journeyProcess(df, **method):
    for k,v in method.items():
        df.rename({df.columns[0]:'event_dates'},axis=1, inplace=True)
        if v == 'reset':
            df = df.sort_values(by='event_dates', ascending=True).reset_index(drop=True)
        if v == 'noreset':
            df = df.sort_values(by='event_dates', ascending=True).reset_index()   
    return df

def journey(las_id,e1,e2):
    '''Pulls out event start dates from root event and all subsequent events on client record'''
    current_event = val.startDates(las_id,e1)
    next_event = val.startDates(las_id,e2)
    current_event = journeyProcess(current_event, method='reset')
    next_event = journeyProcess(next_event, method='noreset')
    
    dictionary = {}

    if current_event.empty==False and next_event.empty==False:
        baseline = current_event['event_dates'][0]
        next_event = next_event[next_event['event_dates']>=baseline]
        dictionary['current_event'] = current_event
        dictionary['next_event'] = next_event   
    else:
        dictionary['current_event'] = current_event
        dictionary['next_event'] = next_event
        
        
    return dictionary

def foreignKey(las_id,event1,event2):
    '''Builds a request dataframe for all requests linked to assessments if a link exists.'''
    df = pd.DataFrame()
    start_column = val.startColumns(las_id,event1)
    las_column = ig.las_col(event1)
    key_index = las_column.split('_')[0]
    output = journey(las_id,event1,event2)
    global_dates = output['current_event']['event_dates']
    if output == {}:
        df = None
    else:
        matched_pairs = date_match(output)
        if matched_pairs != []:
            matched_one = [a[0] for a in matched_pairs]
            unmatched = [a for a in global_dates if a not in matched_one]
            for a in matched_pairs:
                index = indices(output, a[1])
                filtered = event1[(event1[las_column]==las_id)&(event1[start_column]==a[0])]
                filtered[key_index+('_')+'next_key'] = index
                df = df.append(filtered)
            for b in unmatched:
                filtered = event1[(event1[las_column]==las_id)&(event1[start_column]==b)]
                filtered[key_index+('_')+'next_key'] = None
                df = df.append(filtered)
        else:
            for c in global_dates:
                filtered = event1[(event1[las_column]==las_id)&(event1[start_column]==c)]
                filtered[key_index+('_')+'next_key'] = None
                df = df.append(filtered)
                
    return df
                
            
          
def date_match(df):
    matches = []
    current_event = df['current_event']
    if df['next_event'].empty == False:
        next_event = df['next_event']
        outcome_col = current_event.columns[1]
        current_event = current_event[(current_event[outcome_col]=='Progress to Assessment / Unplanned Review')|\
                                        (current_event[outcome_col]=='Progress to Support Planning / Services')|\
                                         (current_event[outcome_col]=='Progress to Reablement/ST-Max')|\
                                         (current_event[outcome_col]=='Progress to Support Planning / Services')|\
                                         (current_event[outcome_col]=='Draft LT Plan - No Service')|\
                                         (current_event[outcome_col]=='ST Plan - No Service')]

        for a in next_event['event_dates']:
            counter_part = current_event[current_event['event_dates']<=a].max()
            matches.append((counter_part['event_dates'],a))
    return matches    

def indices(frame, date):
    df = frame['next_event']
    cols = ['index','event_dates']
    df = df[cols]
    
    for idx in df.index:
        if date == df['event_dates'][idx]:
            index = df['index'][idx]
    return index 


def pinon(las_id,x,df,startcol,lascol,event1, key):
    '''Helper function for requestBuilder'''
    df2 = pd.DataFrame()
    for i in x:
        if i not in list(df[startcol]):
            output2 = event1[(event1[lascol]==las_id)&(event1[startcol]==i)]
            output2[key+('_')+'next_key'] = None
            df2 = df2.append(output2)
    return df2

def eventCounter(alist):
    found = []
    for a in alist:
        one = magnify(a,requests)
        two = magnify(a, assessments)
        if len(two)> 1:
            found.append((a,len(one),len(two)))
    return found


def dataMerge(las_id,e1,e2):
    frame = foreignKey(las_id,e1,e2)
    #a,b,c = journey(las_id,e1,e2)
    output = journey(las_id,e1,e2)
    one,two = val.viewer(las_id,e1,e2)
    if frame is not None:
        if output['next_event'].empty == False:
            on = [i for i in frame.columns if i.endswith('next_key')][0]
            frame[on] = frame[on].apply(lambda x : str(x))
            e2 = e2.reset_index()
            e2['index'] = e2['index'].apply(lambda x : str(x))
            df = frame.merge(e2, left_on=on, right_on='index', how='left')
        else:
            empty = emptydf(las_id,e1,e2,frame)
            df = pd.concat([frame,empty],axis=1)    
    else:
        df = emptydf(las_id,e1,e2)  
    return df
   
def emptydf(las_id,df,df2,frame):
    out = val.viewer(las_id,df,df2)
    two = out[1]
    emptycols = two.columns
    if frame is not None:
        emptyrows = frame.index
        emptydf = pd.DataFrame(index=emptyrows,columns=emptycols)
    return emptydf 

def deadends(las_id,*df):
    out = val.viewer(las_id,*df)
    root = out[0]
    index = out[0].index
    columns = list(out[1].columns.append(out[2].columns))
    empty = pd.DataFrame(index=index,columns=columns)
    output = pd.concat([root,empty], axis=1, ignore_index=True)
    return output


def mergeTest(alist,e1,e2):
    failed = list()
    for i in alist:
        try:
            dataMerge(i,e1,e2)
        except:
            failed.append(i)
    if len(failed)==0:
        print('All tests passed')
    else:
        print(failed)
    
def Bridge(alist,event1,event2):
    '''1min 11s ± 5.01 s per loop (mean ± std. dev. of 7 runs, 1 loop each) for 1st merge'''
    '''4min 53s ± 7.71 s per loop (mean ± std. dev. of 7 runs, 1 loop each) for 2nd merge'''
    
    df = pd.DataFrame()
    for i in alist:
        plus = dataMerge(i,event1,event2)
        for a in plus.columns:
            if a.startswith('index'):
                plus.drop(a,axis=1, inplace=True)
        df = df.append(plus,ignore_index=True)
    return df.sort_index()

def diff(left,right, **method):
    for k,v in method.items():
        first_list = list(val.ids_list(left,'requests_Person Unique Identifier'))
        second_list = list(val.ids_list(right, 'requests_Person Unique Identifier'))
        if v == 'diff':
            diff = [a for a in first_list if a not in second_list]
            return diff
        if v == 'same':
            same = [a for a in first_list if a in second_list]
            return same

def ETL(event1,event2,event3):
    '''Final function that brings it all together'''
    main = ig.pull(HDM_requests=event1, HDM_assessments=event2, HDM_services=event3)
    las_column = ig.las_col(main[0])
    alist = val.ids_list(main[0],las_column)
    alist = list(map(str,alist))
    one = Bridge(alist,main[0],main[1])
    two = Bridge(alist,one,main[2])
    
    return two
    '''
    missing = diff(main[0], two, method='diff')
    for a in missing:
        deads = deadends(a,main[0],main[1],main[2])
        two = two.append(deads, ignore_index=True)  
    two['requests_Person Unique Identifier'] = two['requests_Person Unique Identifier'].apply(lambda x : int(x))
    two['requests_Person Unique Identifier'] = two['requests_Person Unique Identifier'].apply(lambda x : str(x))
    '''
    #return clean.colsDrop(two)


def quarterBuild(**loops):
    bag = list()
    today = dt.datetime.today()
    count = 1 
    date = today
    while count <= loops.get('loops'):
        quarter = False
        count2 = 0 
        while count2 < 12 and not quarter:
            date = date - dt.timedelta(weeks=count2)
            if date.month % 3 == 0: 
                start = date - dt.timedelta(weeks=12)
                collect = (start.strftime('01/%m/%Y'),date.strftime('30/%m/%Y'))
                bag.append(collect)
                quarter = True         
            else:
                count2 += 1
        date = date - dt.timedelta(weeks=12)
        count += 1
    
    return bag 


def quarterQueue(**loops):
    queue = Queue()
    x = quarterBuild(**loops)
    for i in x:
        queue.enqueue(i)
    return queue  