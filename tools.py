import pandas as pd 
import clean
import matplotlib.pyplot as plt
import datetime as dt

def data(file):
    data = pd.read_csv(file,index_col=False)
    x = clean.colsDrop(data)
    #data.drop('Unnamed: 0',axis=1,inplace=True)
    x = clean.colsClean(data)
    x = convertDT(x)
    x = age(x)
    x = add_provider(x)
    return x

def lenn(col,row):
    for a in enumerate(x[col][row]):
        out = a[0]
    print(out+1)
    
'''
'Request Event start date', 'Request Event end date', 
           'Date of Birth', 'Date of Death', 
           'Assessment Event start date', 'Assessment Event end date', 
           'Service Event start date', 'Service Event end date',

'''
def convertDT(df,cols_to_use):
    import datetime as dt
    df.reset_index(drop=True,inplace=True)
    datecols = cols_to_use
    
    for rows in range(len(df)):
        for cols in datecols:
            if type(df[cols][rows]) != float:
                #print(str(df[cols][rows]))
                df[cols][rows] = dt.date.fromisoformat(str(df[cols][rows]))
    return df
                
def age(df):
    index = 0 
    df = df.dropna(subset=['Date of Birth']).reset_index(drop=True)
    df['Age'] = None
    while index < len(df):
        df['Age'][index] = ((dt.date.today() - df['Date of Birth'][index])/365).days
        index += 1
    return df

def convertstr(df):
    import datetime as dt
    df.reset_index(drop=True,inplace=True)
    datecols = ['Request Event start date', 'Request Event end date', 
           'Date of Birth', 'Date of Death', 
           'Assessment Event start date', 'Assessment Event end date', 
           'Service Event start date', 'Service Event end date']
    
    for rows in range(len(df)):
        for cols in datecols:
            if type(df[cols][rows]) != float:
                #print(str(df[cols][rows]))
                df[cols][rows] = str(df[cols][rows])
    return df

def add_provider(df):
    df['Provider'] = None
    for a, b in enumerate(df['Name']):
        if isinstance(b, str):
            df['Provider'].loc[a] = b.split('-')[0]
            stopover = b.split('-')[0]
            df['Provider'].loc[a] = stopover.split('(')[0]
    return df
 
def assDelta(df):
    '''Time taken to complete assessment by month'''
    df['Assessment Delta'] = abs(df['Assessment Event start date'] - df['Assessment Event end date'])
    subset = df[df['Assessment Delta'].isna() == False]
    subset['Assessment Delta'] = subset['Assessment Delta'].apply(lambda x: x.days)
    subset['Month'] = subset['Assessment Event start date'].apply(lambda x: x.month)
    xx = subset.groupby(['Age','Eligibility'])['Assessment Delta'].mean()
    xx = xx.to_frame()
    xx = xx.reset_index()
    return xx

def ldLivEmp(df):
    '''% of LD clients for diff accommodation status by diff employment status'''
    agg = df.groupby(['Employment Status','Accommodation Status'])['Accommodation Status'].count().to_frame()
    sums = agg['Accommodation Status'].sum()
    agg['Percentage'] = agg['Accommodation Status'].apply(lambda x: round((x/sums)*100,2))
    return agg

def mediCount(df):
    '''Count of medical conditions'''
    medical_conditions = df.columns[18:37]
    condi = {}
    for i in medical_conditions:
            count = len(df[i].dropna())
            condi[i] = count
    return condi  

def healthProfile(df):
    '''Health profile by ethnicity'''
    medical_conditions = mediCount(df)
    eth = [i for i in set(df['Ethnicity'])]
    starter = pd.DataFrame(index=eth)
    for i in medical_conditions:
        x = df.groupby(['Ethnicity'])[i].count()
        starter = pd.concat([starter,x],axis=1)

    starter['sum'] = starter.sum(axis=1)
    for a in starter.index:
        for b in starter.columns[:-1]:
            starter.loc[a,b] = (starter.loc[a,b])
    pd.set_option('display.float_format', lambda x: '%.2f' % x)
    return starter   

def serviceCostBreakDown(df):
    df['Service Component'].replace([' Home Support'],'Home Support',inplace=True)
    return df.groupby(['Service Type','Service Component'])[['Planned Cost (Weekly)']].median()

def healthConditionVisits(df):
    '''Mean weekly visits by health condition'''
    medical_conditions = mediCount(df)
    blank = pd.DataFrame(columns=['Health Condition','Service Type','Weekly Visits (Count)','Planned Cost (Weekly)'])
    for i in medical_conditions: 
        value = df.groupby([i,'Service Type'])[['Service Type','Weekly Visits (Count)','Planned Cost (Weekly)']].mean().dropna().reset_index()
        condition = value.columns[0]
        for j in range(len(value)):
            value.loc[j,i] = i 
        value.rename({condition:'Health Condition'},axis=1, inplace=True)
        blank = blank.append(value)
    blank = blank.reset_index(drop=True)
    return blank

def create_providers(df):
    providers = df.groupby(['Service Type','Service Component','Provider'])['Provider'].count()
    providers = providers.to_frame()
    providers.rename({'Provider': 'Count'},axis=1,inplace=True)
    providers.reset_index(inplace=True)
    return providers
    
def provider_count(df):
    '''Top 10 Providers By Service Type Rendered'''
    providers = create_providers(df)
    types = set(providers['Service Type'])
    bag = pd.DataFrame()
    for a in types:
        sub = providers[providers['Service Type']==a].sort_values(by='Count', ascending=False).head(10)
        bag = bag.append(sub)
    return bag.reset_index(drop=True)

def dateViewer(las_id,df):
    output = df[df['LAS ID']==las_id][['Request Event start date','Assessment Event start date','Service Event start date','Planned Cost (Weekly)']]
    return output

def viewer(df,x,y,**types):
    for a,b in types.items():
        plt.figure(figsize=(20,8))
        if b == 'lineplot':
            sns.lineplot(x=df[x],y=df[y])
            plt.show()
        if b == 'histogram':
            '''for histogram y value is simply a placeholder'''
            plt.hist(x=df[x],bins=30,edgecolor='k')
            plt.show()
         
