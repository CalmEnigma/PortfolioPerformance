### SET UP
#%reset

# Import libraries
import datetime as dt
import calendar
import numpy as np
import pandas as pd
import os as os
import math
import pyxirr
import decimal
import itertools as itertools
from pyxirr import xirr
from collections import Counter
from numpy import inf
from itertools import product

# Turn off copywarnings
pd.options.mode.chained_assignment = None  # default='warn'

# Time periods
year = 2022                                     # EDIT THIS
month = 8                                       # EDIT THIS
day = 31                                        # EDIT THIS
day_num = day
datetype = '%d-%b-%Y'

end = dt.date(year, month, day)
start = dt.date(year-1, 12, 31)

mperiod = pd.date_range(start,end,freq='M').strftime(datetype).tolist()
yperiod = pd.date_range(start,end,freq='Y').strftime(datetype).tolist()
if end not in yperiod:
    yperiod.append(end)

# Select date before which returns should not be calculated
limit = dt.date(2020, 12, 31)

# Select date before which NAVs should not be calculated
limit2 = dt.date(2021, 12, 31) 

# Select date before which historical NAVs should not be displayed
# 2 years of NAVs
limit3 = max([limit2, dt.date(year-2, 12, 31)])

# Set and check directories
dir_current = 'C:\\Users\\calmp\\OneDrive\\Work\\KSH\\0 - Revamp'
os.chdir(dir_current)
os.getcwd()

# Get last month directory
ms_date = end.replace(day=1)- dt.timedelta(days=1)
n = str(ms_date)
n1 = n[:4]
n2 = n[5:7]
dir_past = f'{dir_current}\\{n1} {n2}\\0 - Transformed Data'


### IMPORTING

# File name
source = 'Raw Data v3.xlsx'                     # EDIT THIS
source2 = 'Stationary Data.xlsx'                # EDIT THIS

# Read static data excel files
df_det = pd.read_excel(source, sheet_name='Position Details')

# Read flow-related excel files
df_bal = pd.read_excel(source, sheet_name='Bank Balances')
f_inv = pd.read_excel(source, sheet_name='F_Invest')
f_exp = pd.read_excel(source, sheet_name='F_Expense')
f_loan = pd.read_excel(source, sheet_name='F_Loan')
f_int = pd.read_excel(source, sheet_name='F_Internal')
f_conv = pd.read_excel(source, sheet_name='F_Convert')

# Read nav-related excel files
n_cust = pd.read_excel(source, sheet_name='Custom Private',
                       index_col=[0,1,2,3,4], skiprows = [1])
n_pe = pd.read_excel(source, sheet_name='Other Private')
n_bond = pd.read_excel(source, sheet_name='Bonds',
                       index_col=[0,1,2,3,4,5,6], header = [0,1], skiprows = [3])
n_eq = pd.read_excel(source, sheet_name='Equities',
                     index_col=[0,1,2,3,4,5,6], skiprows = [1])
n_der = pd.read_excel(source, sheet_name='Derivs',
                      index_col=[0,1,2,3,4,5,6,7,8], skiprows = [1])
n_fxf = pd.read_excel(source, sheet_name='FX F')
n_fxo = pd.read_excel(source, sheet_name='FX O',
                      index_col=[0,1,2,3,4,5,6,7,8,9,10,11,12,13], skiprows = [1])
pe_com = pd.read_excel(source, sheet_name='RemCom')




### CLEANING AND REARRANGING

# Fix Position Details dataframe
#f_det.info()

# Fix Bank Balances dataframe
df_bal['Date'] = df_bal['Date'].apply(lambda x: x.date())

# Fix Investment Flows dataframe
f_inv['Date'] = f_inv['Date'].apply(lambda x: x.date())
f_inv['Outflows'].fillna(0, inplace=True)
f_inv['Inflows'].fillna(0, inplace=True)

# Fix Loan Flows dataframe
f_loan['Date'] = f_loan['Date'].apply(lambda x: x.date())
f_loan['Outflows'].fillna(0, inplace=True)
f_loan['Inflows'].fillna(0, inplace=True)
f_loan['Flow'] = f_loan['Inflows']-f_loan['Outflows']

# Fix Internal Flows dataframe
f_int['Date'] = f_int['Date'].apply(lambda x: x.date())
f_int['Outflows'].fillna(0, inplace=True)
f_int['Inflows'].fillna(0, inplace=True)
f_int['Flow'] = f_int['Inflows']-f_int['Outflows']

# Fix Conversion Flows dataframe
f_conv['Date'] = f_conv['Date'].apply(lambda x: x.date())

# Fix Expense Flows dataframe
f_exp['Date'] = f_exp['Date'].apply(lambda x: x.date())
f_exp.drop('Notes', axis=1, inplace=True)
f_exp['Subname Lower'] = f_exp['Position Subname'].apply(lambda l: l.lower())

# Fix Custom NAVs dataframe
n_cust = n_cust.stack()
n_cust.name = 'NAV'
n_cust = n_cust.reset_index()
n_cust.rename(columns={'level_5': 'Date'}, inplace = True)
n_cust['Date'] = n_cust['Date'].apply(lambda x: x.date())
n_cust['Last Updated'] = n_cust['Last Updated'].apply(lambda x: x.date())

# Fix Other Private NAVs dataframe
n_pe = n_pe[['Date', 'Position Name', 'Position Subname', 'Asset Class B',
             'Currency', 'NAV']]
n_pe['Date'] = n_pe['Date'].apply(lambda x: x.date())

# Fix Bond NAVs dataframe
n_bond = n_bond.stack()
n_bond = n_bond.reset_index()
n_bond['Date'] = n_bond['Date'].apply(lambda x: x.date())
n_bond['Accrued'].fillna(0, inplace=True)

# Fix Equity NAVs dataframe
n_eq = n_eq.stack()
n_eq.name = 'Price'
n_eq = n_eq.reset_index()
n_eq.rename(columns={'level_7': 'Date'}, inplace = True)
n_eq['Date'] = n_eq['Date'].apply(lambda x: x.date())

# Fix Derivative NAVs dataframe
n_der = n_der.stack()
n_der.name = 'Price'
n_der = n_der.reset_index()
n_der.rename(columns={'level_9': 'Date'}, inplace = True)
# n_der['Date'] = n_der['Date'].apply(lambda x: dt.datetime.strptime(x, '%d-%m-%y %H:%M:%S'))
n_der['Date'] = n_der['Date'].apply(lambda x: x.date())
n_der['Expiry'] = n_der['Expiry'].apply(lambda x: x.date())

# Fix FX Forwards NAVs dataframe
n_fxf['Date Initiated'] = n_fxf['Date Initiated'].apply(lambda x: x.date())
n_fxf['Expiry'] = n_fxf['Expiry'].apply(lambda x: x.date())

# Fix FX Options NAVs dataframe
n_fxo = n_fxo.stack()
n_fxo.name = 'NAV'
n_fxo = n_fxo.reset_index()
n_fxo.rename(columns={'level_14': 'Date'}, inplace = True)
n_fxo['Date'] = n_fxo['Date'].apply(lambda x: x.date())
n_fxo['Expiry'] = n_fxo['Expiry'].apply(lambda x: x.date())
n_fxo['Date Initiated'] = n_fxo['Date Initiated'].apply(lambda x: x.date())
n_fxo['Spot Price'] = n_fxo['Spot Price'].apply(lambda l:
                                                'NA'
                                                if 'Column' in l
                                                else l)

# Fix Remaining Commitments dataframe
pe_com['Date'] = pe_com['Date'].apply(lambda x: x.date())





## CHECK THAT ALL POSITIONS USE THE SAME REGISTERED CURRENCY
y = ['Position Name', 'Currency']
x = n_bond[y].append(n_cust[y])
x = x.append(n_der[y])
x = x.append(n_eq[y])
x = x.append(n_pe[y])
x['NAV'] = 1

x = x[['Position Name', 'Currency', 'NAV']]
x = x.groupby(['Position Name', 'Currency']).agg(sum).reset_index()
x = list(x['Position Name'])
x = list(set(i for i in x if x.count(i) > 1))

if len(x)>0:
    print('*** Warning! ***  The following Positions have been assigned multiple currencies:', x)
else:
    pass




### UNIVERSAL INVESTMENT CALCULATIONS

# Calculate total units in each month end
units = []
for i in mperiod:
    loopdate = dt.datetime.strptime(i, datetype).date()                                 
    x = f_inv[f_inv['Date'] <= loopdate]
    x = x.groupby(['Position Name', 'Position Subname'])['Units Direction'].sum().reset_index()
    x['Units Direction'] = round(x['Units Direction'],3)
    x['Date'] = loopdate
    units.append(x)                               #Add each df created to list
units = pd.concat(units, ignore_index=True)       #Concat each df in list
units.rename(columns={'Units Direction': 'Units'}, inplace= True)

# Give direction to flows
f_inv['Flow'] = f_inv['Inflows']-f_inv['Outflows']

# Calculate MTD flows
f_mtd = []
for i in mperiod:
    loopdate = dt.datetime.strptime(i, datetype).date()
    last = loopdate.replace(day=1)- dt.timedelta(days=1)
    x = f_inv[(f_inv['Date'] <= loopdate) & (f_inv['Date'] > last)]
    x = x.groupby(['Position Name','Position Subname'])['Flow'].sum().reset_index()
    x['Date'] = loopdate
    f_mtd.append(x)                                 
f_mtd = pd.concat(f_mtd, ignore_index=True)
f_mtd.rename(columns={'Flow': 'MTD Flows'}, inplace= True)

# Calculate YTD flows
f_ytd = []
for i in mperiod:
    loopdate = dt.datetime.strptime(i, datetype).date()
    last = loopdate.replace(day=1, month=1)- dt.timedelta(days=1)
    x = f_inv[(f_inv['Date'] <= loopdate) & (f_inv['Date'] > last)]
    x = x.groupby(['Position Name', 'Position Subname'])['Flow'].sum().reset_index()
    x['Date'] = loopdate
    f_ytd.append(x)                                 
f_ytd = pd.concat(f_ytd, ignore_index=True)
f_ytd.rename(columns={'Flow': 'YTD Flows'}, inplace= True)










### UNIVERSAL INVESTMENT FUNCTIONS

# MTD % gain function
def mtd_ret(flow_df, name, msnav, nav, m_start, m_end, nametype = 'Position Subname',
            subset = "all", subset_name = 'all'):
    
    # Get relevant flows. Subsets are used when calculating MTD% of asset classes by vehicle
    # which means you have to only use the positions in the asset classes of the vehicles
    a = flow_df[(flow_df['Date'] <= m_end) & (flow_df['Date'] > m_start)
              & (flow_df[nametype] == name)]    
    
    # Use a subset if there are multiple filters e.g. all asset classes that are alsin KSH
    if subset != 'all':
        a = a[a[subset]==subset_name]
    else:
        pass
    
    # Create and sort dataframe with NAVs and flows of the month
    a = a[['Date', 'Flow']]
    a = a.append(pd.DataFrame({'Date': [m_start, m_end],
                  'Flow': [-msnav, nav]}),
                 ignore_index = True)
    a = a.sort_values(by=['Date']).reset_index(drop=True)
        
    # Correct for positions that open during the month
    # Correct for positions that close during the month  
    # by dropping all 0 values
    a = a[a['Flow']!=0]
    a = a.reset_index(drop=True)

    # Determine the whether the first flow is positive or negative
    # This determines how to consider the change in value
    # The df may be empty at this point, so also include a try clause
    try:
        b = 1 if a['Flow'][0] < 0 else -1
    except (ValueError, KeyError):
        b = 1
    
    # Allocate flows to beginning or end of month
    # For the IRR to replicate a MTD/YTD calc, all flows must be at the beginning or end of the period
    
    # If the direction is negative, it's a long position (negative initial flow)
    # Thus, aggregate all negative flows at month start, and all positive ones at month end
    # Do the reverse for short positions
    if b==1:
        a.loc[a['Flow']<0, 'Date'] = m_start
        a.loc[a['Flow']>0, 'Date'] = m_end
    else:
        a.loc[a['Flow']>0, 'Date'] = m_start
        a.loc[a['Flow']<0, 'Date'] = m_end    
    #print(name, a)
    
    # Return monthly IRR adjusted for number of days passed   
    try:
        if ((len(set(a['Date'])) > 1) & (m_start>=limit)
            & (sum(1 for number in a['Flow'] if number < 0) > 0)    # at least 1 negative and positive number
            & (sum(1 for number in a['Flow'] if number > 0) > 0)):
            irr = xirr(a['Date'], a['Flow'])
            irr = (1+irr) ** ((max(a['Date'])-min(a['Date'])).days/365) -1
            
            # Multiply IRR by direction to ensure the returns have the right sign
            irr = irr*b
            
            return irr             #(1+xirr(a['Date'], a['Flow'])) ** ((m_end-min(a['Date'])).days/365) -1
        else:
            return None      # No result for positions with no monthly numbers
    except (TypeError):
        return 0         # NaN for things like derivatives that do not have % returns



# YTD % gain function  (aggregates from MTD %)  
def ytd_ret(df, name, y_start, m_end, nametype = 'Position Subname',
            subset = "all", subset_name = 'all'):
    
    # Create list with relevant MTD% returns 
    try:
        x = df[(df['Date'] <= m_end) & (df['Date'] > y_start)
                  & (df[nametype] == name)]
        
        if subset != 'all':
            x = x[x[subset]==subset_name]
        else:
            pass
        
        x = x['MTD %'].fillna(0)
        x = x.to_numpy()
        x = x+1
        
        # Compound the monthly rate into a YTD rate 
        return np.cumprod(x, dtype=float)[-1]-1     # Return last value in array and then do -1
    
    except KeyError:
        return 'NaN'


# YTD % gain function  (MTD % weighted by Cumulative Base Value) 
def ytd_ret_w(df, name, y_start, m_end, nametype = 'Position Subname',
            subset = "all", subset_name = 'all'):
    
    # Create list with relevant MTD% returns 
    try:
        x = df[(df['Date'] <= m_end) & (df['Date'] > y_start)
                  & (df[nametype] == name)]
        
        if subset != 'all':
            x = x[x[subset]==subset_name]
        else:
            pass
        
        # Calculate weights
        x['TotW'] = x['CumBase'].sum()
        x['Weight'] = x['CumBase']/x['TotW']*len(x)
        
        # Weight the MTD %
        x['MTD %'] = x['MTD %'].fillna(0)
        x = x['MTD %'] * x['Weight']
        
        # Compound the MTD %
        x = x.to_numpy()
        x = x+1
        
        # Compound the monthly rate into a YTD rate 
        return np.cumprod(x, dtype=float)[-1]-1     # Return last value in array and then do -1
    
    except KeyError:
        return 'NaN'




# Average cost function
def av_cost(name, cost_type="avg", nametype = 'Position Subname', date = end):
    #print(name)
    
    # Select dataframe section
    x = f_inv[(f_inv[nametype] == name) &
              (f_inv['Flow Subtype'].isin(['Buy', 'Sell', 'Premium']))&
              (f_inv['Date']<=date)]
       
    # Select required columns
    x = x[['Date', 'Flow Subtype', 'Units Direction', 'Price']]
    
    # Aggregate by date for correct calculations on flows occuring on the same date
    x['Value'] = x['Price']*x['Units Direction']
    calcs = {'Units Direction': 'sum',
             'Value': 'sum'}
    x = x.groupby(['Date', 'Flow Subtype']).agg(calcs).reset_index()
    x['Price'] = x['Value']/x['Units Direction']
    
    # Calculate cumulative units
    x['CumU'] = np.cumsum(x['Units Direction'], axis=0)
    x = x.sort_values(by='Date', ascending = False).reset_index(drop=True)
    
    # Save the current number of units if there are any values in the df
    if len(x)>0:
        z = x['CumU'][0]

        # Calculate cumulative buy units backwards if long position, and cumulative sales if short
        if z>=0:
            y = x[x['Flow Subtype'].isin(['Buy', 'Premium'])]
        else:
            y = x[x['Flow Subtype'].isin(['Sell', 'Premium'])]
        y['CumBuy'] = np.cumsum(y['Units Direction'], axis=0)
        
        # Calculate the remaining units before each buy transaction
        y = y.sort_values(by='Date', ascending = False).reset_index(drop=True)
        y['Remainder'] = z - y['CumBuy']
        
        # Calculate the number of units in the buy transaction that are still relevant in the current position
        if z>=0:
            y['UCh'] = y.apply(lambda l: l['Units Direction'] if l['Remainder']>=0 else l['Units Direction']+l['Remainder'], axis=1)
            y['UCh'] = y.apply(lambda l: np.max([l['UCh'], 0]), axis=1)
        else:
            y['UCh'] = y.apply(lambda l: l['Units Direction'] if l['Remainder']<=0 else l['Units Direction']+l['Remainder'], axis=1)
            y['UCh'] = y.apply(lambda l: np.min([l['UCh'], 0]), axis=1)
        
        # Calculate the value of the currently relevant units
        y['UxP'] = y['UCh']*y['Price']
        
        # Calculate costs
        avg_cost = sum(y['UxP'])/sum(y['UCh']) if sum(y['UCh']) >0 else 0
        tot_cost = sum(y['UxP'])
        
        # Return average or total cost
        if cost_type == "avg":
            return avg_cost
        else:
            return tot_cost
       
    # If df is empty, return 0
    else:
        return len(x)+1

#y = av_cost('CS Put (7.6 May)', 'avg', 'Position Subname')


# Find cumulative base value
def get_cumbase_sub(df, name, date, y_start, ys_nav, current_units, ytd_flows, fx):
    
    # Filter and sort df
    a = df[(df['Position Subname']==name) &
           (df['Date']>y_start) &
           (df['Date']<= date) &
           (df['Date']> limit2)]
    a = a[a['NAV']!=0]
    a = a.sort_values(by='Date', ascending=True).reset_index(drop=True)
    
    # Get YS NAV or first NAV of year
    if ys_nav != 0:
        ys = "Yes"
        pass
    else:
        ys = "No"
        ys_nav = df['NAV'][0]
    
    # If the position has units:
    if abs(a['Units']).sum() > 0:        
        # Get YS Units or first units of year
        ys_u = a['Units'][0]
        # Calculate YS Price
        ys_p = ys_nav/ys_u
        # Multiply the YS Price by the current units to get cum base value#
        cumbase = ys_p*current_units/fx
    
    # If the position does not have units:
    else:    
        # If position existed at YS, get YTD flows:
         if ys != 'No': 
            # Cumbase = YS NAV - YTD flows, min 0
            cumbase = np.max([ys_nav-ytd_flows,0])/fx
        # If not, then cumbase = ytd flows as the initial nav is included:
         else:   
            cumbase = np.max([-ytd_flows,0])/fx
    
    return cumbase




# Sum all flows
def sumflow(name, nametype = 'Position Subname', date = end):
    
    # Select dataframe section
    x = f_inv[(f_inv[nametype] == name) & (f_inv['Date']<=date)]
    
    # Run sum
    x = x['Flow'].sum()
    return -x




# Find first investment in a position    
def first_inv(name, name_type):
    x = f_inv[f_inv[name_type]==name]['Date'].min()
    return x


# Identify closed positions if NAV=0
# And no flow this year
# And not flows after last nav date
# ANd if the first NAV has not occurred during the year (in case there are empty shell positions entered with no flows yet)
def p_closed(df, name):
    x = df[df['Position Subname']==name]
    x = x.sort_values(by='Date', ascending = False).reset_index(drop=True)
    y = x['Date'][0]    # Latest NAV date
    a = np.min(x['Date'])   # First NAV date
    x = x['NAV'][0]     # Current latest registered NAV
    z = f_inv[f_inv['Position Subname']==name]
    if len(z)>0:
        z = z['Date'].max()      # Latest transaction
    else:
        z = dt.date(1940, 12, 31)
    # print(y,z, a)
    return 'Closed' if (x==0) & (start>=z) & (start>=a) & (y>z) else 'Open'


# Get prices from equities sheet
def get_price(fx, date, fwd_points = False):
    if fwd_points == False:
        name = fx
    else:
        name = '3M ' + fx + ' Fwd Points'              # Name of the 3M fwd points, keep consistent
    a = n_eq[(n_eq['Position Name'] == name)
             & (n_eq['Date'] == date)]
    a = a.reset_index(drop=True)
    return 0 if len(a) == 0 else a['Price'][0]


# Get EUR-FX rate
def get_fx(currency, date, start = 'current'):
    if currency.strip() == 'EUR':                              # base currency EUR
        y = 1
    else:
        name = "EUR-" + currency.strip()
        
        # Get year start conversion if required
        if start == 'YS':
            date = dt.date(date.year-1, 12, 31)
        elif start == 'MS':
            date = date.replace(day=1)
            date = date-dt.timedelta(days=1)
        else:
            date = date
        
        # Get price for desired date and currency    
        y = n_eq[(n_eq['Date'] == date) & (n_eq['Position Name']==name)]
        #print(y, name, date)
        y = y['Price'].reset_index()
        y = y['Price'][0]
    return y


# Get other FX rates based on their full name
def get_fx2(name, date, start = 'current'):        
    # Get year start conversion if required
    if start == 'YS':
        date = dt.date(date.year-1, 12, 31)
    elif start == 'MS':
        date = date.replace(day=1)
        date = date-dt.timedelta(days=1)
    else:
        date = date
    
    
    # Print name (to check)
    #print(name, date)
    
    # Get price for desired date and currency    
    y = n_eq[(n_eq['Date'] == date) & (n_eq['Position Name']==name)]
    #print(y, name, date)
    y = y['Price'].reset_index()
    
    try:
        y = y['Price'][0]
    except(KeyError):
        print('**Error**: There is a new FX conversion being used which has no price.',
              name)
        y = float('nan')
    return y

# Get period start values from a df
def get_pstartval(dataframe, date, subname, value_to_get, start):
    
    # Identify start date
    if start == 'YS':
        date = dt.date(date.year-1, 12, 31)
    elif start == 'MS':
        date = date.replace(day=1)
        date = date-dt.timedelta(days=1)
    else:
        return 'Incorrect start date'
    
    # Get desired value for desired date and position
    try:
        y = dataframe[(dataframe['Date']==date)]
        y = y[y['Position Subname']==subname]
        y = y[value_to_get].reset_index()
        y = y[value_to_get][0]
        return y
    except KeyError:
        return 0








### CALCULATIONS: BONDS

# Find unique positions
subnames = list(set(n_bond['Position Subname']))

# Calculate NAVs
a = units[units['Position Subname'].isin(subnames)]
n_bond = n_bond.merge(a, on = ['Position Name', 'Position Subname', 'Date'],
                      how='left')
n_bond['NAV'] = n_bond['Units']/100*(n_bond['Price']+n_bond['Accrued']*100)





### CALCULATIONS: EQUITIES

# Drop benchmarks and unlinked prices
x = n_eq[(~n_eq['Asset Class B'].isin(['Benchmark']))]
x.dropna(subset=['Asset Class B'], inplace=True)

# Find unique positions
subnames = list(set(x['Position Subname']))

# Calculate NAVs
a = units[units['Position Subname'].isin(subnames)]
n_eq = n_eq.merge(a, on = ['Position Name', 'Position Subname', 'Date'],
                      how='left')
n_eq['NAV'] = n_eq['Units']*n_eq['Price']






### CALCULATIONS: DERIVATIVES

# Find unique positions
subnames = list(set(n_der['Position Subname']))

# Calculate NAVs
a = units[units['Position Subname'].isin(subnames)]
n_der = n_der.merge(a, on = ['Position Name', 'Position Subname', 'Date'],
                      how='left')
n_der['NAV'] = n_der['Units']*n_der['Price']*n_der['Units per Contract']

# Identify closed positions
n_der['Closed'] = n_der.apply(lambda l:
                              'Closed'
                              if ((l['Date']>=l['Expiry']) | (l['Price']==0))
                              else 'Open',
                              axis=1)
n_der['Closed'] = n_der.apply(lambda l:
                              l['Closed']
                              if (abs(l['Units'])>0)
                              else 'Closed',
                              axis=1)

# For futures closed within the year, assign Price = last price, so that YTD gain remains constant
# And does not become negative due to current NAV being 0 with no flows

# Define function to get latest future price recorded
def get_fut_p(name, asset_class, date):
    if (asset_class == 'Futures'):
        
        # Select observations where position prices have values
        a = n_der[(n_der['Position Subname']==name)&(n_der['Date']<=date)]
        a = a[a['Price']!=0]
        
        # Get latest price
        b = a.loc[a['Date']==np.max(a['Date']), 'Price']
        b = b.reset_index(drop = True)
        
        return b[0]
        
    else:
        return float('nan')

# Define function to get last future units recorded
def get_fut_u(name, asset_class, status):
    if (asset_class == 'Futures'):
        
        # Select observations where position prices have values
        a = n_der[(n_der['Position Subname']==name)]
        a = a[a['Price']!=0]
             
        # Get units (only one futures positions per subposition)
        a = abs(a['Units']).max()
        
        # Get units           
        return a
        
    else:
        return float('nan')
    
# Get last value and units for closed futures
n_der['L_Fut_P'] = n_der.apply(lambda l: get_fut_p(l['Position Subname'],
                                                   l['Asset Class B'],
                                                   l['Date']), axis=1)
n_der['L_Fut_U'] = n_der.apply(lambda l: get_fut_u(l['Position Subname'],
                                                   l['Asset Class B'],
                                                   l['Closed']), axis=1)




## NAV for futures = Gain versus initial NAV
# Get first futures value
n_der['Fut_Cost'] = n_der['Units']*n_der['Strike']

# Calculate futures NAV
n_der.loc[(n_der['Asset Class B']=='Futures'), 'NAV'] = n_der['NAV']-n_der['Fut_Cost'] 

# Drop futures where the price = 0 and expiry was before the start of the year
n_der['YS'] = n_der['Date'].apply(lambda l: dt.date(l.year-1, 12, 31))
n_der = n_der[-((n_der['Asset Class B']=='Futures') &
                (n_der['Price']==0) &
                (n_der['Expiry']<n_der['YS']))]

# For futures that are closed but active within YTD, keep NAVs past expiry
# equal to the last NAV. In this way, the gains will be 0
n_der['L_Fut'] = n_der['L_Fut_P']*n_der['L_Fut_U']*n_der['Units per Contract']
n_der.loc[(n_der['Expiry']>n_der['YS'])&
      (n_der['Closed']=='Closed')&
      (n_der['Asset Class B']=='Futures'), 'NAV'] = n_der['L_Fut']-n_der['Fut_Cost']













### CALCULATIONS: PE FUNDS

# Find unique positions
subnames = list(set(n_pe['Position Subname']))

# Identify closed positions
n_pe['Closed'] = n_pe.apply(
    lambda x: p_closed(n_pe, x['Position Subname']), axis=1)

# Create dataframe with first date in each investment
calcs = {
    'Currency': lambda x: x.mode(),
    'Closed': lambda x: x.mode(),
    'Asset Class B': lambda x: x.mode(),
    'Date': 'max'
    }
x = n_pe.groupby(['Position Name', 'Position Subname']).agg(calcs).reset_index()
x['Vintage'] = x.apply(lambda a: first_inv(a['Position Subname'], 'Position Subname'), axis = 1)

# Choose last date for investment: either current date or year end of last nav date
x['Last Date'] = x.apply(lambda y:
                         dt.date(y['Date'].year, 12, 31) if y['Closed'] == 'Closed'
                         else end, axis=1)

# Create observations for each date in the chosen range    
y = []
for i in list(range(0,len(x))):
    
    # Get range of dates for each fund (Use double brackets so iloc returns df)
    a = (x.iloc[[i,]]).reset_index(drop=True)
    a['Vintage'].fillna(a['Last Date'], inplace=True) 
    p = pd.date_range(a['Vintage'][0],a['Last Date'][0],freq='M').tolist()
    p = pd.DataFrame({'Date': p})
    p['Date'] = p['Date'].apply(lambda x: x.date())
    
    # Add other fund info to new df
    p['Position Name'] = a['Position Name'][0]
    p['Position Subname'] = a['Position Subname'][0]
    p['Asset Class B'] = a['Asset Class B'][0]
    p['Currency'] = a['Currency'][0]
    p['Vintage'] = a['Vintage'][0]
    
    # Add into y
    y.append(p)                               

y = pd.concat(y, ignore_index=True)       

# Get last NAV reported for each date, as well as that date
def last_nav(name, date):
    x = n_pe[(n_pe['Position Subname']==name)
             & (n_pe['Date'] <= date)]
    x = x.sort_values(by='Date', ascending=False).reset_index(drop=True)
    return (float('NaN'), 0) if len(x)==0 else (x['Date'][0], x['NAV'][0])
    
y['Last NAV'] = y.apply(lambda l: last_nav(l['Position Subname'], l['Date'])[1]
                        ,axis=1)
y['Last NAV Date'] = y.apply(lambda l: last_nav(l['Position Subname'], l['Date'])[0]
                        ,axis=1)
y['Last NAV Date'].fillna(y['Vintage'], inplace=True)   

# Get flows since latest NAV
def flows_since(name, date, nav_date, ftype='all', ntype='Position Subname'):
    x = f_inv[(f_inv[ntype]==name)
             & (f_inv['Date'] <= date)
             & (f_inv['Date'] > nav_date)]
    if ftype=='all':
        return 0 if len(x)==0 else sum(x['Flow'])
    elif ftype=='call':
        return 0 if len(x)==0 else sum(x['Outflows'])
    elif ftype=='dist':
        return 0 if len(x)==0 else sum(x['Inflows'])
 
y['Flows Since'] = y.apply(lambda l: flows_since(name = l['Position Subname'],
                                                 date = l['Date'],
                                                 nav_date = l['Last NAV Date'])
                    ,axis=1)
  
# Calculate NAVs at each date
y['NAV'] = y.apply(lambda l: max(1, l['Last NAV']-l['Flows Since'])
                   if l['Last NAV'] == 1
                   else max(0, l['Last NAV']-l['Flows Since'])
                   , axis=1)
n_pe = y.copy()





### CALCULATIONS: CO-INVESTMENTS AND CUSTOM NAVS
### seems ok already






### CALCULATIONS: FX FORWARDS

# Buffer to add onto forward calcs, edit if Paolo asks
buffer = 0.002                   

# Create df for each date between initiation and expiry's year end or current date
y = []
for i in list(range(0,len(n_fxf))):
    
    # Get range of dates for each fx position (Use double brackets so iloc returns df)
    a = (n_fxf.iloc[[i,]]).reset_index(drop=True)
    p = pd.date_range(a['Date Initiated'][0],
                      min(end,
                          dt.date(a['Expiry'][0].year, 12, 31)),
                          freq='M').tolist()
    p = pd.DataFrame({'Date': p})
    p['Date'] = p['Date'].apply(lambda x: x.date())
    
    # Add other fund info to new df
    p['Position Name'] = a['Position Name'][0]
    p = p.merge(a, on = ['Position Name'], how='left')
    
    # Add into y
    y.append(p)                               
y = pd.concat(y, ignore_index=True)  

# Get forward points to use at each date
y['3M Fwd Points'] = y.apply(lambda l: get_price(l['Conversion'][0:7],
                                                 l['Date'], True), axis=1)
y['3M Fwd Points'] = y.apply(lambda l:
                             l['3M Fwd Points']/90*
                             np.max([(l['Expiry']-l['Date']).days,0])
                             -buffer
                             , axis = 1)

# Get relevant currency conversion at each date
y['Current FX Rate'] = y.apply(lambda l: get_price(l['Conversion'][0:7],
                                                 l['Date'], False), axis=1)

# Calculate Current FWD price
y['Current Fwd Price'] = y.apply(lambda l: l['Term Amount']/(l['Forward Price']-l['3M Fwd Points'])
                   if l['Expiry'] > l['Date']
                   else 0, axis =1)

# Calculate NAV
y['NAV_inc_Closed'] = -(y['Term Amount']/y['Current FX Rate']-y['Current Fwd Price'])
y['NAV'] = y.apply(lambda l: l['NAV_inc_Closed'] *
                   (0 if l['Date'] >= l['Expiry'] else 1), axis=1)
n_fxf = y

# Fill some columns for consistency
n_fxf['Currency'] = n_fxf['Currency A']
n_fxf['Asset Class B'] = 'FX Forwards'

# Identify closed positions
n_fxf['Closed'] = n_fxf.apply(lambda l:'Open' if (l['Date']<l['Expiry']) else 'Closed',
                              axis=1)

# Calculate the current estimated base value in the source currency
# For forwards
n_fxf['FX rate'] = n_fxf.apply(lambda l: get_fx(l['Currency B'], l['Date']),
                               axis =1)
n_fxf['Est. Val'] = n_fxf['NAV']+n_fxf['Term Amount']/n_fxf['FX rate']




### CALCULATIONS: FX OPTIONS

# Fill some columns for consistency
n_fxo['Currency'] = n_fxo['Currency B']   # different for Options as you get flows in Term Currency
n_fxo['Asset Class B'] = 'FX Options'

# Identify closed positions
n_fxo['Closed'] = n_fxo.apply(lambda l:'Open' if (l['Date']<l['Expiry']) else 'Closed',
                              axis=1)

# Calculate the current estimated base value in the source currency
n_fxo['FX rate'] = n_fxo.apply(lambda l: get_fx(l['Currency B'], l['Date']),
                               axis =1)
n_fxo['Est. Val'] = n_fxo['NAV']+n_fxo['Term Amount']/n_fxo['FX rate']












### COLLECT NAVS INTO POSITION OVERVIEW AND PERFORM RETURN MTD & YTD CALCS

# Prepare for spot price weighted average for FX aggregation
n_fxf['Term_Spot'] = n_fxf['Term Amount']*n_fxf['Spot Price']

# Aggregate FX forward NAVs
calcs = {
    'Term Amount': sum,
    'NAV': sum,
    'NAV_inc_Closed': sum,
    'Base Amount': sum,
    'Est. Val': sum,
    'Term_Spot': sum,
    'Asset Class B': lambda x: x.mode(),
    'Expiry': lambda x: x.mode()
    }
n_fxf_agg = n_fxf.groupby(['Date', 'Position Name', 'Position Subname',
                           'Closed', 'Currency B', 'Currency']).agg(calcs).reset_index()
n_fxf_agg['Forward Price'] = abs(n_fxf_agg['Term Amount']/n_fxf_agg['Base Amount'])
n_fxf_agg['Spot Price'] = n_fxf_agg['Term_Spot']/n_fxf_agg['Term Amount']

# Repeat but also separating by Buy/Sell => allows to display separation in FX Summary
n_fxf_agg2 = n_fxf.groupby(['Date', 'Position Name', 'Position Subname',
                           'Closed', 'Currency B', 'Currency', 'Action']).agg(calcs).reset_index()
n_fxf_agg2['Forward Price'] = abs(n_fxf_agg2['Term Amount']/n_fxf_agg2['Base Amount'])
n_fxf_agg2['Spot Price'] = n_fxf_agg2['Term_Spot']/n_fxf_agg2['Term Amount']



# Aggregate bank balance NAVs
calcs = {
    'Currency': lambda x: x.mode(),
    'NAV': sum,
    'Asset Class B': lambda x: x.mode()
    }
df_bal_agg = df_bal.groupby(['Date', 'Position Name', 'Position Subname']).agg(calcs).reset_index()


# Select NAV dataframes
y = ['n_bond', 'n_cust', 'n_der', 'n_eq', 'n_fxf_agg', 'n_fxo', 'n_pe', 'df_bal_agg']

# For each df, extract relevant columns and combine
x = []

for i in y:
    a = locals()[i]
    a = a[['Date', 'Position Name', 'Position Subname', 'Asset Class B',
           'Currency', 'NAV']]
    x.append(a)
x = pd.concat(x, ignore_index=True) 
x['NAV'].fillna(0, inplace=True)

# Add asset class A and drop prices that are not a position
x = x.merge(df_det[['Position Name', 'Asset Class A']],
            on = ['Position Name'], how = 'left')
x.dropna(subset = ['Asset Class A'], inplace = True)
x = x[x['Asset Class B'] != 'Benchmark']

# Drop observations before date that NAVs stop
x = x[x['Date']>=limit] 

# Find unique positions
subnames = list(set(x['Position Subname']))

# Add month and year start dates
x['M Start'] = x['Date'].apply(
    lambda l: l.replace(day=1)- dt.timedelta(days=1))
x['Y Start'] = x['Date'].apply(
    lambda l: l.replace(day=1, month=1)- dt.timedelta(days=1))

# Add month start NAV
a = x[['Date', 'Position Name', 'Position Subname', 'NAV']]
a.rename(columns={'Date': 'M Start', 'NAV': 'MS NAV'}, inplace = True)
x = x.merge(a, on = ['Position Name', 'Position Subname', 'M Start'],
                       how='left')
x['MS NAV'].fillna(0, inplace=True)
 
# Add year start NAV
a.rename(columns={'M Start': 'Y Start', 'MS NAV': 'YS NAV'}, inplace = True)
x = x.merge(a, on = ['Position Name', 'Position Subname', 'Y Start'],
                      how='left')
x['YS NAV'].fillna(0, inplace=True)
 
# Add MTD and YTD flows
a = f_mtd[f_mtd['Position Subname'].isin(subnames)]
x = x.merge(a, on = ['Position Name', 'Position Subname', 'Date'],
                      how='left')
x['MTD Flows'].fillna(0, inplace=True)

a = f_ytd[f_ytd['Position Subname'].isin(subnames)]
x = x.merge(a, on = ['Position Name', 'Position Subname', 'Date'],
                      how='left')
x['YTD Flows'].fillna(0, inplace=True)

# Exclude asset classes that do not need gain calcs
z = x[x['Asset Class A'] != 'Liquidity']

# Calculate MTD and YTD Gain
z['MTD Gain'] = z['NAV']-z['MS NAV']+z['MTD Flows']
z['YTD Gain'] = z['NAV']-z['YS NAV']+z['YTD Flows']

# Exclude asset classes that do not need % gain calcs
y = z.copy()
y = y[y['Asset Class A'] != 'Derivatives'] # Drop derivatives
y = y[y['Asset Class A'] != 'FX Hedging'] # Drop FX



## Prepare to calculate return %

# Add in units
y = y.merge(units, on = ['Position Name', 'Position Subname', 'Date'], how='left')
y['Units'] = y['Units'].fillna(0)

# Add in YS FX rate
y['YS FX rate'] = y.apply(lambda l: get_fx(l['Currency'], l['Date'], start = 'YS'),
                              axis=1)

# Get cumulative base value at the end of each month
y['CumBase'] = y.apply(lambda l: get_cumbase_sub(y,
                                                 l['Position Subname'],
                                                 l['Date'],
                                                 l['Y Start'],
                                                 l['YS NAV'],
                                                 l['Units'],
                                                 l['YTD Flows'],
                                                 l['YS FX rate']), axis=1)

# If position closed during the month, make base value equal to MS base value
# or YS NAV if January
a = y[['Date', 'Position Name', 'Position Subname', 'CumBase']]
a.rename(columns={'Date': 'M Start', 'CumBase': 'MS CumBase'}, inplace = True)
y = y.merge(a, on = ['Position Name', 'Position Subname', 'M Start'],
                       how='left')
y['MS CumBase'].fillna(0, inplace=True)

def fix_cumbase(date, cum, ms_cum, ys_nav):
    if cum != 0:
        return cum
    else:
        if date.month == 1:
            return ys_nav
        else:
            return ms_cum

y['CumBase'] = y.apply(lambda l: fix_cumbase(l['Date'],
                                             l['CumBase'],
                                             l['MS CumBase'],
                                             l['YS NAV'],), axis=1)



# Calculate MTD Gain %
y['MTD %'] = y.apply(
    lambda l: mtd_ret(f_inv, l['Position Subname'], l['MS NAV'], l['NAV'],
                      l['M Start'], l['Date']), axis=1)
y['MTD %'].fillna(0, inplace=True)

# Calculate YTD Gain %
y['YTD %'] = y.apply(
    lambda l: ytd_ret_w(y, l['Position Subname'], l['Y Start'], l['Date']), axis=1)

# Merge gross MTD and YTD back into main df
z = z.merge(y[['Date', 'Position Name', 'Position Subname', 'MTD %', 'YTD %', 'CumBase']],
            on = ['Date', 'Position Name', 'Position Subname'],
            how = 'left')

# Merge MTD % and YTD % back into main df
x = x.merge(z[['Date', 'Position Name', 'Position Subname',
               'MTD Gain', 'YTD Gain',
               'MTD %', 'YTD %', 'CumBase']],
            on = ['Date', 'Position Name', 'Position Subname'],
            how = 'left')



## Adust for futures calcs
# For futures, set NAV = 0 when price = NA or 0
# Merge derivative prices into df
x = x.merge(n_der[['Date', 'Position Name', 'Position Subname', 'Price']],
            on = ['Date', 'Position Name', 'Position Subname'],
            how = 'left')

# Replace selected NAVS with 0
x.loc[(x['Asset Class B']=='Futures') &
      ((x['Price']==0)|(x['Price']=='na')),
      'NAV'] = 0

# Drop price column
x = x.drop('Price', axis=1)

# Redo the adding of MS and YS NAVS to ensure the futures ones are correct
x = x.drop(['MS NAV', 'YS NAV'], axis=1)

# Add month start NAV
a = x[['Date', 'Position Name', 'Position Subname', 'NAV']]
a.rename(columns={'Date': 'M Start', 'NAV': 'MS NAV'}, inplace = True)
x = x.merge(a, on = ['Position Name', 'Position Subname', 'M Start'],
                       how='left')
x['MS NAV'].fillna(0, inplace=True)
 
# Add year start NAV
a.rename(columns={'M Start': 'Y Start', 'MS NAV': 'YS NAV'}, inplace = True)
x = x.merge(a, on = ['Position Name', 'Position Subname', 'Y Start'],
                      how='left')
x['YS NAV'].fillna(0, inplace=True)





## CONVERSON TO EUROS AND FX GAIN/LOSS CALC ON POSITIONS

# Get FX rate
x['FX rate'] = x.apply(lambda l: get_fx(l['Currency'], l['Date']), axis=1)
x['YS FX rate'] = x.apply(lambda l: get_fx(l['Currency'], l['Date'], start = 'YS')
                       , axis=1)
x['MS FX rate'] = x.apply(lambda l: get_fx(l['Currency'], l['Date'], start = 'MS')
                       , axis=1)

# Convert NAVs to Euros
x['NAV EUR'] = x['NAV']/x['FX rate']
#x['NAV EUR (YS FX)'] = x['NAV']/x['YS FX rate']

# Convert Gain to Euros using year/month start conversion (excluding FX effect)
x['MTD Gain EUR (MS FX)'] = x['MTD Gain']/x['MS FX rate']
x['YTD Gain EUR (YS FX)'] = x['YTD Gain']/x['YS FX rate']

# Get period-start NAV EUR
y = x
x['YS NAV EUR'] = x.apply(lambda l: get_pstartval(dataframe = y,
                                                  date = l['Date'],
                                                  subname = l['Position Subname'],
                                                  value_to_get = 'NAV EUR',
                                                  start ='YS'), axis=1)

x['MS NAV EUR'] = x.apply(lambda l: get_pstartval(dataframe = y,
                                                  date = l['Date'],
                                                  subname = l['Position Subname'],
                                                  value_to_get = 'NAV EUR',
                                                  start ='MS'), axis=1)

# Adjust NAV EUR to reflect ending FX (used for return calcs to ensure FX not included)
# Otherwise, month end and start NAVs use different FX
x['MS NAV EUR E_FX'] = x['MS NAV EUR']*x['MS FX rate']/x['FX rate']
x['YS NAV EUR E_FX'] = x['YS NAV EUR']*x['YS FX rate']/x['FX rate']


# Calculate FX gain/loss on each position
x['YTD FX Gain'] = (x['NAV EUR']-x['YS NAV EUR']+(x['YTD Flows']/x['FX rate'])    # EUR returns including FX
                    - x['YTD Gain']/x['YS FX rate'])                              # EUR returns excluding FX
x['MTD FX Gain'] = (x['NAV EUR']-x['MS NAV EUR']+(x['MTD Flows']/x['FX rate'])
                    - x['MTD Gain']/x['MS FX rate'])


# Liquidity FX gain/loss only relates to the initial period balance
# other FX gains/losses on flows are captured by the investment flows.
# Thus, fix FX Gains in liquidity rows

x['YTD FX Gain'][x['Asset Class A']=='Liquidity'] = (x['YS NAV']/x['FX rate']
                                                     - x['YS NAV']/x['YS FX rate'])
x['MTD FX Gain'][x['Asset Class A']=='Liquidity'] = (x['MS NAV']/x['FX rate']
                                                     - x['MS NAV']/x['MS FX rate'])


# Save in new df
o_sub = x.copy()
x = 'empty'
y = 'empty'
z = 'empty'



## IDENTIFY AND DROP CLOSED POSITIONS AND SUBPOSITIONS

# If NAV, ytd flows, and ytd gains are all 0 then closed
def closed_pos(nav, flows, ytd):
    if (abs(nav) + abs(flows) + abs(ytd))==0:
        return 'Closed'
    else:
        return ''
    
o_sub['Status'] = o_sub.apply(lambda l: closed_pos(l['NAV'],
                                                   l['YTD Flows'],
                                                   l['YTD Gain']), axis=1)


# Save df of currently closed positions
o_closed = o_sub[o_sub['Date']==end][['Position Name', 'Position Subname', 'Status']].drop_duplicates()

# Drop closed positions
o_sub = o_sub[o_sub['Status']!='Closed']


## Separate out EUR returns and NAVs for Tableau formatting
o_sub_eur = o_sub[['Date', 'Asset Class A', 'Asset Class B', 'Position Name', 'Position Subname',
                   'MTD Gain EUR (MS FX)', 'YTD Gain EUR (YS FX)', 'NAV EUR']]
o_sub_eur['Currency'] = 'EUR'
o_sub_eur['CCy_Type'] = 'Total'











### CHECK THAT ALL POSTION SUBNAMES FOR NAVS APPEAR IN THE FLOWS

# Define function to get list of subnames per position
def get_sub(df, posname):
    a = df[df['Position Name']==posname]
    a = a.sort_values(by=['Position Subname']).reset_index(drop=True)
    a = list(a['Position Subname'].unique())
    return a

# Create df of flows without closed subpositions
y = f_inv.merge(o_closed[['Position Subname', 'Status']],
                on = 'Position Subname', how='left')
y = y[y['Status']=='']

# Get list of subpositions per position from flows
x = pd.DataFrame(y['Position Name'].unique())
x.columns = ['Position Name']
x['Subnames Flows'] = x.apply(lambda l: get_sub(y, l['Position Name']),
                        axis=1)

# Get list of subpositions per position from NAVs
y = o_sub[o_sub['Date']==end]
x['Subnames NAVs'] = x.apply(lambda l: get_sub(y, l['Position Name']),
                        axis=1)

# Drop positions with empty lists ()
# x = x[x['Subnames NAVs'].map(lambda d: len(d)) > 0]

# Define checking function
x['Check'] = x.apply(lambda l: "" if l['Subnames Flows'] == l['Subnames NAVs'] else "Check", axis=1)

# Add Asset Class A and drop FX
x = x.merge(df_det[['Position Name', 'Asset Class A']],
            on = 'Position Name', how='left')
x = x[x['Asset Class A']!= 'FX Hedging']

# Keep only the ones to check
x = x[x['Check']== 'Check']

# Print warning
if len(x)>0:
    print('*** Warning! ***: The following Positions have inbalanced Subpositions:', list(x['Position Name']))
else:
    pass











### AGGREGATE FX HEDGING

# Switch back the Currency used for Options. Previously it was to match NAV and Flows
# Now the Currency refers to the Term Value
n_fxo['Currency'] = n_fxo['Currency A'] 

# Join the FX options and forwards dataframes
cols = ['Date', 'Asset Class B', 'Closed', 'Currency B', 'Term Amount',
        'Currency', 'NAV', 'Base Amount', 'Est. Val']
x = n_fxf_agg[cols].append(n_fxo[cols])

# Drop closed positions
x = x[x['Closed']=='Open']

# Aggregate once, keeping option, forward split
calcs = {
    'Term Amount': sum,
    'Base Amount': sum,
    'NAV': sum,
    'Est. Val': sum
    }
o_fx1 = x.groupby(['Date', 'Asset Class B', 'Currency B', 'Currency']).agg(calcs).reset_index()

# Aggregate again to get the total for the FX hedging asset class
calcs = {
    'Term Amount': sum,
    'Base Amount': sum,
    'NAV': sum,
    'Est. Val': sum
    }
o_fx2 = x.groupby(['Date', 'Currency B', 'Currency']).agg(calcs).reset_index()
o_fx2['Asset Class B'] = 'FX Hedging'



## For subpositions: replace FX local currency NAVs with Term Amount and Base Amount

# All FX entries in the o_sub dataframe are in Currency A (base currency).
# Thus, replace all current NAVs with their current estimated value

# Get all dates and FX subnames in the subpositions df
x = o_sub[o_sub['Asset Class A']=='FX Hedging'][['Date', 'Position Subname']].reset_index(drop=True)

# Get the current estimated and term values of options and forwards, and join them
a = ['Date', 'Position Name', 'Position Subname', 'Term Amount', 'Est. Val',
     'Closed', 'Currency B', 'Currency', 'Asset Class B']
y = n_fxf_agg[a].append(n_fxo[a])

# For each of the FX NAVs in the subpositions df, replace the NAV with the
# estimated value of the same subposition at the same date
n = range(0, len(x), 1)
for i in n:
    # Get date and position
    day = x['Date'][i]
    sub = x['Position Subname'][i]
    
    # Filter o_sub to get correct entry index to modify
    z = o_sub[(o_sub['Position Subname']==sub) &
              (o_sub['Date']==day)].reset_index()
    z = z['index'][0]
    
    # Filter the FX df to get the correct entry to replace with
    est = y[(y['Position Subname']==sub) &
          (y['Date']==day)][['Closed', 'Est. Val', 'Currency']].reset_index(drop=True)
    ccy = est['Currency'][0]
    
    # Deal with entries too old to be in o_sub, and deal with closed positions
    if est.empty:
        est = 0
    else:
        if est['Closed'][0] == 'Open':
            est = est['Est. Val'][0]
        else:
            est=0
    
    # Replace NAV in the o_sub df
    o_sub['NAV'][z] = est
    
    # Double check currencies are correct. Need to switch them round for FX_Options
    # because their flows are in the Term Currency
    o_sub['Currency'][z] = ccy



## Append term values with Currency B (the term currency) to o_sub

# Get Term Values and other data
x = y[['Date', 'Position Name','Position Subname',
                         'Currency B', 'Term Amount', 'Asset Class B', 'Closed']]

# Deal with closed positions
x['Term Amount'] = x.apply(lambda l: 0 if l['Closed']=='Closed' else -l['Term Amount'],
                           axis=1)

# Rename certain variables
x.rename(columns={'Currency B': 'Currency', 'Term Amount': 'NAV'}, inplace= True)

# Add Asset Class A and drop 'Closed'
x['Asset Class A'] = 'FX Hedging'
x = x.drop('Closed', axis=1)

# Remove options local currency gain to local sub df
# Add options local currency gain to Currency B df we're adding here
# This is done because the for loop above replaced the gains' curreny with Currency A
# but actually they're supposed to be in curreny B. Only Est. Val was uspposed to change to Currency A
a = o_sub[o_sub['Asset Class B']=='FX Options'][['Date', 'Position Subname', 'MTD Gain',
                                                 'YTD Gain', 'MTD Flows', 'YTD Flows']]

# Set previous values to 0 to avoid double counting
o_sub.loc[o_sub['Asset Class B']=='FX Options',['MTD Gain', 'YTD Gain', 'MTD Flows',
                                                'YTD Flows']] = 0

# Merge in
x = x.merge(a, on = ['Date', 'Position Subname'], how='left')


# Note the length of o_sub before the append, to identify the added rows later
n = len(o_sub)

# Append to o_sub
o_sub = o_sub.append(x)

# Identify rows of o_sub just appended
n = range(n, len(o_sub), 1)

# Make NaNs in new append equal to 0 to allow later calcs
o_sub.iloc[n] = o_sub.iloc[n].fillna(0)

# Add identifier to show that the values are in local currency
o_sub['CCy_Type'] = 'Local Currencies'

# Drop benchmarks
o_sub = o_sub[o_sub['Asset Class A']!='Benchmark']









### CALCULATE PERFORMANCE FOR OVERVIEW BY CLASS

# Calculate MTD EUR flows
o_sub['MTD Flows EUR (MS FX)'] = o_sub['MTD Flows']/o_sub['MS FX rate']
o_sub['YTD Flows EUR (YS FX)'] = o_sub['YTD Flows']/o_sub['YS FX rate']


# Assign overview classes to asset class Bs (some of them are aggregated)
x = o_sub.copy()
y = pd.read_excel(source2, sheet_name='ENEB')
x = x.merge(y[['Overview Class', 'Asset Class B']],
            on = 'Asset Class B', how='left')
x['Asset Class B'] = x['Overview Class']
    



## Local currency version

# Sum Local Currency NAVs, Flows, and Gains
calcs = {
    'NAV': sum,
    'MTD Gain': sum,
    'YTD Gain': sum
    }
o_classb_local = x.groupby(['Date', 'Asset Class B', 'Currency']).agg(calcs).reset_index()

# Add identifier to show that the values are in local currency
o_classb_local['CCy_Type'] = 'Local Currencies'



## EUR version

# Sum EUR values
calcs = {
    'NAV EUR': sum,
    'MS NAV EUR': sum,
    'YS NAV EUR': sum,
    'MS NAV EUR E_FX': sum,
    'YS NAV EUR E_FX': sum,
    'MTD Flows EUR (MS FX)': sum,
    'YTD Flows EUR (YS FX)': sum,
    'MTD Gain EUR (MS FX)': sum,
    'YTD Gain EUR (YS FX)': sum,
    'CumBase': sum,
    'M Start': lambda x: x.mode(),
    'Y Start': lambda x: x.mode(),
    'Asset Class A': lambda x: x.mode()
    }
x = x.groupby(['Date', 'Asset Class B']).agg(calcs).reset_index()
x['Currency']='EUR'

# Separate derivatives and FX
y = x[x['Asset Class B'] != 'Futures']
y = y[-y['Asset Class A'].isin(['FX Hedging'])]

# Create MTD and YTD % using aggregated monthly NAVs and flows
y['Position Subname'] = y['Asset Class B']
y['Flow'] = y['MTD Flows EUR (MS FX)']
y['MTD %'] = y.apply(lambda l: mtd_ret(y, l['Asset Class B'],
                                       l['MS NAV EUR E_FX'], l['NAV EUR'],
                                       l['M Start'], l['Date']), axis =1)

y['YTD %'] = y.apply(lambda l: ytd_ret_w(y, l['Asset Class B'],
                                       l['Y Start'], l['Date']), axis =1)

# Merge back into df with derivs and FX
x = x.merge(y[['Date', 'Asset Class B', 'MTD %', 'YTD %']],
            on = ['Date', 'Asset Class B'],
            how = 'left')
o_classb_eur = x.copy()

# Add identifier to show that the EUR values are aggregate
o_classb_eur['CCy_Type'] = 'Total'












  
    










### CALCULATE TOTAL PORTFOLIO PERFORMANCE


## TOP-DOWN APPROACH

# Calculate total portfolio NAV - local currencies
calcs = {
    'NAV': sum,
    'MTD Gain': sum,
    'YTD Gain': sum
    }
o_port_local = o_classb_local.groupby(['Date', 'Currency']).agg(calcs).reset_index()


# Calculate total portfolio NAV - EUR
calcs = {
    'NAV EUR': sum,
    'YS NAV EUR': sum,
    'MS NAV EUR': sum,
    'MTD Gain EUR (MS FX)': sum,
    'YTD Gain EUR (YS FX)': sum
    }
o_port_eur = o_classb_eur.groupby(['Date', 'Currency']).agg(calcs).reset_index()

# Get contributions to KSH
x = f_int[f_int['Position Name'] == 'Contributions to KSH']  

# Convert to EUR
x['FX rate'] = x.apply(lambda l: get_fx(l['Currency'], end),
                       axis=1)
x['Flow'] = x['Flow']/x['FX rate']

# Change to last day in month to allow merge
x['Date'] = x['Date'].apply(lambda l: pd.Period(l,freq='M').end_time.date())

# Merge contributions into the dataframe and give a value of 0 contributions to NAs
o_port_eur = o_port_eur.merge(x[['Date', 'Flow']], on=['Date'], how ='left')
o_port_eur['Flow'].fillna(0, inplace=True)
o_port_eur.rename(columns={'Flow': 'MTD Contributions to KSH'}, inplace = True)

# Calculate YTD contributions by summing contributions in the year so far
o_port_eur['Year']=o_port_eur['Date'].apply(lambda l: l.year)
o_port_eur['YTD Contributions to KSH']= o_port_eur.apply(lambda l: o_port_eur[(o_port_eur['Year']==l['Year'])
                                      & (o_port_eur['Date']<=l['Date'])]['MTD Contributions to KSH'].sum(), axis=1)

# Calculate portfolio gains from the NAVs and contributions
# Includes FX effects: conversions, gains on asset, and hedging
# These are the top-down gains that the bottom up approach then has to match!
o_port_eur['MTD Gain'] = o_port_eur['NAV EUR']-o_port_eur['MS NAV EUR']-o_port_eur['MTD Contributions to KSH']
o_port_eur['YTD Gain'] = o_port_eur['NAV EUR']-o_port_eur['YS NAV EUR']-o_port_eur['YTD Contributions to KSH']

o_port_eur['MTD Gain %']=o_port_eur['MTD Gain']/o_port_eur['MS NAV EUR']
o_port_eur['YTD Gain %']=o_port_eur['YTD Gain']/o_port_eur['YS NAV EUR']






## BOTTOM-UP APPROACH TO PORTFOLIO GAINS

## The EUR gain above sums up individual investment gains and excludes FX and expenses
## But includes FX hedging, because it is one of the subpositions included
## Below I derive the gains from the NAVs, which includes FX and expenses

## Now I calculate the FX impact to add onto the gain derived from individual investments 

# Get FX hedging return per month and year
x = o_classb_eur[o_classb_eur['Asset Class B']=='FX Hedging'][['Date', 'MTD Gain EUR (MS FX)', 'YTD Gain EUR (YS FX)']]
x.rename(columns ={'MTD Gain EUR (MS FX)': 'MTD FX Hedging Gain', 'YTD Gain EUR (YS FX)': 'YTD FX Hedging Gain'},
         inplace=True)
o_port_eur = o_port_eur.merge(x, on=['Date'], how ='left')

# Exclude FX hedging from investment gains to avoid double counting (hedging was one of the positions)
o_port_eur['MTD Gain ex FX']=o_port_eur['MTD Gain EUR (MS FX)']-o_port_eur['MTD FX Hedging Gain']
o_port_eur['YTD Gain ex FX']=o_port_eur['YTD Gain EUR (YS FX)']-o_port_eur['YTD FX Hedging Gain']

# Calculate portfolio % returns exc. FX
o_port_eur['MTD Gain % ex FX']=o_port_eur['MTD Gain ex FX']/o_port_eur['MS NAV EUR']
o_port_eur['YTD Gain % ex FX']=o_port_eur['YTD Gain ex FX']/o_port_eur['YS NAV EUR']

# Select observations beyond limit
o_port_eur = o_port_eur[o_port_eur['Date']>limit].reset_index(drop=True)



## NET CURRENCY IMPACT CALCULATION
## 1) Calculate the Gain/Loss on conversions

# Clean Conversions sheet
f_conv.drop(labels=['Notes'], axis=1, inplace=True)
f_conv=f_conv[f_conv['Date']>limit]

# Create the exchange rate used
f_conv['FX Name'] = f_conv.apply(lambda l: '-'.join(l[['Outflow Currency', 'Inflow Currency']]),axis=1)

# Define function that calculates the FX gain/loss on past conversions
def conv_gain(date):   
    # Select conversions that occurred within the reference timeframe and get the FX rates for date
    a = f_conv[(f_conv['Date']>dt.date(date.year-1, 12, 31))
               & (f_conv['Date']<= min(dt.date(date.year, 12, 31), date))]
    
    # 0 if there have not yet been any conversions during the year
    if (len(a)==0):
        return 0

    else:    
        a['Current FX rate'] = a.apply(lambda l: get_fx2(l['FX Name'], date), axis =1)
        
        # Calculate gain/loss on the conversion in terms of the inflow currency
        a['Gain'] = a['Inflows']/a['Current FX rate']-a['Inflows']/a['Exchange Rate']
        
        # Convert the gain/loss to euros
        a['Gain Currency'] = a['Outflow Currency']
        a['Convert Gain FX'] = a.apply(lambda l: 1 if (l['Gain Currency']=='EUR') else 
                                       (l['Current FX rate'] if (l['Inflow Currency']=='EUR') else
                                        get_fx2('-'.join([l['Outflow Currency'], 'EUR']), date))
                                        ,axis=1)
        a['Gain'] = a['Gain']*a['Convert Gain FX']
        
        # Sum gains
        #print(a[['Date', 'Gain', 'Gain Currency', 'Convert Gain FX', 'Current FX rate']])
        a = a['Gain'].sum()
        return a

# Calculate the YTD gains and losses on conversions during the year
o_port_eur['YTD FX Conversion Gain']=o_port_eur['Date'].apply(lambda l: conv_gain(l))

# Calculate the MTD gains and losses by subtracting the YTD gain from the YTD gain at month start
o_port_eur['M Start'] = o_port_eur['Date'].apply(lambda l: l.replace(day=1)- dt.timedelta(days=1))
o_port_eur['Y Start'] = o_port_eur['Date'].apply(lambda l: dt.date(l.year-1, 12, 31))
o_port_eur['MTD FX Conversion Gain']=(o_port_eur['YTD FX Conversion Gain']
                                      - o_port_eur['M Start'].apply(lambda l: conv_gain(l)))



## 2) Calculate total FX Gain/Loss on foreign assets

# Define a function that sums the period FX gains previously calculated for subpositions
def fx_ass(date, period):
    
    # Select obs at date
    a = o_sub[o_sub['Date']==date]
    
    # Sum FX gains
    name = period + ' FX Gain'
    a = sum(a[name])
    return a

o_port_eur['YTD FX Gain on Assets']=o_port_eur['Date'].apply(lambda l: fx_ass(l, 'YTD'))    
o_port_eur['MTD FX Gain on Assets']=o_port_eur['Date'].apply(lambda l: fx_ass(l, 'MTD'))    



## 3) Calculate the Net Currency Impact
o_port_eur['YTD Net FX Impact'] = (o_port_eur['YTD FX Conversion Gain']
                                   + o_port_eur['YTD FX Gain on Assets']
                                   + o_port_eur['YTD FX Hedging Gain'])
o_port_eur['MTD Net FX Impact'] = (o_port_eur['MTD FX Conversion Gain']
                                   + o_port_eur['MTD FX Gain on Assets']
                                   + o_port_eur['MTD FX Hedging Gain'])







## CALCULATE EXPENSES

# Create copy of original expenses
o_exp = f_exp.copy()

# Assign expenses to their month end
f_exp['M End'] = f_exp['Date'].apply(lambda l: pd.Period(l,freq='M').end_time.date())

# Group expenses by type
calcs = {'Outflows': sum,
         'Position Subname': lambda l: l.mode()}
f_exp = f_exp.groupby(['M End', 'Subname Lower', 'Currency']).agg(calcs).reset_index()

# Convert expenses to EUR at current FX rate (for comparability)
f_exp['FX'] = f_exp['Currency'].apply(lambda l: get_fx(l, end))
f_exp['Outflows EUR'] = f_exp['Outflows']/f_exp['FX']

# Group expenses again to summarise into one currency
calcs = {'Outflows EUR': sum,
         'Position Subname': lambda l: l.mode()}
f_exp_eur = f_exp.groupby(['M End', 'Subname Lower']).agg(calcs).reset_index()



# Create expense dataframe to display in Tableau

# Convert expenses to EUR at current FX rate (for comparability)
o_exp['FX'] = o_exp['Currency'].apply(lambda l: get_fx(l, end))
o_exp['Outflows EUR'] = o_exp['Outflows']/o_exp['FX']

# Create df with both individual and EUr expenses
x = o_exp[['Date', 'Bank', 'Position Subname', 'Currency', 'Outflows']]
y = o_exp[['Date', 'Bank', 'Position Subname', 'Outflows EUR']]
y['Currency'] = 'Total (EUR)'
y.rename(columns ={'Outflows EUR': 'Outflows'}, inplace=True)
x = x.append(y)

# Add Vehicle
x.rename(columns={'Bank': 'Custody'}, inplace=True)
y = df_det[['Vehicle', 'Custody']].drop_duplicates()
x = x.merge(y, on='Custody', how='left')

# Identify MTD, YTD and Last 5Y
b = dt.date(year-5, month, day_num-2)
x['5Y'] = x['Date'].apply(lambda l: 'Yes' if l > b else float('nan'))

## Identify expense groups
# Expense groups
exp_groups = {'EA':'Estate Advisor', 'GT': 'Grant Thornton', 'MGB': 'MGB Capital'}
exp_fintax = ['Interest', 'interest', 'Trust', 'trust','Financ', 'financ',
      'earnout', 'Earnout', 'Tax', 'tax']

# Define function for table groups
def id_exp(name):   
    
    # If name part of a group identified, allocate that group
    if name.split(" ")[0] in list(exp_groups.keys()):
        
        # Allocate group name to selected variables than need a group
        for i in list(exp_groups.keys()):
            if i in name:
                return exp_groups[i]
        
    # Allocate Other to Other variables
    elif 'Other' in name:
        return 'Other'
    
    # Allocate to general finance group
    elif any(l in name for l  in exp_fintax):
        return 'Financial & Trust Charges'
    
    # Remaining variables have their own group
    else:
        return 'Operational Expenses'
    
# Define function for graph groups
def id_exp_g(name):   
    
    # If name part of a group identified, allocate that group
    if name.split(" ")[0] in list(exp_groups.keys()):
        
        # Allocate group name to selected variables than need a group
        for i in list(exp_groups.keys()):
            if i in name:
                return exp_groups[i]
        
    # Allocate Other to Other variables
    elif 'Other' in name:
        return 'Other'
    
    # Otherwise return given name
    else:
        return name

# Apply function
x['Expense Group'] = x['Position Subname'].apply(lambda l: id_exp(l))        
x['Expense Group_G'] = x['Position Subname'].apply(lambda l: id_exp_g(l))   


# Split and append total values
y = x[x['Currency']=='Total (EUR)']
y['Currency'] = ' EUR'
y['CCy_Type'] = 'Total'
x['CCy_Type'] = 'Local Currencies'
x = x[x['Currency']!='Total (EUR)'].append(y)


# Save
o_exp = x.copy()









## CALCULATE FULL YTD RETURN FROM GAINS (RATHER THAN FROM NAVS)

# Calculate portfolio returns including FX
o_port_eur['YTD Returns inc. FX Impact'] = (o_port_eur['YTD Gain ex FX']
                                            + o_port_eur['YTD Net FX Impact']                                            )

o_port_eur['MTD Returns inc. FX Impact'] = (o_port_eur['MTD Gain ex FX']
                                            + o_port_eur['MTD Net FX Impact'])


# Calculate MTD and YTD expenses to subtract
def tot_exp(date, period):
    start = (date.replace(day=1)- dt.timedelta(days=1)) if (period =='MTD') else dt.date(date.year-1, 12, 31)
    a = f_exp_eur[(f_exp_eur['M End']>start)&(f_exp_eur['M End']<=date)]
    a = a['Outflows EUR'].sum()
    return a

o_port_eur['MTD Expenses'] = o_port_eur['Date'].apply(lambda l: tot_exp(l, 'MTD'))
o_port_eur['YTD Expenses'] = o_port_eur['Date'].apply(lambda l: tot_exp(l, 'YTD'))

# Calculate returns from gains
o_port_eur['YTD Returns 2'] = o_port_eur['YTD Returns inc. FX Impact']-o_port_eur['YTD Expenses']
o_port_eur['MTD Returns 2'] = o_port_eur['MTD Returns inc. FX Impact']-o_port_eur['MTD Expenses']

# Check it matches with the NAV method for the current month
x = o_port_eur[o_port_eur['Date']==end]['YTD Returns 2']-o_port_eur[o_port_eur['Date']==end]['YTD Gain']
x = x.reset_index()[0]
x = pd.to_numeric(x)

if (abs(x[0]) < 0.005):
    print('Great! YTD Gains calculated bottom-up and top-down match.')
else:
    print('Wait.. YTD Gains differ by ', x)

  


## ASSIGN CURRENCY TYPE TO PORTFOLIO DFS
o_port_local['CCy_Type'] = 'Local Currencies'
o_port_eur['CCy_Type'] = 'Total'









### CALCULATE PORTFOLIO ALLOCATIONS FOR EACH SUBPOSITION

# Add portfolio NAVs to o_sub dfs
y = o_port_eur[['Date', 'NAV EUR']]
y.rename(columns={'NAV EUR': 'Portfolio NAV'}, inplace=True)
o_sub_eur = o_sub_eur.merge(y, on = 'Date', how='left')
o_sub = o_sub.merge(y, on = 'Date', how='left')

# Calculate allocations
o_sub_eur['Allocation'] = o_sub_eur['NAV EUR']/o_sub_eur['Portfolio NAV']
o_sub['Allocation'] = o_sub['NAV EUR']/o_sub['Portfolio NAV']











### CALCULATE PERFORMANCE BY POSITION NAME

# Sum subname positions NAVs, gains and flows by position name and date
# Both local currency and EUR
calcs = {
    
    # Dates
    'Y Start': lambda l: l.mode(),
    'M Start': lambda l: l.mode(),
    
    # Local
    'NAV': sum,
    'YS NAV': sum,
    'MS NAV': sum,
    'MTD Gain': sum,
    'YTD Gain':sum,
    'MTD Flows': sum,
    'YTD Flows':sum,
    
    # EUR
    'NAV EUR': sum,
    'YS NAV EUR': sum,
    'MS NAV EUR': sum,
    'MS NAV EUR E_FX': sum,
    'YS NAV EUR E_FX': sum,
    'CumBase': sum,
    'MTD Gain EUR (MS FX)': sum,
    'YTD Gain EUR (YS FX)': sum,
    'MTD Flows EUR (MS FX)': sum,
    'YTD Flows EUR (YS FX)':sum,
    'Allocation': sum
    }
x = o_sub.groupby(['Date', 'Asset Class A', 'Asset Class B', 'Position Name', 'Currency']).agg(calcs).reset_index()


# Separate derivatives and FX and liquidity
y = x[-x['Asset Class A'].isin(['Derivatives'])]
y = y[y['Asset Class A'] != 'FX Hedging']
y = y[y['Asset Class A'] != 'Liquidity']

# Create EUR MTD and YTD % using aggregated EUR monthly NAVs and flows
y['Position Subname'] = y['Position Name']
y['Flow'] = y['MTD Flows EUR (MS FX)']
y['MTD %'] = y.apply(lambda l: mtd_ret(y, l['Position Name'],
                                       l['MS NAV EUR E_FX'], l['NAV EUR'],
                                       l['M Start'], l['Date']), axis =1)

y['YTD % EUR'] = y.apply(lambda l: ytd_ret_w(y, l['Position Name'],
                                       l['Y Start'], l['Date']), axis =1)
y.rename(columns={'MTD %': 'MTD % EUR'}, inplace=True)

# Create local MTD and YTD % using aggregated local monthly NAVs and flows
y['Flow'] = y['MTD Flows']
y['MTD %'] = y.apply(lambda l: mtd_ret(y, l['Position Name'],
                                       l['MS NAV'], l['NAV'],
                                       l['M Start'], l['Date']), axis =1)

y['YTD %'] = y.apply(lambda l: ytd_ret_w(y, l['Position Name'],
                                       l['Y Start'], l['Date']), axis =1)


# Merge back into df with derivs and FX
x = x.merge(y[['Date', 'Position Name', 'MTD %', 'YTD %', 'MTD % EUR', 'YTD % EUR']],
            on = ['Date', 'Position Name'],
            how = 'left')

o_pos = x.copy()
o_pos['CCy_Type'] = 'Local Currencies'


# Calculate contributions to RoI for both positions and asset classes
x = o_port_eur[['Date', 'YTD Gain ex FX', 'YTD Gain % ex FX']]
x = x.rename(columns={'YTD Gain ex FX': 'Portfolio YTD Gain', 'YTD Gain % ex FX': 'Portfolio YTD Gain %'})
o_pos = o_pos.merge(x, on = 'Date', how ='left')
o_pos['RoI Contribution'] = o_pos['YTD Gain EUR (YS FX)']/o_pos['Portfolio YTD Gain']*o_pos['Portfolio YTD Gain %']*10000
o_classb_eur = o_classb_eur.merge(x, on = 'Date', how ='left')
o_classb_eur['RoI Contribution'] = o_classb_eur['YTD Gain EUR (YS FX)']/o_classb_eur['Portfolio YTD Gain']*o_classb_eur['Portfolio YTD Gain %']*10000


# Separate out EUR returns and NAVs for Tableau formatting
o_pos_eur = o_pos[['Date', 'Asset Class A', 'Asset Class B', 'Position Name',
                   'MTD Gain EUR (MS FX)', 'YTD Gain EUR (YS FX)', 'NAV EUR',
                   'RoI Contribution']]
o_pos_eur['Currency'] = 'EUR'
o_pos_eur['CCy_Type'] = 'Total'





### ADD IN POSITION DETAILS INFORMATION

# Separate out Position, Vehicle, Custody, and Rank
x = df_det[['Position Name', 'Vehicle', 'Custody']]

# Merge them into the subposition dfs
o_sub = o_sub.merge(x, on = 'Position Name', how = 'left')
o_sub_eur = o_sub_eur.merge(x, on = 'Position Name', how = 'left')

# Merge them into the positions dfs
o_pos = o_pos.merge(x, on = 'Position Name', how = 'left')
o_pos_eur = o_pos_eur.merge(x, on = 'Position Name', how = 'left')

# Get asset class ranks
#ranks = df_det[['Asset Class B', 'Rank']].drop_duplicates()

# Merge them into the positions dfs
#o_classb_local = o_classb_local.merge(ranks, on = 'Asset Class B', how = 'left')
#o_classb_eur = o_classb_eur.merge(ranks, on = 'Asset Class B', how = 'left')










### CALCULATE PARTNERS CAP NAV AND RETURNS

# Separate out PC subpositions
x = o_sub[o_sub['Custody']=='PC-HSBC']

# Calculate total PC NAV - local currencies
calcs = {
    'NAV': sum,
    'MTD Gain': sum,
    'YTD Gain': sum
    }
y = x.groupby(['Date', 'Currency']).agg(calcs).reset_index()

# Create and add currency container to latch on in order to fill the table in Tableau
a = o_classb_local['Currency'].unique()
b = o_classb_local['Date'].unique()
z = pd.DataFrame(list(product(b,a)),
                 columns=['Date', 'Currency'])

z['NAV'] = 0
z['YTD Gain'] = 0
z['MTD Gain'] = 0

# Save
y = y.append(z)
y['CCy_Type'] = 'Local Currencies'
pc = y.copy()



# Calculate total PC NAV - EUR
calcs = {
    'NAV EUR': sum,
    'MS NAV EUR': sum,
    'YS NAV EUR': sum,
    'MS NAV EUR E_FX': sum,
    'YS NAV EUR E_FX': sum,
    'CumBase': sum,
    'MTD Flows EUR (MS FX)': sum,
    'YTD Flows EUR (YS FX)': sum,
    'MTD Gain EUR (MS FX)': sum,
    'YTD Gain EUR (YS FX)': sum,
    'M Start': lambda x: x.mode(),
    'Y Start': lambda x: x.mode(),
    'Custody': lambda x: x.mode(),
    'Allocation': sum
    }
pc_eur = x.groupby(['Date']).agg(calcs).reset_index()
pc_eur['Currency']='EUR'

# Create MTD and YTD % using aggregated monthly NAVs and flows
y = pc_eur.copy()
y['Position Subname'] = y['Custody']
y['Flow'] = y['MTD Flows EUR (MS FX)']
y['MTD %'] = y.apply(lambda l: mtd_ret(y, l['Custody'],
                                       l['MS NAV EUR E_FX'], l['NAV EUR'],
                                       l['M Start'], l['Date']), axis =1)

y['YTD %'] = y.apply(lambda l: ytd_ret_w(y, l['Custody'],
                                       l['Y Start'], l['Date']), axis =1)

# Merge back in
y = y[['Date', 'MTD %', 'YTD %']]
pc_eur = pc_eur.merge(y, on = ['Date'], how = 'left')
pc_eur['CCy_Type'] = 'Total'


# Calculate contributions to RoI
x = o_port_eur[['Date', 'YTD Gain ex FX', 'YTD Gain % ex FX']]
x = x.rename(columns={'YTD Gain ex FX': 'Portfolio YTD Gain', 'YTD Gain % ex FX': 'Portfolio YTD Gain %'})
pc_eur = pc_eur.merge(x, on = 'Date', how ='left')
pc_eur['RoI Contribution'] = pc_eur['YTD Gain EUR (YS FX)']/pc_eur['Portfolio YTD Gain']*pc_eur['Portfolio YTD Gain %']*10000










### CALCULATE KSH AND KTS RETURNS


# Calculate total KSH AND KTS NAVs- local currencies
calcs = {
    'NAV': sum,
    'MTD Gain': sum,
    'YTD Gain': sum
    }
o_vehicle = o_sub.groupby(['Date', 'Vehicle', 'Currency']).agg(calcs).reset_index()

o_vehicle['CCy_Type'] = 'Local Currencies'
o_vehicle['Total Type'] = 'Vehicle Total'

# Calculate total KSH and KTS NAVs - EUR
calcs = {
    'NAV EUR': sum,
    'MS NAV EUR': sum,
    'YS NAV EUR': sum,
    'MS NAV EUR E_FX': sum,
    'YS NAV EUR E_FX': sum,
    'MTD Flows EUR (MS FX)': sum,
    'YTD Flows EUR (YS FX)': sum,
    'MTD Gain EUR (MS FX)': sum,
    'YTD Gain EUR (YS FX)': sum,
    'M Start': lambda x: x.mode(),
    'Y Start': lambda x: x.mode(),
    'Allocation': sum
    }
o_vehicle_eur = o_sub.groupby(['Date', 'Vehicle']).agg(calcs).reset_index()
o_vehicle_eur['Currency']='EUR'

# Create MTD % (no need for position by position build up due to the large numbers involved)
o_vehicle_eur['MTD %'] = o_vehicle_eur['MTD Gain EUR (MS FX)']/o_vehicle_eur['MS NAV EUR']

# Create YTD % (need to rename the identifying column for the function to work)
o_vehicle_eur['Position Subname'] = o_vehicle_eur['Vehicle']
o_vehicle_eur = o_vehicle_eur[o_vehicle_eur['Date']>limit]
o_vehicle_eur['YTD %'] = o_vehicle_eur.apply(lambda l: ytd_ret(o_vehicle_eur, l['Vehicle'],
                                       l['Y Start'], l['Date']), axis =1)

# Identify as being totals
o_vehicle_eur['CCy_Type'] = 'Total'
o_vehicle_eur['Total Type'] = 'Vehicle Total'




## Remove FX hedging returns from the returns by vehicle: because we want to see the Gain/Loss ex FX on investments

# Get FX hedging returns by date and by Vehicle
x = o_sub[o_sub['Asset Class A']=='FX Hedging']
x = x[['Date', 'Vehicle', 'Currency', 'MTD Gain', 'YTD Gain']]
x = x.groupby(['Date', 'Vehicle', 'Currency']).agg(sum).reset_index()
x[['MTD Gain', 'YTD Gain']] = x[['MTD Gain', 'YTD Gain']].fillna(0)

# Get FX hedging return for the whole portfolio (aggregate by date)
y = x.groupby(['Date', 'Currency']).agg(sum).reset_index()

# Create returns ex-FX by adding the FX hedging to the vehicle portfolio dfs, and and back to gains 
x.rename(columns={'MTD Gain': 'MTD FX Gain', 'YTD Gain': 'YTD FX Gain'}, inplace=True)
o_vehicle = o_vehicle.merge(x, on =['Date', 'Vehicle', 'Currency'], how='left')
o_vehicle[['MTD FX Gain', 'YTD FX Gain']] = o_vehicle[['MTD FX Gain', 'YTD FX Gain']].fillna(0)
o_vehicle['MTD Gain ex FX'] = o_vehicle['MTD Gain']-o_vehicle['MTD FX Gain']
o_vehicle['YTD Gain ex FX'] = o_vehicle['YTD Gain']-o_vehicle['YTD FX Gain']

y.rename(columns={'MTD Gain': 'MTD FX Gain', 'YTD Gain': 'YTD FX Gain'}, inplace=True)
o_port_local = o_port_local.merge(y, on =['Date', 'Currency'], how='left')
o_port_local[['MTD FX Gain', 'YTD FX Gain']] = o_port_local[['MTD FX Gain', 'YTD FX Gain']].fillna(0)
o_port_local['MTD Gain ex FX'] = o_port_local['MTD Gain']-o_port_local['MTD FX Gain']
o_port_local['YTD Gain ex FX'] = o_port_local['YTD Gain']-o_port_local['YTD FX Gain']


# Do the same for total returns
x = o_sub_eur[o_sub_eur['Asset Class A']=='FX Hedging']
x = x[['Date', 'Vehicle', 'MTD Gain EUR (MS FX)', 'YTD Gain EUR (YS FX)']]
x = x.groupby(['Date', 'Vehicle']).agg(sum).reset_index()

x.rename(columns={'MTD Gain EUR (MS FX)': 'MTD FX Gain', 'YTD Gain EUR (YS FX)': 'YTD FX Gain'}, inplace=True)
o_vehicle_eur = o_vehicle_eur.merge(x, on =['Date', 'Vehicle'], how='left')
o_vehicle_eur[['MTD FX Gain', 'YTD FX Gain']] = o_vehicle_eur[['MTD FX Gain', 'YTD FX Gain']].fillna(0)
o_vehicle_eur['MTD Gain ex FX'] = o_vehicle_eur['MTD Gain EUR (MS FX)']-o_vehicle_eur['MTD FX Gain']
o_vehicle_eur['YTD Gain ex FX'] = o_vehicle_eur['YTD Gain EUR (YS FX)']-o_vehicle_eur['YTD FX Gain']


# Already present in o_port_eur, calculated earlier

# Calculate RoI Contributions (ex FX) by Vehicle
x = o_port_eur[['Date', 'YTD Gain ex FX', 'YTD Gain % ex FX']]
x = x.rename(columns={'YTD Gain ex FX': 'Portfolio YTD Gain', 'YTD Gain % ex FX': 'Portfolio YTD Gain %'})
o_vehicle_eur = o_vehicle_eur.merge(x, on = 'Date', how ='left')
o_vehicle_eur['RoI Contribution'] = o_vehicle_eur['YTD Gain ex FX']/o_vehicle_eur['Portfolio YTD Gain']*o_vehicle_eur['Portfolio YTD Gain %']*10000

# Calculate YTD % ex FX by vehicle
o_vehicle_eur['MTD Gain % ex FX'] = o_vehicle_eur['MTD Gain ex FX']/o_vehicle_eur['MS NAV EUR']
o_vehicle_eur['YTD Gain % ex FX'] = o_vehicle_eur['YTD Gain ex FX']/o_vehicle_eur['YS NAV EUR']

















### CALCULATE ASSET CLASS RETURNS SPLIT BY VEHICLE (KSH AND KTS)

# Assign overview classes to asset class Bs (some of them are aggregated)
x = o_sub.copy()
y = pd.read_excel(source2, sheet_name='ENEB')
x = x.merge(y[['Overview Class', 'Asset Class B']],
            on = 'Asset Class B', how='left')
x['Asset Class B'] = x['Overview Class']
    


## Local currency version
# Sum Local Currency NAVs, Flows, and Gains
calcs = {
    'NAV': sum,
    'MTD Gain': sum,
    'YTD Gain': sum
    }
y = x.groupby(['Date', 'Vehicle', 'Asset Class B', 'Currency']).agg(calcs).reset_index()

# Create and add currency container to latch on in order to fill the table in Tableau
a = y['Currency'].unique()
b = y['Asset Class B'].unique()
d = y['Vehicle'].unique()
c = y['Date'].unique()
z = pd.DataFrame(list(product(c, a, b, d)),
                 columns=['Date', 'Currency', 'Asset Class B', 'Vehicle'])

z['NAV'] = 0
z['YTD Gain'] = 0
z['MTD Gain'] = 0

y = y.append(z)

# Add identifier to show that the values are in local currency and that they are class totals
y['CCy_Type'] = 'Local Currencies'
y['Total Type'] = 'Class Total'

# Add to vehicle df 
o_vehicle = o_vehicle.append(y)



## EUR version

# Replace NAVs and Flows for Derivatives, with just the gains
x.loc[x['Asset Class A'].isin(['Derivatives']), 'NAV EUR'] = x.loc[x['Asset Class A'].isin(['Derivatives']), 'MTD Gain EUR (MS FX)']
x.loc[x['Asset Class A'].isin(['Derivatives']), ['MTD Flows EUR (MS FX)', 'MS NAV EUR E_FX']] = 0
x[['NAV EUR', 'MTD Flows EUR (MS FX)', 'MS NAV EUR E_FX']] = x[['NAV EUR', 'MTD Flows EUR (MS FX)', 'MS NAV EUR E_FX']].fillna(0)


# Sum EUR values
calcs = {
    'NAV EUR': sum,
    'MS NAV EUR': sum,
    'YS NAV EUR': sum,
    'MS NAV EUR E_FX': sum,
    'YS NAV EUR E_FX': sum,
    'CumBase': sum,
    'MTD Flows EUR (MS FX)': sum,
    'YTD Flows EUR (YS FX)': sum,
    'MTD Gain EUR (MS FX)': sum,
    'YTD Gain EUR (YS FX)': sum,
    'M Start': lambda x: x.mode(),
    'Y Start': lambda x: x.mode(),
    'Asset Class A': lambda x: x.mode(),
    'Allocation': sum
    }
x = x.groupby(['Date', 'Vehicle', 'Asset Class B']).agg(calcs).reset_index()
x['Currency']='EUR'

# Separate derivatives and FX
y = x[x['Asset Class B'] != 'Futures']
y = y[-y['Asset Class A'].isin(['FX Hedging'])]

# Create MTD and YTD % using aggregated monthly NAVs and flows
y['Position Subname'] = y['Asset Class B']
y['Flow'] = y['MTD Flows EUR (MS FX)']

y['MTD %'] = y.apply(lambda l: mtd_ret(y, l['Asset Class B'],
                                       l['MS NAV EUR E_FX'], l['NAV EUR'],
                                       l['M Start'], l['Date'],
                                       subset = 'Vehicle',
                                       subset_name = l['Vehicle']), axis =1)

y['YTD %'] = y.apply(lambda l: ytd_ret_w(y, l['Asset Class B'],
                                       l['Y Start'], l['Date'],
                                       subset = 'Vehicle',
                                       subset_name = l['Vehicle']), axis =1)

# Merge back into df with derivs and FX
x = x.merge(y[['Date', 'Vehicle', 'Asset Class B', 'MTD %', 'YTD %']],
            on = ['Date', 'Vehicle', 'Asset Class B'],
            how = 'left')

# Add identifier to show that the EUR values are aggregate
x['CCy_Type'] = 'Total'
x['Total Type'] = 'Class Total'

# Calculate contributions to RoI by asset classes
a = o_port_eur[['Date', 'YTD Gain ex FX', 'YTD Gain % ex FX']]
a = a.rename(columns={'YTD Gain ex FX': 'Portfolio YTD Gain', 'YTD Gain % ex FX': 'Portfolio YTD Gain %'})
x = x.merge(a, on = 'Date', how ='left')
x['RoI Contribution'] = x['YTD Gain EUR (YS FX)']/x['Portfolio YTD Gain']*x['Portfolio YTD Gain %']*10000

# Add to vehicle df 
o_vehicle_eur = o_vehicle_eur.append(x)
























### CALCULATE GEOGRAPHICAL EXPOSURE

# Get position geographies
x = df_det.columns[df_det.columns.str.contains('eography')].tolist()
x.append('Position Name')
x = df_det[x]

# Drop positions that don't have a geography
x = x[x['Geography 1'].notna()]

# Merge into position NAVs
n = ['Date', 'Asset Class A', 'Asset Class B', 'Position Name', 'NAV EUR', 'RoI Contribution']
y = o_pos_eur[n].merge(x, on='Position Name', how = 'left')

# Pivot into a longer position with a new row for each geography
o_geo = []
p = list(range(0,len(x.columns)-1, 2))     # Get indices of each new geography number

for i in p:
    # Keep the indices of columns to identify each position + date + NAV
    z = list(range(0,len(n)))
    
    # Add the indices of the desired geography columns
    geo = len(n)+i
    geo_pct = len(n)+i+1
    
    z.append(geo)
    z.append(geo_pct)
    
    # Subset the desired columns
    x = y.iloc[:,z]
    
    # Rename the geography columns
    x.rename(columns = {x.columns[len(n)]: 'Geography', x.columns[len(n)+1]: 'Geography %'},
             inplace= True)
    
    # Stick together to form longer table
    o_geo.append(x)
o_geo = pd.concat(o_geo, ignore_index=True) 

# Get NAV allocated to each geography
o_geo['Geography NAV'] = o_geo['NAV EUR']*o_geo['Geography %']

# Get performance contribution allocated to each geography
o_geo['Performance Contribution (bps)'] = o_geo['RoI Contribution']*o_geo['Geography %']

# Drop positions without geography (FX, cash, and positions that are too old)
o_geo = o_geo[o_geo['Geography'].notna()]

# Summarise by geography and date
calcs = {
    'Geography NAV': sum,
    'Performance Contribution (bps)': sum
    }
o_geo_agg = o_geo.groupby(['Date', 'Geography']).agg(calcs).reset_index()

# Calculate total NAV per date, and consequently the % per geography
o_geo_agg['Tot NAV'] = o_geo_agg['Geography NAV'].groupby(o_geo_agg['Date']).transform('sum')
o_geo_agg['% of Investments'] =  o_geo_agg['Geography NAV']/o_geo_agg['Tot NAV']

# Calculate the % gain by geography
o_geo_agg['YTD RoI'] = o_geo_agg['Performance Contribution (bps)']/o_geo_agg['% of Investments']/10000














### PRIVATE EQUITY PROFILES (not historical)

# Get private investment positions at current date (not historical)
x = o_pos[(o_pos['Asset Class A'] == 'Private Investments')
          & (o_pos['Date'] == end)]

## Add closed private investments

# Get list of investments to add
y = o_closed[o_closed['Status']=='Closed']
z = list(df_det[df_det['Asset Class A']=='Private Investments']['Position Name'])
y = y[(y['Position Name'].isin(z)) & -(y['Position Name'].isin(x['Position Name']))]

# Add details
y = y.merge(df_det[['Position Name', 'Asset Class A', 'Asset Class B', 'Vehicle',
                    'Custody']],
            on = 'Position Name', how ='left')
y[['NAV', 'NAV EUR']] = 0
y['CCy_Type'] = 'Local Currencies'
y['Date'] = end
y[['Y Start', 'M Start']] = 'Local Currencies'

def get_currency(name):
    n = f_inv[f_inv['Position Name']==name]
    n = n['Currency'].mode()
    return n

y['Currency'] = y['Position Name'].apply(lambda l: get_currency(l))

# Add to df
x = x.append(y)
x = x.drop(['Position Subname'], axis=1)
x = x.fillna(0)




# Get vintages
x['Vintage'] = x.apply(lambda l: first_inv(l['Position Name'], 'Position Name'), axis=1)
    
# Get original commitment from position details
y = df_det[['Position Name', 'PE Commitment']]
x = x.merge(y, on = 'Position Name', how = 'left')
x = x[x['PE Commitment']!='<Drop>']

# If no commitment given, sum the investment flows
def get_totflow(name):
    n = f_inv[f_inv['Position Name']==name]
    n = n['Outflows'].sum()
    return n   

x['FlowSum'] = x['Position Name'].apply(lambda l: get_totflow(l))
x['PE Commitment'] = x['PE Commitment'].fillna(x['FlowSum'])


## Calculate remaining commitments
# Get data from RemCom sheet
y = pe_com[['Date', 'Position Name', 'Position Subname', 'Currency', 'Remaining Commitment']]

# Rename
y.rename(columns={'Date': 'Latest RC Date', 'Remaining Commitment': 'Latest RC'}, inplace = True)

# Identify and keep the latest entries
y['Latest'] = y['Latest RC Date'].groupby(y['Position Subname']).transform('max')
y['Latest'] = y.apply(lambda l: 'Yes' if l['Latest']==l['Latest RC Date'] else 'No', axis=1)

# Drop duplicates (if I entered the same latest commitment multiple times)
y['PosCur'] = y['Position Subname']+y['Currency']
y = y.drop_duplicates(subset='PosCur', keep='last')

# Get calls since the last date
y['Calls Since'] = y.apply(lambda l: flows_since(name = l['Position Subname'],
                                                 date = end,
                                                 nav_date = l['Latest RC Date'],
                                                 ftype = 'call')
                    ,axis=1)

# Aggregate by date and position name
calcs = {
    'Currency': lambda x: x.mode(),
    'Latest RC': 'sum',
    'Calls Since': 'sum'
    }
y  = y.groupby('Position Name').agg(calcs).reset_index()


# Merge into PE position data
try:
    x = x.merge(y, on = ['Position Name', 'Currency'], how = 'left')
except (TypeError):
    currencies = f_inv['Currency'].unique()
    a = y[-y['Currency'].isin(currencies)]['Position Name']
    print('Error: Mismatched flow and NAV currencies in the following positions: ', a)

# If there is no remaining commitment, assume its the initial commitment
x['Latest RC'] = x['Latest RC'].fillna(x['PE Commitment'])

# Check all funds have a registered flow
a = list(x[x['Vintage'].isna()]['Position Name'])
if len(a)>0:
    print("The following fund(s) do not have a flow registered:", a,
          "New commitments should be given a flow of 0 so that they are tracked.")
else:
    pass

# Positions that do not have remcoms: called since = called to date
x['Called to Date'] = x.apply(lambda l: flows_since(name = l['Position Name'],
                                                 date = end,
                                                 nav_date = l['Vintage']- dt.timedelta(days=1),
                                                 ftype = 'call',
                                                 ntype = 'Position Name'),axis=1)
x['Calls Since'] = x['Calls Since'].fillna(x['Called to Date'])

# Set remaining commitments to 0 for co-investments and closed positions
x['Latest RC'][x['Asset Class B']=='Co-Investments']=0
x.loc[x['Status']=='Closed', 'Latest RC'] = 0

# Estimate current latest commitment, with min=0
x['Remaining Commitment'] = x.apply(lambda l: np.max([(l['Latest RC']-l['Calls Since']),0]),
                                    axis=1)



# Calculate Distributed to Date
x['Distributed to Date'] = x.apply(lambda l: flows_since(name = l['Position Name'],
                                                 date = end,
                                                 nav_date = l['Vintage']- dt.timedelta(days=1),
                                                 ftype = 'dist',
                                                 ntype = 'Position Name'), axis=1)

# Calculate MOIC and DPI
x['MOIC'] = (x['NAV']+x['Distributed to Date'])/x['Called to Date']
x['DPI'] = x['Distributed to Date']/x['Called to Date']

# Calculate IRR
def get_irr(name, nav, p_end, name_type='Position Name', fx_mix=False):
    
    # Get relevant flows
    a = f_inv[(f_inv[name_type] == name)]    
    a = a[['Date', 'Currency', 'Flow']]
    
    # Get Current FX rate and convert to EUR if there are a mix of FX
    if fx_mix:
        a['FX rate'] = a.apply(lambda l: get_fx(l['Currency'], end), axis=1)
        a['Flow'] = a['Flow']/a['FX rate']
    else:
        pass
    
    # Create and sort dataframe with NAVs and flows of the month
    a = a.append(pd.DataFrame({'Date': [p_end],
                  'Flow': [nav], 'Currency': ['EUR']}),
                 ignore_index = True)
    a = a.sort_values(by=['Date']).reset_index(drop=True)
   
    # Remove last entry for positions that have closed to avoid IRR elongation    
    if a['Flow'].iloc[-1] == 0:
        a = a[:-1]

    try:
        if ((len(set(a['Date'])) > 1)
            & (sum(1 for number in a['Flow'] if number < 0) > 0)    # at least 1 negative and postitive number
            & (sum(1 for number in a['Flow'] if number > 0) > 0)):
            irr = xirr(a['Date'], a['Flow'])
            return irr
        else:
            return None      # No result for positions with no monthly numbers
    except (TypeError):
        return 0         # NaN for things like derivatives that do not have % returns

x['IRR'] = x.apply(lambda l: get_irr(l['Position Name'],
                                     l['NAV'],
                                     end), axis=1)


# Drop positions with no commitment (the provisions) and save
x = x[-x['PE Commitment'].isna()]
o_pe = x.copy()




## ADD IN LAST NAV RECORDED, ITS DATE, AND FLOWS SINCE

# Get last NAV date and last NAV for custom positions
# Get last NAV dates
x = n_cust[(n_cust['Date']==end)][['Date', 'Position Name', 'Position Subname', 'Last Updated']]
x.rename(columns={'Last Updated': 'Last NAV Date'}, inplace=True)


# Get flows since then
x['Flows Since'] = x.apply(lambda l: flows_since(l['Position Subname'],
                                                 l['Date'],
                                                 l['Last NAV Date']), axis=1)

# Get current NAVs
y = o_sub[o_sub['Date']==end][['Position Subname', 'Date', 'NAV']]
x = x.merge(y, on=['Date', 'Position Subname'], how='left')

# Get NAV at last date by subtracting flows since from current NAV
# Missing values, mean the last update was pre-the python position
x['Last NAV'] = x['NAV']-x['Flows Since']


# Get last NAVs from other PE positions
y = list(x.columns)
y = n_pe[n_pe['Date']==end][y]

# Merge the two
x = x.append(y)

# Sum to position level
calcs = {'Last NAV': sum,
         'Flows Since': sum,
         'Last NAV Date': min}
x = x.groupby('Position Name').agg(calcs).reset_index()

# Add to PE df
o_pe = o_pe.merge(x, on=['Position Name'], how='left')


# Make PE status type consistent
o_pe['Status'] = o_pe['NAV'].apply(lambda l: 'Closed' if l==0 else 'Open')




## CREATE SUMMARY PERFORMANCE AND STATS FOR PRIVATE INVESTMENT CATEGORIES

# Get unique PE categories
n = o_pe['Asset Class B'].unique()

# Convert Remaining Commitment to EUR
o_pe['FX rate'] = o_pe.apply(lambda l: get_fx(l['Currency'], end), axis=1)
o_pe['RemCom EUR'] = o_pe['Remaining Commitment']/o_pe['FX rate']

# Sum remaining commitment, Current NAV, YTD Gain
calcs = {'RemCom EUR': sum,
         'NAV EUR': sum,
         'YTD Gain EUR (YS FX)': sum}
o_pe_agg = o_pe.groupby('Asset Class B').agg(calcs).reset_index()
o_pe_agg['Currency'] = 'EUR'
o_pe_agg.rename(columns = {'RemCom EUR': 'Remaining Commitment'})

# From investment flows df: Called to date, Distributed to date
calcs = {'Outflows': sum,
         'Inflows': sum}
x = f_inv.groupby(['Asset Class B', 'Currency']).agg(calcs).reset_index()
x = x[x['Asset Class B'].isin(n)]

# Convert flows to EUR
x['FX rate'] = x.apply(lambda l: get_fx(l['Currency'], end), axis=1)
x['Outflows'] = x['Outflows']/x['FX rate']
x['Inflows'] = x['Inflows']/x['FX rate']

# Aggregate and add back into pe df
x = x.groupby('Asset Class B').agg({'Outflows': sum, 'Inflows':sum}).reset_index()
x = o_pe_agg.merge(x, on = 'Asset Class B', how = 'left')

# MOIC and DPI
x['MOIC'] = (x['NAV EUR']+x['Inflows'])/x['Outflows']
x['DPI'] = x['Inflows']/x['Outflows']
o_pe_agg = x.copy()

# Get YTD % from asset b df
x = o_classb_eur[(o_classb_eur['Date']==end) &
                 (o_classb_eur['Asset Class B'].isin(n))][['Asset Class B',
                                                           'YTD %']]
o_pe_agg = o_pe_agg.merge(x, on = 'Asset Class B', how = 'left')
                                                           
# Recalculate IRR
o_pe_agg['IRR'] = o_pe_agg.apply(lambda l: get_irr(l['Asset Class B'],
                                                   l['NAV EUR'],
                                                   end,
                                                   name_type='Asset Class B',
                                                   fx_mix=True), axis=1)











### HEDGE FUND PROFILES (not historical)

# Get private investment positions at current date (not historical)
x = o_pos[(o_pos['Asset Class A'] == 'Hedge Funds')
          & (o_pos['Date'] == end)]

# Calculate cost basis
x['Cost'] = x.apply(lambda l: av_cost(l['Position Name'], "total", 'Position Name'),
                    axis=1)

# Get current units
calcs = {'Units': 'sum'}
y = units.groupby(['Position Name','Date']).agg(calcs).reset_index()
y = x.merge(y, on = ['Position Name', 'Date'], how ='left')

# Keep necessary columns
a = ['Asset Class A', 'Asset Class B', 'Position Name', 'Currency', 'NAV',
     'YTD Gain', 'Cost', 'Units']
y = y[a]

# Datatype identifier to separate from other HF data
y['Datatype'] = 'Details'
o_hf = y.copy()
o_hf = o_hf.reset_index(drop=True)



## Hedge fund % performance

# Create month df container to latch on in order to fill the table in Tableau
a = {'Period': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Sep', 'Oct', 'Nov', 'Dec']}
z = pd.DataFrame(a)
z['Performance Type'] = 'MTD'
z['Year'] = year
z['Asset Class A'] = 'Hedge Funds'
z['Position Name'] = o_hf['Position Name'][0]
z['Asset Class B'] = o_hf['Asset Class B'][0]
z['Currency'] = o_hf['Currency'][0]


## For subpositions
# Get historical data: we need past performance here
y = o_sub[o_sub['Asset Class A'] == 'Hedge Funds']

# Get units
y = y.merge(units, on = ['Position Name', 'Position Subname', 'Date'], how ='left')

# Calculate NAV/share
y['NAV_psh'] = y['NAV']/y['Units']

# Get month start NAV/sh
x = y[['Date', 'Position Subname', 'NAV_psh']]
x.rename(columns = {'Date': 'M Start', 'NAV_psh': 'MS NAV_psh'}, inplace=True)
y = y.merge(x, on = ['M Start', 'Position Subname'], how='left')

# Calculate MTD peformance
y['MTD %'] = y['NAV_psh']/y['MS NAV_psh']-1

# Calculate YTD performance
y['YTD %'] = y.apply(lambda l: ytd_ret(y, l['Position Subname'], l['Y Start'], l['Date']),
                     axis=1)

# Keep necessary columns for MTD % and identify them as MTD + month name
a = ['Date',  'Currency', 'Asset Class A', 'Asset Class B', 'Position Name', 'Position Subname', 'MTD %']
x = y[a]
x.rename(columns = {'MTD %': 'Performance'}, inplace = True)
x['Period'] = pd.DatetimeIndex(x['Date']).strftime("%b")
x['Year'] = pd.DatetimeIndex(x['Date']).strftime("%Y")
x['Performance Type'] = 'MTD'

# Keep necessary columns for YTD % and identify them as YTD
a = ['Date',  'Currency', 'Asset Class A', 'Asset Class B', 'Position Name', 'Position Subname', 'YTD %']
y = y[a]
y['Performance Type'] = 'YTD'
y.rename(columns = {'YTD %': 'Performance'}, inplace = True)
y['Month'] = pd.DatetimeIndex(y['Date']).strftime("%m")
y['Period'] = pd.DatetimeIndex(y['Date']).strftime("%Y")
y['Year'] = y['Period']

# Keep only the current YTD %, and other year-ends
y = y[((y['Date']==end)|(y['Month']== '12'))&(pd.to_numeric(y['Period'])>2021)]

# Join the MTD and YTD
y = y.append(x)
y = y.append(z)

# Datatype identifier to separate from other HF data
y['Datatype'] = 'Performance - Subpositions'

hf_sub = y.copy()




## For positions
# Get historical data: we need past performance here
y = o_pos[o_pos['Asset Class A'] == 'Hedge Funds']

# Get units
calcs = {'Units': 'sum'}
a = units.groupby(['Position Name','Date']).agg(calcs).reset_index()
y = y.merge(a, on = ['Position Name', 'Date'], how ='left')

# Calculate NAV/share
y['NAV_psh'] = y['NAV']/y['Units']

# Get month start NAV/sh
x = y[['Date', 'Position Name', 'NAV_psh']]
x.rename(columns = {'Date': 'M Start', 'NAV_psh': 'MS NAV_psh'}, inplace=True)
y = y.merge(x, on = ['M Start', 'Position Name'], how='left')

# Calculate MTD peformance
y['MTD %'] = y['NAV_psh']/y['MS NAV_psh']-1

# Calculate YTD performance
y['YTD %'] = y.apply(lambda l: ytd_ret(y, l['Position Name'], l['Y Start'], l['Date'], 'Position Name'),
                     axis=1)

# Keep necessary columns for MTD % and identify them as MTD + month name
a = ['Date',  'Currency', 'Asset Class A', 'Asset Class B', 'Position Name', 'MTD %']
x = y[a]

x.rename(columns = {'MTD %': 'Performance'}, inplace = True)
x['Period'] = pd.DatetimeIndex(x['Date']).strftime("%b")
x['Year'] = pd.DatetimeIndex(x['Date']).strftime("%Y")

x['Performance Type'] = 'MTD'


# Keep necessary columns for MTD % and identify them as YTD
a = ['Date',  'Currency', 'Asset Class A', 'Asset Class B', 'Position Name', 'YTD %']
y = y[a]
y['Performance Type'] = 'YTD'
y.rename(columns = {'YTD %': 'Performance'}, inplace = True)
y['Month'] = pd.DatetimeIndex(y['Date']).strftime("%m")
y['Period'] = pd.DatetimeIndex(y['Date']).strftime("%Y")
y['Year'] = y['Period']

# Keep only the current YTD %, and other year-ends
y = y[((y['Date']==end)|(y['Month']== '12'))&(pd.to_numeric(y['Period'])>2021)]

# Join the MTD and YTD
y = y.append(x)
y = y.append(z)

# Datatype identifier to separate from other HF data
y['Datatype'] = 'Performance - Positions'

hf_pos = y.copy()



## ADD BENCHMARK PERFORMANCE
# Get benchmarks from equity sheet
y = n_eq.copy()
y['Asset Class B'] = y['Asset Class B'].fillna('Drop') # can't do the next line if the column contains NAs
x = y[y['Asset Class B'].str.contains("Benchmark")]

# Keep desired columns and Asset Class A
x = x[['Date', 'Asset Class B', 'Position Name', 'Currency', 'Price']]
x['Asset Class A'] = 'Benchmark'

# Add month and year start dates
x['M Start'] = x['Date'].apply(
    lambda l: l.replace(day=1)- dt.timedelta(days=1))
x['Y Start'] = x['Date'].apply(
    lambda l: l.replace(day=1, month=1)- dt.timedelta(days=1))

# Get month start price
y = x[['Date', 'Position Name', 'Price']]
y.rename(columns = {'Date': 'M Start', 'Price': 'MS Price'}, inplace=True)
x = x.merge(y, on = ['M Start', 'Position Name'], how='left')

# Calculate MTD peformance
x['MTD %'] = x['Price']/x['MS Price']-1

# Calculate YTD performance
x['YTD %'] = x.apply(lambda l: ytd_ret(x, l['Position Name'], l['Y Start'], l['Date'], 'Position Name'),
                     axis=1)

# Separate out the HF group the benchmark relates to
x['Asset Class B'] = x.apply(lambda l: l['Asset Class B'].split(' - ')[1], axis=1)

# Keep necessary columns for MTD % and identify them as MTD + month name
a = ['Date', 'Currency', 'Asset Class A', 'Asset Class B', 'Position Name', 'MTD %', 'Price']
y = x[a]

y.rename(columns = {'MTD %': 'Performance'}, inplace = True)
y['Period'] = pd.DatetimeIndex(y['Date']).strftime("%b")
y['Year'] = pd.DatetimeIndex(y['Date']).strftime("%Y")
y['Performance Type'] = 'MTD'

# Keep necessary columns for MTD % and identify them as YTD
a = ['Date', 'Currency', 'Asset Class A', 'Asset Class B', 'Position Name', 'YTD %', 'Price']
x = x[a]
x['Performance Type'] = 'YTD'
x.rename(columns = {'YTD %': 'Performance'}, inplace = True)
x['Month'] = pd.DatetimeIndex(x['Date']).strftime("%m")
x['Period'] = pd.DatetimeIndex(x['Date']).strftime("%Y")
x['Year'] = x['Period']

# Keep only the current YTD %, and other year-ends
x = x[((x['Date']==end)|(x['Month']== '12'))&(pd.to_numeric(x['Period'])>2021)]

# Change the Month container to have Benchmark characteristics
x = x.reset_index(drop=True)
z['Asset Class A'] = 'Benchmark'
z['Position Name'] = x['Position Name'][4]
z['Asset Class B'] = x['Asset Class B'][4]
z['Currency'] = x['Currency'][4]

# Join the MTD and YTD
x = x.append(y)
x = x.append(z)

# Datatype identifier to separate from other HF data and save
x['Datatype'] = 'Benchmark'
bmrks = x.copy()



## Create the aggregated hf dataframe
y = o_hf.append(hf_sub)
y = y.append(hf_pos)
y = y.append(x)
o_hf = y.copy()
del hf_pos, hf_sub

# Fill NAs with 0
a = ['Units', 'Cost', 'NAV', 'YTD Gain']
o_hf[a] = o_hf[a].fillna(0)

# Replace infinites with NA
o_hf.loc[o_hf['Performance'] == -inf, 'Performance'] = float('nan')
o_hf.loc[o_hf['Performance'] == inf, 'Performance'] = float('nan')



## HEDGE FUND AVERAGES AND AGGREGATES

## Create weighted mean by Asset Class B
# Weighted mean by asset class = MTD and YTD % performance already calculated by Asset Class B
x = o_classb_eur[o_classb_eur['Asset Class A'].isin(['Hedge Funds'])]

# Get month and year
x['Month'] = pd.DatetimeIndex(x['Date']).strftime("%b")
x['Year'] = pd.to_numeric(pd.DatetimeIndex(x['Date']).strftime("%Y"))

# Create MTD dataset
y = x[['Date', 'Month', 'Year', 'Asset Class A', 'Asset Class B', 'Currency', 'MTD %']]
y['Period'] = y['Month']
y['Datatype'] = 'Weighted Average'
y['Performance Type'] = 'MTD'
y.rename(columns = {'MTD %': 'Performance'}, inplace=True)

# Create YTD dataset and keep only the current YTD and relevant past year ends
x = x[['Date', 'Month', 'Year', 'Asset Class A', 'Asset Class B', 'Currency', 'YTD %']]
x['Period'] = x['Year']
x['Datatype'] = 'Weighted Average'
x['Performance Type'] = 'YTD'
x.rename(columns = {'YTD %': 'Performance'}, inplace=True)
x = x[((x['Date']==end)|(x['Month']== '12'))&(pd.to_numeric(x['Period'])>2021)]

# Add to hedge fund df
o_hf = o_hf.append(x)
o_hf = o_hf.append(y)




## Create Simple mean by Asset Class B

# Identify closed positions
x = o_hf[(o_hf['NAV']==0)&(o_hf['Asset Class A']!='Benchmark')&(o_hf['Datatype']=='Details')]
x = list(x['Position Name'])
o_hf['Status'] = o_hf['Position Name'].apply(lambda l: "Closed" if l in x else "")

# Simple mean by Asset Class B = average performance by month and by asset class b (group by)
# MTD %
x = o_hf[(o_hf['Performance Type']=='MTD')&
         (o_hf['Datatype'] == 'Performance - Positions')&
         (o_hf['Asset Class A']!='Benchmark')&
         (o_hf['Performance']!=0)&
         (o_hf['Status']=="")]      # This filters out duplicates in aggregates and closed positions
calcs = {'Performance': 'mean',
         'Performance Type': lambda x: x.mode()}
x = x.groupby(['Date', 'Asset Class A', 'Asset Class B']).agg(calcs).reset_index()
x['Datatype'] = 'Simple Average'
x['Month'] = pd.DatetimeIndex(x['Date']).strftime("%b")
x['Year'] = pd.to_numeric(pd.DatetimeIndex(x['Date']).strftime("%Y"))
x['Period'] = x['Month']
x['Performance Type'] = 'MTD'
o_hf = o_hf.append(x)

# YTD %
x = o_hf[(o_hf['Performance Type']=='YTD')&
         (o_hf['Datatype'] == 'Performance - Positions')&
         (o_hf['Asset Class A']!='Benchmark')&
         (o_hf['Performance']!=0)&
         (o_hf['Status']=="")]      # This filters out duplicates in aggregates and closed positions
calcs = {'Performance': 'mean',
         'Performance Type': lambda x: x.mode()}
x = x.groupby(['Date', 'Asset Class A', 'Asset Class B']).agg(calcs).reset_index()
x['Datatype'] = 'Simple Average'
x['Month'] = pd.DatetimeIndex(x['Date']).strftime("%b")
x['Year'] = pd.to_numeric(pd.DatetimeIndex(x['Date']).strftime("%Y"))
x['Period'] = x['Year']
x['Performance Type'] = 'YTD'
o_hf = o_hf.append(x)



## Create simple mean for HF portfolio
# Simple mean for portfolio = average performance by month 
# MTD %
x = o_hf[(o_hf['Performance Type']=='MTD')&
         (o_hf['Datatype'] == 'Performance - Positions') &
         (o_hf['Asset Class A']!='Benchmark')&
         (o_hf['Performance']!=0) &
         (o_hf['Status']=="")]      # This filters out duplicates in aggregates
calcs = {'Performance': 'mean',
         'Performance Type': lambda x: x.mode()}
x = x.groupby(['Date']).agg(calcs).reset_index()
x['Asset Class B'] = 'Total Hedge Fund Portfolio'
x['Datatype'] = 'Simple Average'
x['Month'] = pd.DatetimeIndex(x['Date']).strftime("%b")
x['Year'] = pd.to_numeric(pd.DatetimeIndex(x['Date']).strftime("%Y"))
x['Period'] = x['Month']
x['Performance Type'] = 'MTD'
o_hf = o_hf.append(x)

# YTD %
x = o_hf[(o_hf['Performance Type']=='YTD')&
         (o_hf['Datatype'] == 'Performance - Positions') &
         (o_hf['Asset Class A']!='Benchmark')&
         (o_hf['Performance']!=0) &
         (o_hf['Status']=="")]      # This filters out duplicates in aggregates
calcs = {'Performance': 'mean',
         'Performance Type': lambda x: x.mode()}
x = x.groupby(['Date']).agg(calcs).reset_index()
x['Asset Class B'] = 'Total Hedge Fund Portfolio'
x['Datatype'] = 'Simple Average'
x['Month'] = pd.DatetimeIndex(x['Date']).strftime("%b")
x['Year'] = pd.to_numeric(pd.DatetimeIndex(x['Date']).strftime("%Y"))
x['Period'] = x['Year']
x['Performance Type'] = 'YTD'
o_hf = o_hf.append(x)



## Calculate weighted mean for portfolio

# Separate out hedge fund subpositions
x = o_sub[o_sub['Asset Class A']=='Hedge Funds']

# Calculate total NAVs in EUR
calcs = {
    'NAV EUR': sum,
    'MS NAV EUR': sum,
    'YS NAV EUR': sum,
    'MS NAV EUR E_FX': sum,
    'YS NAV EUR E_FX': sum,
    'CumBase': sum,
    'MTD Flows EUR (MS FX)': sum,
    'YTD Flows EUR (YS FX)': sum,
    'MTD Gain EUR (MS FX)': sum,
    'YTD Gain EUR (YS FX)': sum,
    'M Start': lambda x: x.mode(),
    'Y Start': lambda x: x.mode(),
    'Asset Class A': lambda x: x.mode()
    }
x = x.groupby(['Date']).agg(calcs).reset_index()
x['Currency']='EUR'

# Create MTD and YTD % using aggregated monthly NAVs and flows
y=x.copy()
y['Position Subname'] = y['Asset Class A']
y['Flow'] = y['MTD Flows EUR (MS FX)']
y['MTD %'] = y.apply(lambda l: mtd_ret(y, l['Asset Class A'],
                                       l['MS NAV EUR E_FX'], l['NAV EUR'],
                                       l['M Start'], l['Date']), axis =1)

y['YTD %'] = y.apply(lambda l: ytd_ret_w(y, l['Asset Class A'],
                                       l['Y Start'], l['Date']), axis =1)

# Merge back in
y = y[['Date', 'MTD %', 'YTD %']]
x = x.merge(y, on = ['Date'], how = 'left')

# Get month and year
x['Month'] = pd.DatetimeIndex(x['Date']).strftime("%b")
x['Year'] = pd.to_numeric(pd.DatetimeIndex(x['Date']).strftime("%Y"))

# Create MTD dataset
y = x[['Date', 'Month', 'Year', 'Asset Class A', 'Currency', 'MTD %']]
y['Period'] = y['Month']
y['Datatype'] = 'Weighted Average'
y['Performance Type'] = 'MTD'
y['Asset Class B'] = 'Total Hedge Fund Portfolio'
y.rename(columns = {'MTD %': 'Performance'}, inplace=True)

# Create YTD dataset and keep only the current YTD and relevant past year ends
x = x[['Date', 'Month', 'Year', 'Asset Class A', 'Currency', 'YTD %']]
x['Period'] = x['Year']
x['Datatype'] = 'Weighted Average'
x['Performance Type'] = 'YTD'
x['Asset Class B'] = 'Total Hedge Fund Portfolio'
x.rename(columns = {'YTD %': 'Performance'}, inplace=True)
x = x[((x['Date']==end)|(x['Month']== '12'))&(pd.to_numeric(x['Period'])>2021)]

# Add to hedge fund dataframe
o_hf = o_hf.append(x)
o_hf = o_hf.append(y)




## Get aggregated NAVs and Gains
# Get current NAVs and YTD Gain by Asset Class at current date
x = o_sub[(o_sub['Asset Class A']=='Hedge Funds')
          & (o_sub['Date']==end)]
calcs = {
    'NAV EUR': sum,
    'YTD Gain EUR (YS FX)': sum,
     'Asset Class A': lambda x: x.mode()
    }
x = x.groupby(['Date', 'Asset Class B']).agg(calcs).reset_index()
x['Currency']='EUR'
x['Datatype']='Weighted Average'
x.rename(columns = {'NAV EUR': 'NAV', 'YTD Gain EUR (YS FX)': 'YTD Gain'}, inplace=True)
o_hf = o_hf.append(x)

# Calculate NAV and YTD gain for hedge fund portfolio
x = o_sub[(o_sub['Asset Class A']=='Hedge Funds')
          & (o_sub['Date']==end)]
calcs = {
    'NAV EUR': sum,
    'YTD Gain EUR (YS FX)': sum
    }
x = x.groupby(['Date']).agg(calcs).reset_index()
x['Currency']='EUR'
x['Datatype']='Weighted Average'
x['Asset Class B'] = 'Total Hedge Fund Portfolio'
x.rename(columns = {'NAV EUR': 'NAV', 'YTD Gain EUR (YS FX)': 'YTD Gain'}, inplace=True)
o_hf = o_hf.append(x)


# Create total portfolio month df container to latch on in order to fill the table in Tableau
a = {'Period': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Sep', 'Oct', 'Nov', 'Dec']}
z = pd.DataFrame(a)
z['Performance Type'] = 'MTD'
z['Year'] = year
z['Asset Class A'] = 'Hedge Funds'
z['Datatype'] = 'Weighted Average'
z['Asset Class B'] = 'Total Hedge Fund Portfolio'
o_hf = o_hf.append(z)








### MARKETABLE SECURITIES BACK-UP

# Select which asset classes are included
b = ['Stocks', 'Bonds', 'Options', 'Futures', 'Liquidity Funds' ]

# Get relevant data from existing dfs
a = ['Date', 'Asset Class B', 'Position Name', 'Position Subname', 'Currency',
     'NAV', 'M Start', 'Y Start', 'MTD Gain', 'MTD %', 'YTD Gain', 'YTD %'  ]
x = o_sub[o_sub['Asset Class B'].isin(b)]
x = x[a]
o_mkt = x.copy()

# Create joint df
a = ['Date', 'Asset Class B', 'Position Name', 'Position Subname', 'Currency',
     'Price', 'Accrued', 'YTM', 'YTW', 'Strike', 'Expiry', 'Units per Contract']
x = n_eq[n_eq.columns.intersection(a)]
x = x.append(n_bond[n_bond.columns.intersection(a)])
x = x.append(n_der[n_der.columns.intersection(a)])
x['Units per Contract'] = x['Units per Contract'].fillna(1)

# Merge into main df
x = o_mkt.merge(x, on = ['Date', 'Asset Class B', 'Position Name',
                         'Position Subname', 'Currency'],
                how = 'left')

# Get period start prices
y = x[['Date', 'Asset Class B', 'Position Name', 'Position Subname', 'Price']]
y.rename(columns={'Date': 'M Start', 'Price': 'MS Price'}, inplace=True)
x = x.merge(y, on = ['M Start', 'Asset Class B', 'Position Name', 'Position Subname'],
            how = 'left')
y.rename(columns={'M Start': 'Y Start', 'MS Price': 'YS Price'}, inplace=True)
x = x.merge(y, on = ['Y Start', 'Asset Class B', 'Position Name', 'Position Subname'],
            how = 'left')

# Get units
x = x.merge(units, on = ['Date', 'Position Name', 'Position Subname'],
            how = 'left')

# Get total cost and premium
x['Cost/ Premium'] = x.apply(lambda l: av_cost(l['Position Subname'], cost_type='total'),
                                     axis=1)

# Get average cost
x['Avg Cost'] = x['Cost/ Premium']/x['Units']
x['Avg Cost'] = x.apply(lambda l: l['Avg Cost'] *
                        (100 if l['Asset Class B'] == 'Bonds' else 1),
                        axis=1)


# Get coupons and dividends - not needed: not really looked at

# Drop if closed
x['Closed'] = x['NAV']+x['YTD Gain']
x = x[x['Closed']!=0]

# Identify if expired (for stocks, say closed if units=0)
x['Expiry']=x['Expiry'].astype(str)
x.loc[x['Expiry'] == 'nan', 'Closed'] = 'nan'
x.loc[x['Expiry'] != 'nan', 'Closed'] = x[x['Expiry'] != 'nan'].apply(lambda l: 'Closed'
                                                                  if dt.datetime.strptime(l['Expiry'], '%Y-%m-%d').date()
                                                                  <= end else 'Open',
                                                                  axis=1)
x.loc[x['Closed'] == 'nan', 'Closed'] = x[x['Closed'] == 'nan'].apply(lambda l: 'Closed'
                                                                  if l['Units']==0 else 'Open',
                                                                  axis=1)

# Identify as subpositions
x['Datatype'] = 'Subposition'


## Add in numbers by position for positions with multiple subpositions

# Aggregate subpositions into positions by date
y = x.copy()
calcs = {'Currency': lambda x: x.mode(),
         'Units': sum,
         'Cost/ Premium': sum,
         'NAV': sum,
         'YTD Gain': sum,
         'Expiry': min,
         'Position Subname': 'count'}
y = y.groupby(['Date', 'Asset Class B', 'Position Name']).agg(calcs).reset_index()
y.loc[y['Asset Class B']=='Options', 'Units'] = 'NM'


# Drop positions with only one subposition per date and merge in
y = y[y['Position Subname']>1]
y['Position Subname'] = 'Position Total'
y['Datatype'] = 'Position'
x = x.append(y)


## Add benchmarks
y = bmrks[(bmrks['Date']==end)&(bmrks['Asset Class B']=='Stock Portfolio')]
y['Asset Class B'] = 'Benchmarks'
a = ['Date', 'Currency', 'Asset Class B', 'Position Name', 'Price', 'Datatype', 'Performance']
z = y[y['Performance Type']=='MTD'][a]
z.rename(columns={'Performance': 'MTD %'}, inplace=True)
y = y[y['Performance Type']=='YTD'][a]
y.rename(columns={'Performance': 'YTD %'}, inplace=True)
y = y.merge(z, on = ['Date', 'Currency', 'Asset Class B', 'Position Name', 'Price', 'Datatype'],
            how = 'left')
x = x.append(y)


# Make Position Subname empty for positions that have the same subname
x['Position Subname'] = x.apply(lambda l: "" if l['Position Name'] == l['Position Subname'] else l['Position Subname'],
                                axis=1)

# Save df
o_mkt = x.copy()















### MTD PERFORMANCE BY ASSET CLASS A (USED FOR THE BENCHMARKS COMPARISON DASHBOARD)

# Select dates
x = o_sub[o_sub['Date']>limit2]

# Select which class B's to include in securities
x.loc[x['Asset Class B'].isin(['Stocks', 'Bonds', 'Derivatives']), 'Asset Class A'] = 'Securities'

# Select Asset classes
x = x[x['Asset Class A'].isin(['Securities', 'Private Investments', 'Hedge Funds'])]


# Sum EUR Values
calcs = {
    'NAV EUR': sum,
    'MS NAV EUR': sum,
    'YS NAV EUR': sum,
    'MS NAV EUR E_FX': sum,
    'YS NAV EUR E_FX': sum,
    'MTD Flows EUR (MS FX)': sum,
    'YTD Flows EUR (YS FX)': sum,
    'MTD Gain EUR (MS FX)': sum,
    'YTD Gain EUR (YS FX)': sum,
    'CumBase': sum,
    'M Start': lambda x: x.mode(),
    'Y Start': lambda x: x.mode()
    }
x = x.groupby(['Date', 'Asset Class A']).agg(calcs).reset_index()

# Create MTD % using aggregated monthly NAVs and flows
x['Position Subname'] = x['Asset Class A']
x['Flow'] = x['MTD Flows EUR (MS FX)']
x['MTD %'] = x.apply(lambda l: mtd_ret(x, l['Asset Class A'],
                                       l['MS NAV EUR E_FX'], l['NAV EUR'],
                                       l['M Start'], l['Date']), axis =1)


# Add identifier to separate from benchmark returns
x['KSH/Bench'] = 'KSH'
x['Position Name'] = 'KSH'

# Save df with relevant fields
KSHvBench = x[['Date', 'KSH/Bench', 'Asset Class A', 'Position Name', 'MTD %']]   




## Get Benchmarks

# Get data
x = bmrks[bmrks['Performance Type'] == 'MTD'][['Date', 'Asset Class A',
                                              'Asset Class B', 'Position Name',
                                              'Performance']]
x = x[x['Date']>limit2]

# Rename columns
x.rename(columns = {'Asset Class A': 'KSH/Bench', 'Performance': 'MTD %'}, inplace=True)

# Add Asset Class A
def assign_a(name):   
    if name == 'Stock Portfolio':
        return 'Securities'
    elif name == 'Total Hedge Fund Portfolio':
        return 'Hedge Funds'
    elif name == 'Private Investment Portfolio':
        return 'Private Investments'
    else:
        return 'nan'

x['Asset Class A'] = x['Asset Class B'].apply(lambda l: assign_a(l))

# Drop non-Asset A benchmarks
x = x[x['Asset Class A'] != 'nan']


# Merge into df
KSHvBench = KSHvBench.append(x)




## Create benchmark for total KSH Portfolio

# Create target
target = 0.053
target = (1+target) ** (1/12) - 1

# Get Dates
x = pd.DataFrame(KSHvBench['Date'].unique())
x.columns = ['Date']

# Add Target and other fields
x['MTD %'] = target
x['KSH/Bench'] = 'Benchmark'
x['Asset Class A'] = 'Total Portfolio'
x['Position Name'] = 'KSH 7% Target'

# Merge into df
KSHvBench = KSHvBench.append(x)



## Add in total portfolio actual returns

# Get performance
x = o_port_eur[['Date', 'MTD Gain %']]
x = x[x['Date']>limit2]

# Rename columns
x.rename(columns = {'MTD Gain %': 'MTD %'}, inplace=True)

# Add other fields
x['KSH/Bench'] = 'KSH'
x['Asset Class A'] = 'Total Portfolio'
x['Position Name'] = 'KSH'

# Merge into df
KSHvBench = KSHvBench.append(x)




## Calculate Cumulative return

# Add value of 0 to first date
x = KSHvBench[['KSH/Bench', 'Asset Class A', 'Position Name']].drop_duplicates()
x['Date'] = limit2
x['MTD %'] = 0
KSHvBench = KSHvBench.append(x)

# Calculate return cumulative
KSHvBench['1+MTD'] = KSHvBench['MTD %'] +1
KSHvBench.sort_values(by = 'Date', inplace=True)
KSHvBench['ID'] = KSHvBench['Asset Class A'] + KSHvBench['Position Name']
KSHvBench['CumMTD'] = KSHvBench.groupby('ID')['1+MTD'].cumprod()
KSHvBench['CumMTD'] = KSHvBench['CumMTD']-1















### CALCULATE HEDGING SUMMARY AND EXPIRY SCHEDULE

## Currency hedging positions

# Get MTD Gains from n_fxf_agg2
# Get MS NAV
a = ['Date', 'Position Name', 'Position Subname', 'Currency B', 'Currency', 'Action', 'NAV']
x = n_fxf_agg2[a]
x['Date'] = x['Date']+dt.timedelta(1)
x['Date'] = x['Date'].apply(lambda l: dt.date(l.year, l.month, calendar.monthrange(l.year, l.month)[-1]))
x.rename(columns ={'NAV': 'MS NAV'}, inplace = True)
a.remove('NAV')
a = list(a)
n_fxf_agg2 = n_fxf_agg2.merge(x, on = a, how='left')
n_fxf_agg2['MS NAV'] = n_fxf_agg2['MS NAV'].fillna(0)

# Get MTD flows
a = f_mtd[f_mtd['Position Subname'].isin(n_fxf_agg2['Position Subname'])]
n_fxf_agg = n_fxf_agg.merge(a, on = ['Position Name', 'Position Subname', 'Date'],
                      how='left')
n_fxf_agg['MTD Flows'].fillna(0, inplace=True)

# Split flows by buy/sell
n_fxf_agg['Closing Spot'] = n_fxf_agg['Term Amount']/(-n_fxf_agg['Base Amount']-n_fxf_agg['MTD Flows'])
a = n_fxf_agg[['Date', 'Position Name', 'Position Subname', 'MTD Flows', 'Closing Spot', 'Term Amount']]
a.rename(columns = {'Term Amount': 'Agg Term Amount'}, inplace=True)
a = a[a['MTD Flows']!=0].drop_duplicates()
n_fxf_agg2 = n_fxf_agg2.merge(a, on = ['Date', 'Position Name', 'Position Subname'],
                              how = 'left')
n_fxf_agg2['MTD Flows'] = n_fxf_agg2['Term Amount']/n_fxf_agg2['Closing Spot']+n_fxf_agg2['Base Amount']
n_fxf_agg2['MTD Flows'] = n_fxf_agg2['MTD Flows'].fillna(0)

# Calculate MTD Gains
n_fxf_agg2['MTD Gain'] = n_fxf_agg2['NAV']-n_fxf_agg2['MS NAV']-n_fxf_agg2['MTD Flows']

# Calculate YTD Gains
n_fxf_agg2['Year'] = n_fxf_agg2['Date'].apply(lambda l: l.year)

def calc_fx_tot(date, year, name, action, period):
    a = n_fxf_agg2[(n_fxf_agg2['Date']<=date) &
                   (n_fxf_agg2['Position Subname']==name) &
                   (n_fxf_agg2['Action']==action)]
    
    if period == 'YTD':
        a = a[a['Year']==year]
    else:
        pass
    
    a = a['MTD Gain'].sum()
    return a

n_fxf_agg2['YTD Gain'] = n_fxf_agg2.apply(lambda l: calc_fx_tot(l['Date'],
                                                                l['Year'],
                                                                l['Position Subname'],
                                                                l['Action'],
                                                                'YTD'), axis=1)

# ITD Gain = NAV for forwards
n_fxf_agg2['ITD Gain'] = n_fxf_agg2.apply(lambda l: calc_fx_tot(l['Date'],
                                                                l['Year'],
                                                                l['Position Subname'],
                                                                l['Action'],
                                                                'ITD'), axis=1)

# Remove irrelevant fx dfs, and get relevant data
x = n_fxf_agg2[n_fxf_agg2['Date']==end]

# Add back instrument for FX
x['Instrument']  ='Forward'

# Save df
o_fx = x.copy()



## Add options
x = n_fxo[n_fxo['Date']==end]

# Add in MTD and YTD gains
y = o_sub[o_sub['Date']==end][['Position Subname', 'YTD Gain EUR (YS FX)', 'MTD Gain EUR (MS FX)']]
calcs = {'YTD Gain EUR (YS FX)': sum, 'MTD Gain EUR (MS FX)': sum}
y = y.groupby('Position Subname').agg(calcs).reset_index()
y.rename(columns={'MTD Gain EUR (MS FX)': 'MTD Gain',
                  'YTD Gain EUR (YS FX)': 'YTD Gain'}, inplace=True)
x = x.merge(y, on='Position Subname', how='left')

# ITD gain = sum of current YTD gains and past YTD Gains at end of each year
o_sub['Month'] = o_sub['Date'].apply(lambda l: l.month)
y = o_sub[(o_sub['Date']==end)|(o_sub['Month']==12)]
y = y[y['Position Subname'].isin(x['Position Subname'])]
y = y.groupby('Position Subname').agg({'YTD Gain EUR (YS FX)': sum})
y.rename(columns={'YTD Gain EUR (YS FX)': 'ITD Gain'}, inplace=True)
x = x.merge(y, on='Position Subname', how='left')

x = o_fx.append(x)




# Determine whether buying or selling
x['Action'] = x.apply(lambda l: "Sell" if l['Term Amount'] >=0 else "Buy", axis=1)

# Get term currency
x['Currency'] = x['Currency B']

# Add back FX rate
y = n_fxf[n_fxf['Date']==end][['Position Subname', 'FX rate']]
y = y.append(n_fxo[n_fxo['Date']==end][['Position Subname', 'FX rate']])
y = y.drop_duplicates()
x = x.drop(['FX rate'], axis=1)
x = x.merge(y, on='Position Subname', how='left')



# Drop if expiry earlier than current year
x = x[x['Expiry']>start]

# Save df
o_fx = x.copy()






## Currency hedging summmary

# Calculate lookthrough by currency for a tableau pie chart
# Merge lookthrough into positions df
x = df_det[['Position Name', 'USD exposure']]
x = o_pos.merge(x, on = 'Position Name', how='left')
x = x[['Date', 'Position Name', 'NAV', 'NAV EUR', 'Currency', 'USD exposure']]

# Replace NAs with 0
x['USD exposure'] = x['USD exposure'].fillna(0)

# Calculate USD lookthrough
x['USD Lookthrough'] = x['NAV EUR']*x['USD exposure']
x['USD Lookthrough (Local)'] = x['NAV']*x['USD exposure']

# Calculate lookthrough to other currencies
# Get other currencies
currencies = x['Currency'].unique()

# For each currency, get lookthrough
cols = []
for i in currencies:
    
    # Make column name
    n = i + " Lookthrough"
    
    # Get currency subset
    y = x[x['Currency']==i]
    
    # Get remainder after USD allocation, unless in USD, in which case allocate to other
    if i in ['USD']:
        n = 'Other'
    else:
        pass       
    y[n] = y['NAV EUR']-y['USD Lookthrough']
        
    # Merge into x
    x = x.merge(y[['Date', 'Position Name', 'Currency', n]], on=['Date', 'Position Name', 'Currency'], how='left')
    
    # Save column name
    cols.append(n)

# Get total USD lookthroughs in local currency (USD)
usd_lt = x[['Date', 'USD Lookthrough (Local)']].groupby('Date').agg(sum).reset_index()

# Aggregate by date
cols.append('Date')
cols.append('USD Lookthrough')
x = x[cols].groupby('Date').agg(sum).reset_index()

# Subtract the value hedged for each currency
# Get currencies that have been hedged, and all dates
y = o_fx2['Currency B'].unique()
z = o_pos['Date'].unique()

# Calculate the value hedged in EUR
o_fx2['Hedged EUR'] = o_fx2['Est. Val']-o_fx2['NAV']

# For each date and currency that has been hedged, subtract the EUR value hedged from the lookthrough
for i in y:
    for j in z:
        # Select the value to subtract
        a = o_fx2[(o_fx2['Date']==j)&(o_fx2['Currency B']==i)]['Hedged EUR']
        # Make column name
        n = i + " Lookthrough"
        # Subtract from respective lookthrough value
        x.loc[x['Date']==j, n] = x.loc[x['Date']==j, n] - a

# For each date and currency that has been hedged, add back the EUR value backing the hedge
y = o_fx2['Currency'].unique()
for i in y:
    for j in z:
        # Select the value to subtract
        a = o_fx2[(o_fx2['Date']==j)&(o_fx2['Currency']==i)]['Hedged EUR']
        # Make column name
        n = i + " Lookthrough"
        # Subtract from respective lookthrough value
        x.loc[x['Date']==j, n] = x.loc[x['Date']==j, n] + a


# Pivot 
cols.remove('Date')
x = pd.melt(x, id_vars='Date', value_vars=cols)
x.rename(columns={'variable': 'Currency', 'value': 'Lookthrough Value'},
         inplace=True)

# Remove the 'Looktrough' from strings
x['Currency'] = x['Currency'].str.replace(' Lookthrough', '')

# Save
o_look = x.copy()




## USD currency hedging expiry schedule

# Get month end of month after max expiry
n = np.max(n_fxf['Expiry'].append(n_fxo['Expiry']))
n = dt.date(n.year + n.month // 12, n.month % 12 + 2, 1) - dt.timedelta(1)
    
# Create set of monthly dates between now and max expiry
xperiod = pd.date_range(end,n,freq='M').strftime(datetype).tolist()

# Join options and forwards dfs
x = n_fxf.append(n_fxo)

# Get those open at current date only
x = x[(x['Date']==end)&(x['Closed']=='Open')].drop('Date', axis=1)

# Add credit hedging
y = o_pos[(o_pos['Asset Class B']=='Loans') &
          (o_pos['Currency']=='USD') &
          (o_pos['Date']==end) &
          (o_pos['NAV']!=0)]
y = y[['Asset Class B', 'Position Name', 'NAV', 'Currency']]

# If no USD loan, make it equal to 0 so that df contains values up to end date after max expiry
a = {'Asset Class B': 'Loans',
     'Position Name': 'USD Loans',
     'NAV': 0,
     'Currency': 'USD'}

if len(y) == 0:
    a = pd.DataFrame(a, index = [0])
    y = y.append(a)
else:
    pass

y['NAV'] = -y['NAV']     # loan navs are negative
y.rename(columns={'NAV': 'Term Amount', 'Currency': 'Currency B'}, inplace=True)
y['Instrument'] = 'Loan'
y['Closed'] = 'Open'
y['Expiry'] = n
x = x.append(y)

# Replicate the df for each future period
y = []
for i in xperiod:
    a = x.copy()
    a['Date'] = i
    y.append(a)
x = pd.concat(y, ignore_index=True)
x['Date'] = pd.to_datetime(x['Date'])
x['Date'] = x['Date'].apply(lambda l: l.date())

# Drop if expired
x = x[x['Date']<x['Expiry']]

# Select columns
x = x[['Date', 'Position Name', 'Instrument', 'Term Amount', 'Currency B', 'Expiry']]

# Get lookthrough for USD
y = usd_lt[usd_lt['Date']==end][['USD Lookthrough (Local)']].reset_index(drop=True)

# Keep only USD values
x = x[x['Currency B']=='USD']

# Divide hedged amount by total lookthrough
x['Lookthrough Value'] = y.iloc[0,0]
x['% of Lookthrough'] = x['Term Amount']/x['Lookthrough Value']

# Save df
o_fx_sched = x.copy()











### CALCULATE FREE LIQUIDITY

# Get Local Currency and EUR positions
x = o_pos[['Date', 'Asset Class A', 'Asset Class B', 'Position Name', 'Currency',
           'NAV', 'CCy_Type', 'Custody', 'Vehicle']]
y = o_pos_eur[['Date', 'Asset Class A', 'Asset Class B', 'Position Name', 'Currency',
           'NAV EUR', 'CCy_Type', 'Custody', 'Vehicle']]
y.rename(columns={'NAV EUR': 'NAV'}, inplace=True)
x = x.append(y)

# Drop positions where NAV = 0
x = x[x['NAV']!=0]



## GET OTHER MARGIN REQS
# Get other margin requirements
y = pd.read_excel(source2, sheet_name='Other_Margin_Req')
y = y.drop('Comment', axis=1)

# Identify date range where margin reqs should be added in
y.rename(columns={'Date of Modification': 'Date'}, inplace=True)
y['Date'] = y['Date'].apply(lambda x: x.date())
y['Until'] = y['Until'].fillna(end)
p = pd.date_range(np.min(y['Date']), np.max(y['Until']),freq='M').strftime(datetype).tolist()

# Convert to date
p = pd.DataFrame({'Date' :p})
p['Date'] = p['Date'].apply(lambda i: dt.datetime.strptime(i, datetype).date())

# Create df of other margin reqs: for each date in the period, concat the margin reqs active in that period
a = []
for i in p['Date']:
    
    # Filter out the adjustments active at current date
    z = y[(y['Date']<=i) & 
          ((y['Until']>i)| (y['Until']==end))]
    
    # Assign date
    z['Date'] = i
    z = z.drop('Until', axis=1)
    
    # Concat
    a.append(z)
y = pd.concat(a, ignore_index=True) 


# Get exchange rates
y['FX'] = y.apply(lambda l: get_fx(l['Currency'], l['Date']), axis=1)

# Save both local currency and EUR versions
y['CCy_Type'] = 'Local Currencies'
a = y.copy()
a['CCy_Type'] = 'Total'
a['Currency'] = 'EUR'
a['Margin Requirement'] = a['Margin Requirement']/a['FX']
y = y.append(a)

# Add to main liquidity df
add_mgn = y.copy()



## GET TERM VALUES FOR FX POSITIONS, OPTIONS, AND FUTURES (THAT'S WHAT MARGINS ARE CALCULATED ON)

# Drop FX from Liquidity df
x = x[-x['Asset Class A'].isin(['FX Hedging', 'Derivatives'])]

# Get relevant Forwards and Options df
y = n_fxf_agg[n_fxf_agg['Closed']=='Open'][['Date', 'Position Name', 'Position Subname',
                                            'Currency B', 'Term Amount',
                                            'Asset Class B']]
z = n_fxo[n_fxo['Closed']=='Open'][['Date', 'Position Name', 'Position Subname',
                                            'Currency B', 'Term Amount',
                                            'Asset Class B']]
y = y.append(z)
y.rename(columns={'Currency B': 'Currency'}, inplace=True)
y['Asset Class A'] = 'FX Hedging'

# Get Derivatives term amounts etc.
n_der['Term Amount'] = n_der['Strike']*n_der['Units']*n_der['Units per Contract']
z = n_der[n_der['Closed']=='Open'][['Date', 'Position Name', 'Position Subname',
                                            'Currency', 'Term Amount',
                                            'Asset Class B']]
z['Asset Class A'] = 'Derivatives'
y = y.append(z)
y = y[y['Term Amount'].notna()]

# Aggregate
y = y.groupby(['Date', 'Asset Class A', 'Asset Class B', 'Position Name', 'Currency']).agg(sum).reset_index()

# Make Term Amount exposure absolute so that you don't get negative margin reqs on shorts
y['Term Amount'] = abs(y['Term Amount'])

# Get FX
y['FX'] = y.apply(lambda l: get_fx(l['Currency'], l['Date']), axis=1)

# Change Term AMount to NAV for calcs in next section
y.rename(columns={'Term Amount': 'NAV'}, inplace=True)

# Save both local currency and EUR versions
y['CCy_Type'] = 'Local Currencies'
a = y.copy()
a['CCy_Type'] = 'Total'
a['Currency'] = 'EUR'
a['NAV'] = a['NAV']/a['FX']
y = y.append(a)

# Add other identifiers
y = y.merge(df_det[['Position Name', 'Vehicle', 'Custody']],
            on = 'Position Name',
            how = 'left')

# Add back to liquidity df
x = x.append(y)



## Add margin variation accounts that do not subtract cash directly from bank accounts

# Get variation accounts
y = pd.read_excel(source, sheet_name='Var_Mar', skiprows=[1])
y['Date'] = y['Date'].apply(lambda i: i.date())

# Get FX
y['FX'] = y.apply(lambda l: get_fx(l['Currency'], l['Date']), axis=1)

# Save both local currency and EUR versions
y['CCy_Type'] = 'Local Currencies'
a = y.copy()
a['CCy_Type'] = 'Total'
a['Currency'] = 'EUR'
a['NAV'] = a['NAV']/a['FX']
y = y.append(a)

# Add to liquidity df
x = x.append(y)



## CALCULATIONS

# Combine Custody per Bank
x['Custody'] = x['Custody'].apply(lambda l: l.split(' -')[0])
x['Custody'] = x['Custody'].apply(lambda l: l.split('-')[0])

# Add in the LTV and margin reqs
x = x.merge(df_det[['Position Name', 'LTV', 'Margin Requirement']],
            on='Position Name',
            how='left')

# Add in other margin requirements
x = x.append(y)

# Get margin buffers
y = pd.read_excel(source2, sheet_name='Margin_Req_Buffers')
y = y.drop('Comment', axis=1)

# Add margin buffers
x = x.merge(y, on='Asset Class B', how='left')

# Calculate gross LTV and Margin Reqs
x['LTV'] = x['LTV']*x['NAV']
x['Margin Requirement'] = (x['Margin Requirement']+x['Margin Buffer'])*x['NAV']
x = x.drop('Margin Buffer', axis=1)

# Add additional margin requirements
x = x.append(add_mgn)



# Replace NAs with 0 for the following calcs
x['NAV'] = x['NAV'].fillna(0)
x['LTV'] = x['LTV'].fillna(0)
x['Margin Requirement'] = x['Margin Requirement'].fillna(0)

# Calculate Blocked Liquidity
x['Blocked'] = x['LTV']-x['Margin Requirement']

# Set NAVS for all but cash to 0
x.loc[x['Asset Class B']!='Cash', 'NAV'] = 0

# Calculate free liquidity
x['Free'] = x['NAV']+x['Blocked']




## Rearrage and assign value names: Cash held, Margin Reqs, Blocked Liquidity, LTV, Free Liquidity, Totals
# Rearrange Cash
z = ['Date', 'Asset Class A', 'Asset Class B', 'Position Name', 'Currency',
     'CCy_Type', 'Custody', 'Vehicle']
z.append('NAV')
y = x[z]
y = y[y['Asset Class B']=='Cash']
y['Value Type'] = 'Cash'
y.rename(columns={'NAV': 'Value'}, inplace=True)
a = y.copy()

# Rearrange LTV
z.remove('NAV')
z.append('LTV')
y = x[z]
y['Value Type'] = 'LTV'
y.rename(columns={'LTV': 'Value'}, inplace=True)
a = a.append(y)

# Rearrange Margin Requirements (and turn value negative as they reduce free cash)
z.remove('LTV')
z.append('Margin Requirement')
y = x[z]
y['Value Type'] = 'Margin Requirement'
y.rename(columns={'Margin Requirement': 'Value'}, inplace=True)
y['Value'] = -abs(y['Value'])
a = a.append(y)

# Rearrange Blocked Liquidity
z.remove('Margin Requirement')
z.append('Blocked')
y = x[z]
y['Value Type'] = 'Blocked Liquidity'
y.rename(columns={'Blocked': 'Value'}, inplace=True)
a = a.append(y)

# Rearrange Free Liquidity
z.remove('Blocked')
z.append('Free')
y = x[z]
y['Value Type'] = 'Free Liquidity'
y.rename(columns={'Free': 'Value'}, inplace=True)
a = a.append(y)


# Get portfolio NAV at each date for Total values
a = a.merge(o_port_eur[['Date', 'Currency', 'NAV EUR']],
            on = ['Date', 'Currency'],
            how='left')
a.loc[a['CCy_Type']!='Total', 'NAV EUR'] = float('nan')


# Calculate total EUR values divided by total NAV
a['% of NAV'] = a['Value']/a['NAV EUR']

# Drop positions where Value = 0
a = a[a['Value']!=0]
a = a[-a['Value'].isna()]

# Save
o_free = a.copy()










### CALCULATE ENEB

# Get ENEB weights
x = pd.read_excel(source2, sheet_name='ENEB')

# Add portfolio NAVs to ClassB df to calculate % allocation to each class
y = o_port_eur[['Date', 'NAV EUR']]
y.rename(columns={'NAV EUR': 'Portfolio NAV'}, inplace=True)
o_classb_eur = o_classb_eur.merge(y, on = 'Date', how='left')
o_classb_eur['Allocation'] = o_classb_eur['NAV EUR']/o_classb_eur['Portfolio NAV']

# Add ENEB weights to df
o_classb_eur['Overview Class'] = o_classb_eur['Asset Class B']
x = x[['Overview Class', 'ENEB']].drop_duplicates()
o_classb_eur = o_classb_eur.merge(x[['Overview Class', 'ENEB']],
                                  on = 'Overview Class', how='left')

# Calculate ENEB
o_classb_eur['ENEB'] = o_classb_eur['Allocation']*o_classb_eur['ENEB']









### CALCULATE HISTORICAL KSH NAVS/IRRS

# Get necessary data from portfolio_eur
x = o_port_eur[['Date', 'Year', 'Currency', 'NAV EUR', 'MTD Contributions to KSH',
                'YTD Gain', 'YTD Gain %', 'MTD Gain',
                'MTD Gain %', 'YTD Contributions to KSH']]

# Import historical total NAVs and contributions
y = pd.read_excel(source2, sheet_name='Historical_NAVs')
y['Date'] = y['Date'].apply(lambda i: i.date())

# Keep python-calculated NAV at limit date (end of 2021)
# otherwise you end up with higher 2021 YTD Gains because the python calcs start in 2022
a = x[x['Date']==limit2]['NAV EUR'].reset_index(drop=True)

# Save original historical limit NAV and beginning of limit year and month
b = y[y['Date']==limit2]['NAV EUR'].reset_index(drop=True)
c = y[y['Date']==dt.date(limit2.year-1, 12, 31)]['NAV EUR'].reset_index(drop=True)
d = y[y['Date']==dt.date(limit2.year, 11, 30)]['NAV EUR'].reset_index(drop=True)

# Get difference between new and old NAVs
b = a[0]-b[0]

# Replace historical lmit NAV with python NAV
y.loc[y['Date']==limit2, 'NAV EUR'] = a[0]

# Recalculate gains for replaced value
y.loc[y['Date']==limit2, 'MTD Gain'] = y.loc[y['Date']==limit2, 'MTD Gain']+b
y.loc[y['Date']==limit2, 'YTD Gain'] = y.loc[y['Date']==limit2, 'YTD Gain']+b
y.loc[y['Date']==limit2, 'YTD Gain %'] = y.loc[y['Date']==limit2, 'YTD Gain']/c[0]
y.loc[y['Date']==limit2, 'MTD Gain %'] = y.loc[y['Date']==limit2, 'MTD Gain']/d[0]

# Merge the pre and post 2021 NAVs
x = x[x['Date']>limit2]        # drop overlapping values to avoid duplication
#y = y[y['Date']!=limit2]     
x = x.append(y) 


# Define portfolio IRR function
def port_irr(df, nav_name, flows_name, p_end):
    
    # Get desired df
    a = df
    
    # Remove future values
    a = a[(a['Date'] <= p_end)]  
    
    ## Create flows to use for IRR
    # Set all NAVs last to 0
    keep = np.max(a['Date'])
    a.loc[a['Date']!=keep, nav_name] = 0
      
    # Subtract flows from NAVs (contributions are flows in and need to be subtracted)
    a['IRR_flows'] = a[nav_name]-a[flows_name]
    a = a[['Date', 'IRR_flows']]   
    
    # Sort
    a = a.sort_values(by=['Date']).reset_index(drop=True)

    # Calculate IRRs
    if (len(set(a['Date'])) > 1):
        irr = xirr(a['Date'], a['IRR_flows'])
        return irr
    else:
        return None      # No result for positions with no historical numbers (i.e 2013)


# Calculate portfolio total IRR and performance monthly
x['Inception-to-Date IRR'] = x.apply(lambda l:
                                     port_irr(x,
                                     nav_name = 'NAV EUR',
                                     flows_name = 'MTD Contributions to KSH',
                                     p_end = l['Date']),
                                     axis=1)

# Add adjustments to the non-historical NAVs used for IRRs
y = pd.read_excel(source2, sheet_name='Annual Adj & Targets')
y['Date'] = y['Date'].apply(lambda i: i.date())
x = x.merge(y[['Year', 'Annual Adjustment', 'Target % Returns']], on='Year', how='left')


## Create NAV without contributions
# Sort by date
x = x.sort_values(by=['Date']).reset_index(drop=True)

# Replace first 2013 'contribution' with 0
x['MTD Contributions to KSH'][0] = 0

# Calculate cumulative contributions
x['Cumulative Contributions since 2013'] = np.cumsum(x['MTD Contributions to KSH'])

# NAV Excluding Contributions since 2013
x['NAV exc.Contributions'] = x['NAV EUR']-x['Cumulative Contributions since 2013']



## Create NAV using annual targets since inception

# Set first target equal to first NAV
x['Target NAV'] = 0

# Make targets monthly
x['Target % Returns'] = x['Target % Returns'].apply(lambda l: (1+l)**(1/12) -1)

# Define function
def get_tgt(df, target, contributions, date):
    
    # If date is the minimum return the NAV
    if (date == np.min(df['Date'])):
        df.loc[df['Date'] == date, 'Target NAV'] = df.loc[df['Date'] == date, 'NAV EUR']
    
    else:
        # Get previous month's target
        b = date.replace(day=1)
        b = b - dt.timedelta(days=1)
        b = df.loc[df['Date'] == b, 'Target NAV']
    
        # Multiply by target
        b = b*(1+target)
        
        # Add contributions
        b = b+contributions
        b = b.reset_index(drop=True)
        
        # Save value in df
        df.loc[df['Date'] == date, 'Target NAV'] = b[0]

x.apply(lambda l:
                get_tgt(x,
                target = l['Target % Returns'],
                contributions = l['MTD Contributions to KSH'],
                date = l['Date']),
                axis=1)

    
## Calculate the YTD % deltas from month to month
# Get month start
x['MS Date'] = x['Date'].apply(
    lambda l: l.replace(day=1)- dt.timedelta(days=1))

# Get month start YTD
y = x[['Date', 'YTD Gain %']]
y.rename(columns={'Date': 'MS Date', 'YTD Gain %': 'MS YTD %'}, inplace=True)
x = x.merge(y, on='MS Date', how='left')

# If January, then MS YTD % = 0
x['Month'] = x['Date'].apply(lambda l: l.month)
x.loc[x['Month']==1, 'MS YTD %'] = 0

# Calculate YTD% delta
x['YTD % Delta'] = x['YTD Gain %'] - x['MS YTD %']   
    
# Save df
o_port_h = x.copy()











### MAKE GAIN BREAKDOWN TABLE

# Calculate error
o_port_eur['Error'] = -o_port_eur['YTD Returns 2']+o_port_eur['YTD Gain']
o_port_eur['YTD Net FX Impact'] = o_port_eur['YTD Net FX Impact']+o_port_eur['Error']

# Select columns that will be used
a = ['Date', 'YTD Gain ex FX', 'YTD FX Gain on Assets', 'YTD FX Hedging Gain',
     'YTD FX Conversion Gain', 'Error', 'YTD Net FX Impact', 'YTD Returns inc. FX Impact',
     'YTD Expenses', 'YTD Gain']

# Get columns and transpose
x = o_port_eur[a]
x['YTD Expenses'] = -x['YTD Expenses']
x = np.transpose(x).reset_index()

# Make dates the column names
x.columns = x.loc[0,:]
x = x[1:]

# Make fields the index
x.index = x['Date']
x = x.drop('Date', axis=1)

# Pivot longer
x = pd.DataFrame(x.unstack())
x.columns = ['Value']
x['Value'] = x['Value'].astype(float)
x = x.reset_index()
x.columns = ['Date', 'Field', 'Value']

# Identify totals
a = ['YTD Gain ex FX', 'YTD Returns inc. FX Impact', 'YTD Gain']
x['Field_Type'] = x['Field'].apply(lambda l: "Total" if l in a else "")

# Identify subtotals
a = ['YTD Net FX Impact']
x.loc[x['Field_Type']!='Total', 'Field_Type'] = x.loc[x['Field_Type']!='Total', 'Field'].apply(lambda l: "Subtotal" if (l in a) else "")

# Add year start portfolio NAVs to ClassB df to calculate % impact of each gain component
y = o_port_eur[['Date', 'NAV EUR']]
y.rename(columns={'NAV EUR': 'Portfolio NAV', 'Date': 'YS Date'}, inplace=True)
x['YS Date'] = x['Date'].apply(lambda l: dt.date(l.year-1, 12, 31))
x = x.merge(y, on = 'YS Date', how='left')
x['%'] = x['Value']/x['Portfolio NAV']

# Save
gain_brk = x.copy()





### ADD ADDITIONAL POSITION INFORMATION

## Add units to positions and subpositions
# Subpositions
o_sub = o_sub.merge(units[['Date', 'Position Subname', 'Units']],
                    on = ['Date', 'Position Subname'],
                    how='left')
# Positions
x = units[['Date', 'Position Name', 'Units']].groupby(['Date', 'Position Name']).agg(sum).reset_index()
o_pos = o_pos.merge(x,
                    on = ['Date', 'Position Name'],
                    how='left')



## Add avg_cost to positions and subpositions

# Subpositions
x = o_sub[o_sub['Date']==end]
x['Tot Cost'] = x.apply(lambda l:
            av_cost(l['Position Subname'], "total", 'Position Subname'),
            axis=1)
x.loc[x['Tot Cost']==1, 'Tot Cost'] = 0    # for posiitons with no units, make tot units 0. Could sum all flows, but doesn't give mych info
x['Av Cost'] = x['Tot Cost']/x['Units']
x['Av Cost'] = x.apply(lambda l: l['Av Cost'] *
                        (100 if l['Asset Class B'] == 'Bonds' else 1),
                        axis=1)
x = x[['Date', 'Position Subname', 'Av Cost', 'Tot Cost']]
o_sub = o_sub.merge(x, on =['Date', 'Position Subname'], how='left')
    
# Positions
x = o_pos[o_pos['Date']==end]
x['Tot Cost'] = x.apply(lambda l:
            av_cost(l['Position Name'], "total", 'Position Name'),
            axis=1)
x.loc[x['Tot Cost']==1, 'Tot Cost'] = 0    # for posiitons with no units, make tot units 0. Could sum all flows, but doesn't give mych info
x['Av Cost'] = x['Tot Cost']/x['Units']
x['Av Cost'] = x.apply(lambda l: l['Av Cost'] *
                        (100 if l['Asset Class B'] == 'Bonds' else 1),
                        axis=1)
x = x[['Date', 'Position Name', 'Av Cost', 'Tot Cost']]
o_pos = o_pos.merge(x, on =['Date', 'Position Name'], how='left')
o_pos = o_pos.drop_duplicates() 


## Add price to positions and subpositions
o_pos['Current Price'] = o_pos['NAV']/o_pos['Units']
o_sub['Current Price'] = o_sub['NAV']/o_sub['Units']



## Add expiries and strike to subpositions
# From derivatives
x = o_mkt[(o_mkt['Datatype']=='Subposition')
          & (o_mkt['Date']==end)][['Date', 'Position Subname', 'Expiry', 'Strike']]
x = x.dropna()

# From FX
y = n_fxf_agg[['Date', 'Position Subname', 'Expiry', 'Forward Price']]
y = y.append(n_fxo[['Date', 'Position Subname', 'Expiry', 'Forward Price']])
y.rename(columns={'Forward Price': 'Strike'}, inplace=True)
y = y.dropna()

# Join and add
x = x.append(y)
o_sub = o_sub.merge(x, on =['Date', 'Position Subname'], how='left')

# Drop duplicates
o_sub = o_sub.drop_duplicates()


## Create new MTD gain variable, that allows starting from the limit
# Define function
def mtd_limit(name, date, mtd_gain, nav):
    
    # If before limit, return null
    if date<limit2:
        return 0
    
    # If date equals limit, return difference between NAV and total cost
    elif date == limit2:
        
        # Calculate total flows
        x = f_inv[(f_inv['Position Name']==name) &
                  (f_inv['Date']<=limit2)]
        x = x['Flow'].sum()
        
        # Return NAV minus total flows
        x = nav + x
        return x
    
    # If date after limit, return MTD Gain
    else:
        return mtd_gain

# Apply function
o_pos['MTD Gain postlim'] = o_pos.apply(lambda l: mtd_limit(l['Position Name'],
                                                            l['Date'],
                                                            l['MTD Gain'],
                                                            l['NAV']), axis=1)

# Replace with 0 for cash positions
o_pos.loc[o_pos['Asset Class A']=='Liquidity','MTD Gain postlim'] = 0












### ADJUST SOME COLUMN NAMES BEFORE EXPORT TO ALLOW UNION IN TABLEAU
o_classb_eur.rename(columns={'NAV EUR': 'NAV', 'MTD Gain EUR (MS FX)': 'MTD Gain',
                             'YTD Gain EUR (YS FX)': 'YTD Gain'}
                    , inplace=True)
o_pos_eur.rename(columns={'NAV EUR': 'NAV', 'MTD Gain EUR (MS FX)': 'MTD Gain',
                             'YTD Gain EUR (YS FX)': 'YTD Gain'}
                    , inplace=True)
o_sub_eur.rename(columns={'NAV EUR': 'NAV', 'MTD Gain EUR (MS FX)': 'MTD Gain',
                             'YTD Gain EUR (YS FX)': 'YTD Gain'}
                    , inplace=True)
o_vehicle_eur.rename(columns={'NAV EUR': 'NAV', 'MTD Gain EUR (MS FX)': 'MTD Gain',
                             'YTD Gain EUR (YS FX)': 'YTD Gain'}
                    , inplace=True)
pc_eur.rename(columns={'NAV EUR': 'NAV', 'MTD Gain EUR (MS FX)': 'MTD Gain',
                             'YTD Gain EUR (YS FX)': 'YTD Gain'}
                    , inplace=True)
o_port_eur = o_port_eur.drop(['MTD Gain', 'YTD Gain'], axis=1)
o_port_eur.rename(columns={'NAV EUR': 'NAV', 'MTD Gain EUR (MS FX)': 'MTD Gain',
                             'YTD Gain EUR (YS FX)': 'YTD Gain'}
                    , inplace=True)










### AGGREGATE THE VEHICLE PORTFOLIO TO ALSO INCLUDE PARTNER'S CAPITAL AND THE TOTAL PORTFOLIO

# RoI contribution of total portfolio equals the total YTD performance ex FX and allocation = 100%
o_port_eur['RoI Contribution'] = o_port_eur['YTD Gain % ex FX']*10000
o_port_eur['Allocation'] = 1

# Give vehicle names
o_port_eur['Vehicle'] = 'Total Portfolio'
o_port_eur['Total Type'] = 'Vehicle Total'
o_port_local['Vehicle'] = 'Total Portfolio'
o_port_local['Total Type'] = 'Vehicle Total'
pc['Vehicle'] = 'PC'
pc['Total Type'] = 'Vehicle Total'
pc_eur['Vehicle'] = 'PC'
pc_eur['Total Type'] = 'Vehicle Total'

# Create ex FX columns for PC
pc['MTD Gain ex FX'] = pc['MTD Gain']
pc['YTD Gain ex FX'] = pc['YTD Gain']
pc_eur['MTD Gain ex FX'] = pc_eur['MTD Gain']
pc_eur['YTD Gain ex FX'] = pc_eur['YTD Gain']
pc_eur['MTD Gain % ex FX'] = pc_eur['MTD %']
pc_eur['YTD Gain % ex FX'] = pc_eur['YTD %']

# Append
o_vehicle = o_vehicle.append(o_vehicle_eur)
o_vehicle = o_vehicle.append(o_port_eur)
o_vehicle = o_vehicle.append(o_port_local)
o_vehicle = o_vehicle.append(pc)
o_vehicle = o_vehicle.append(pc_eur)

# Select variables needed
a = ['Date', 'Vehicle', 'NAV', 'MTD Gain', 'YTD Gain', 'Allocation', 'Currency',
     'MTD %', 'YTD %', 'CCy_Type', 'Total Type', 'RoI Contribution', 'Asset Class B',
     'MTD Gain ex FX', 'YTD Gain ex FX', 'YTD Gain % ex FX', 'MTD Gain % ex FX']
o_vehicle = o_vehicle[a]

# Identify if total portfolio
o_vehicle['Overall'] = o_vehicle['Vehicle'].apply(lambda l:
                                                  "Yes" if l == 'Total Portfolio'
                                                  else "No")





### ADD PRESENTATION GROUPING CLASSES TO CERTAIN DATAFRAMES

# Get overview classes
y = pd.read_excel(source2, sheet_name='ENEB')[['Asset Class B', 'Grouping']]
y = y.drop_duplicates()

# Assign overview classes
o_pos = o_pos.merge(y, on='Asset Class B', how='left')
o_pos_eur = o_pos_eur.merge(y, on='Asset Class B', how='left')
o_sub = o_sub.merge(y, on='Asset Class B', how='left')
o_sub_eur = o_sub_eur.merge(y, on='Asset Class B', how='left')
o_geo = o_geo.merge(y, on='Asset Class B', how='left')

# Adjust for dataframes that have Overview Classes as Asset Class B
y = pd.read_excel(source2, sheet_name='ENEB')[['Overview Class', 'Grouping']]
y = y.drop_duplicates()
y.rename(columns={'Overview Class': 'Asset Class B'}, inplace=True)
o_vehicle = o_vehicle.merge(y, on='Asset Class B', how='left')







### ASSIGN PAGES

## PDF VERSION - POSITION LISTS

# Max number of positions per page
n = 33

# Edit groups because several private investment ones are merged in the pdf
def edit_group(assetclassa, grouping):
    if (assetclassa == 'Private Investments') & (grouping not in ['PE Funds', 'Private Equity Funds']):
        return 'Other Private'
    else:
        return grouping

o_pos['Grouping_pdf'] = o_pos.apply(lambda l: edit_group(l['Asset Class A'],
                                                     l['Grouping']), axis=1)

# Get Asset B ranks
y = pd.read_excel(source2, sheet_name='ENEB')
o_pos = o_pos.merge(y[['Asset B Rank', 'Asset Class B']],
            on = 'Asset Class B', how='left')
o_pos['Asset B Rank'] = o_pos['Asset B Rank'].astype(str)

# Define page assigning function
def assign_page(df, date, name, name_header, group, vehicle_sort='Yes',
                vintage_sort='No', closed_sort='No', name_sort='Yes'):
    
    # Get all positions at relevant date
    a = ['Date', 'Asset Class B', 'Asset B Rank', name_header]
    if vehicle_sort=='Yes':
        a.append('Vehicle')
    if vintage_sort=='Yes':
        a.append('Vintage')
    if closed_sort=='Yes':
        a.append('Status')    
    x = df[(df['Date']==date)&(df['Grouping_pdf']==group)][a]
 
    # Drop duplicates
    x = x.drop_duplicates()
    
    # Get list of unique asset classes and order them
    y = list(x['Asset B Rank'].unique())
    y.sort()
    
    #  For each asset class
    # Order by position name, vehicle, or vintage and closed for private investments
    z = []
    for i in y:
        a = x[x['Asset B Rank']==i]
        if vehicle_sort=='Yes':
            a = a.sort_values(by='Vehicle', ascending = True).reset_index(drop=True)
        if vintage_sort=='Yes':
            a = a.sort_values(by='Vintage', ascending = True).reset_index(drop=True)
        if closed_sort=='Yes':
            a = a.sort_values(by='Status', ascending = False).reset_index(drop=True) 
        if name_sort=='Yes':
            a = a.sort_values(by=name_header, ascending = True).reset_index(drop=True)
        z.append(a)
    x = pd.concat(z, ignore_index=True)  
        
    # Get index number
    x = x.reset_index(drop=True)
    x['index'] = list(x.index)
    x = x[x[name_header]==name]
    x = x.reset_index(drop=True)
    
    # Divide index by max number per page and round up to get page number
    y = int(math.ceil((x['index'][0]+1)/n))
    
    # Return result
    return y

    
# Get page numbers
o_pos['Page'] = o_pos.apply(lambda l: assign_page(o_pos,
                                                  l['Date'],
                                                  l['Position Name'],
                                                  'Position Name',
                                                  l['Grouping_pdf']), axis=1)

# Add to eur df
x = o_pos[['Date', 'Position Name', 'Page']]
x = x.drop_duplicates()
o_pos_eur = o_pos_eur.merge(x, on = ['Date', 'Position Name'], how='left')

# Print max page by group
x = o_pos[o_pos['Date']==end][['Grouping_pdf', 'Page', 'Position Name']].groupby('Grouping_pdf').agg(max).reset_index()
x=x[x['Page']>1]
print('The following asset groups require more than 1 position page:', x)





## PDF VERSION - ASSET CLASS DETAILS SHEETS

# New numbers of obs per page
n = 40

# Add relevant grouping split for PEs. Also to others for consistency
o_pe['Grouping_pdf'] = o_pe.apply(lambda l: edit_group(l['Asset Class A'],
                                                     l['Asset Class B']), axis=1)
o_hf['Grouping_pdf'] = 'Hedge Funds'
o_mkt['Grouping_pdf'] = 'Securities'

# Add Asset B ranks
y = pd.read_excel(source2, sheet_name='ENEB')
o_hf = o_hf.merge(y[['Asset B Rank', 'Asset Class B']],
            on = 'Asset Class B', how='left')
o_pe = o_pe.merge(y[['Asset B Rank', 'Asset Class B']],
            on = 'Asset Class B', how='left')
o_mkt = o_mkt.merge(y[['Asset B Rank', 'Asset Class B']],
            on = 'Asset Class B', how='left')


# Get pages
o_pe['Page'] = o_pe.apply(lambda l: assign_page(o_pe,
                                                  l['Date'],
                                                  l['Position Name'],
                                                  'Position Name',
                                                  l['Grouping_pdf'],
                                                  vehicle_sort='No',
                                                  vintage_sort='Yes',
                                                  closed_sort='Yes',
                                                  name_sort='No'), axis=1) 
                              
# Adjust for the fact that hfs have different datatypes
o_hf['Page'] = 1
x = o_hf[(o_hf['Datatype']=='Performance - Positions')&(o_hf['Date'].notnull())]
o_hf.loc[(o_hf['Datatype']=='Performance - Positions')&(o_hf['Date'].notnull()), 'Page'] = x.apply(lambda l: assign_page(x,
                                                  l['Date'],
                                                  l['Position Name'],
                                                  'Position Name',
                                                  l['Grouping_pdf'],
                                                  vehicle_sort='No'), axis=1)                               

o_mkt['Page'] = 1
x = o_mkt[o_mkt['Datatype']=='Position']                                    
o_mkt.loc[o_mkt['Datatype']=='Position', 'Page'] = x.apply(lambda l: assign_page(x,
                                                  l['Date'],
                                                  l['Position Name'],
                                                  'Position Name',
                                                  l['Grouping_pdf'],
                                                  vehicle_sort='No'), axis=1)                               
                                                                        
                                    
# Print max page by group
y =['Grouping_pdf', 'Page', 'Position Name', 'Date']
x = o_pe[y].append(o_hf[y])
x = x.append(o_mkt[y])
x = x[x['Date']==end][['Grouping_pdf', 'Page', 'Position Name']].groupby('Grouping_pdf').agg(max).reset_index()
x = x[x['Page']>1]
print('The following asset groups require more than 1 position page:', x)                                    
                                    
                                    
                                    
                                    






### ADD LOANS TO F_INV

# Create net flows
f_loan['Flow'] = f_loan['Inflows']-f_loan['Outflows']

# Add to f_inv
f_loan['Position Subname'] = f_loan['Position Name']
f_inv = f_inv.append(f_loan)









### CALCULATE GAINS REGISTERED CHANGES DURING MONTH

## Save current total gains in EUR
## Since some gains registered will appear in previous MTD (e.g. dated PE NAV updates)
## we must aggregate all MTD in the database

# by Position
x = o_pos_eur[['Position Name', 'MTD Gain']].groupby('Position Name').agg(sum).reset_index()
x.rename(columns={'MTD Gain': 'Total Gain'}, inplace=True)
y = df_det[['Position Name', 'Position ID']]
x = x.merge(y, on = 'Position Name', how ='left')    # add in position IDs to deal with name changes
totgain_pos = x.copy()

# by Asset Class B
x = o_classb_eur[['Asset Class B', 'MTD Gain']].groupby('Asset Class B').agg(sum).reset_index()
x.rename(columns={'MTD Gain': 'Total Gain'}, inplace=True)
totgain_b = x.copy()




## Get total gains from the previous month

# Set directory to last month data
os.chdir(dir_past)

# Create file prefixes and suffixes
version = ms_date.strftime('%d %b %y')                  #today = dt.datetime.today().strftime('%y%m%d')
x = f'{version} - Total Gain - Positions.csv'
y = f'{version} - Total Gain - Asset Classes.csv'

# Import previous month gains
x = pd.read_csv(x)[['Position ID', 'Total Gain']]
y = pd.read_csv(y)

# Change names of value columns
x.rename(columns={'Total Gain': 'Total Gain MS'}, inplace=True)
y.rename(columns={'Total Gain': 'Total Gain MS'}, inplace=True)

## Calculate change

# Join old dfs to new dfs (positions joined on Position IDs)
x = totgain_pos.merge(x, on='Position ID', how='left')
y = totgain_b.merge(y, on='Asset Class B', how='left')

# Calculate changes from past to current month
x = x.fillna(0)
y = y.fillna(0)
x['MTD Gain'] = x['Total Gain'] - x['Total Gain MS']
y['MTD Gain'] = y['Total Gain'] - y['Total Gain MS']

# Name change warnings

# Add Position details
# Identify if negative
x['Direction'] = x['MTD Gain'].apply(lambda l: 'Pos' if l>0 else 'Neg')
# Get absolute gain for ordering
x['Abs Gain'] = abs(x['MTD Gain'])

# Drop FX positions
a = df_det[['Position Name', 'Asset Class A']]
x = x.merge(a, on = 'Position Name', how='left')
x = x[x['Asset Class A'] != 'FX Hedging']

# Save position MTD
o_mtd_pos = x.copy()



# Add Class B details
# Assign FX/Investment identfiers
y['Asset Type'] = y['Asset Class B'].apply(lambda l: "FX" if l == 'FX Hedging' else 'Investments')

# Get MTD expenses
x = f_exp_eur[f_exp_eur['M End']==end]['Outflows EUR'].sum()

# Add into Asset B
z = {'Asset Class B': ['Expenses'],
     'MTD Gain': -x,
     'Asset Type': ['Expenses']}
x = pd.DataFrame(z)
y = y.append(x)

# Get asset appreciation due to FX
x = o_port_eur[o_port_eur['Date']==end][['MTD FX Conversion Gain', 'MTD FX Gain on Assets']]
x = x.reset_index(drop=True)
x = list(x.iloc[0])

# Add into Asset B
z = {'Asset Class B': ['Gain on FX Conversions', 'Gain on Foreign Assets due to FX'],
     'MTD Gain': x,
     'Asset Type': ['FX', 'FX']}
x = pd.DataFrame(z)
y = y.append(x)

# Save asset MTD
o_mtd_b = y.copy()









### CALCULATE FLOWS REGISTERED CHANGES DURING MONTH

# Import previous month flows df
x = f'{version} - Flows_Investments.csv'
x = pd.read_csv(x)

# Fix classes
x['Date'] = x['Date'].apply(lambda l: dt.datetime.strptime(l, '%Y-%m-%d').date())


# Get the entries that weren't included in the past month
# Based on flow date and flow value. Not flow names, just in case these have changed
y = x[['Date', 'Flow']]
z = f_inv[['Date', 'Position Name', 'Position Subname', 'Asset Class B',
           'Currency', 'Units Direction', 'Flow']]
z = z.append(y)

# Roundabout way to round to two decimal places because Pythin is being a dick
D = decimal.Decimal
z['Flow'] = z['Flow'].apply(lambda l: D(l).quantize(D('0.001'), rounding=decimal.ROUND_UP))
z['Flow'] = z['Flow'].astype(str)
z['Flow'] = z['Flow'].apply(lambda l: l[:-1])
z['Flow'] = z['Flow'].astype(float)

# Drop duplicates
z = z.drop_duplicates(['Date', 'Flow'], keep=False)
z = z.sort_values(by='Date', ascending = True).reset_index(drop=True)



## Summarise the remaining flows (that have been added in the past month)

# Loans are NA atm, because they were in a separate sheet
z['Asset Class B'] = z['Asset Class B'].fillna('Loans')

# Get total units and total flows
calcs = {'Units Direction': sum, 'Flow': sum}
x = z.groupby(['Position Name', 'Position Subname', 'Asset Class B',
               'Currency']).agg(calcs).reset_index()

# Calculate price
x['Av Price'] = -x['Flow']/x['Units Direction']
x['Av Price'] = x.apply(lambda l: l['Av Price'] *
                        (100 if l['Asset Class B'] == 'Bonds' else 1),
                        axis=1)

# Remove price for expiring forwards because not too useful + can create weirdly
# large prices if forward shorts and longs have been combined
x.loc[x['Asset Class B']=='FX Forwards', 'Term Amount'] = float('nan')



## Create the sentence structures
# Add in Asset Class A
x = x.merge(df_det[['Position Name', 'Asset Class A']],
            on = ['Position Name'], how = 'left')

# Name actions
def get_action(assetclassb, assetclassa, flow, units, fx_flow='Yes'):
    if flow > 0:
        if assetclassa == 'Private Investments':
            return 'Distribution'
        elif assetclassa == 'Liquidity':
            return 'Borrowed'
        elif (assetclassb == 'FX Forwards')&(fx_flow=='Yes'):
            return 'Expiration'
        elif units == 0:
            if assetclassa in ['Stocks', 'Hedge Funds']:
                return 'Dividend'
            if assetclassa in ['Liquid Fixed Income']:
                return 'Coupon'
            else:
                return 'Sell'         
        else:
            return 'Sell'
    else:
        if assetclassb in ['Private Equity Funds', 'Private Credit & Funds']:
            return 'Call'
        elif assetclassb in ['Real Estate', 'Co-Investments']:
            return 'Investment'
        elif assetclassa == 'Liquidity':
            return 'Repayment'
        elif (assetclassb == 'FX Forwards')&(fx_flow=='Yes'):
            return 'Expiration'
        elif flow == 0:
            if assetclassa == 'Derivatives':
                return 'Expiration'
            else:
                return 'Buy'  
        else:
            return 'Buy'
        
x['Action'] = x.apply(lambda l: get_action(l['Asset Class B'],
                                           l['Asset Class A'],
                                           l['Flow'],
                                           l['Units Direction']), axis=1)
o_mtd_f = x.copy()        





## Add in derivative expiries and strike
o_mtd_f = o_mtd_f.merge(n_der[['Position Subname', 'Strike', 'Expiry']].drop_duplicates(),
                        on = 'Position Subname',
                        how='left')




## New forwards

# Get MS forwards
x = f'{version} - FX Positions.csv'
x = pd.read_csv(x)

# Get columns needed from last month's forwards
z = ['Asset Class B', 'Position Name', 'Position Subname', 'Currency', 'Term Amount',
     'Forward Price']
x = x[z]
x['MS Fwd x Term'] = x['Forward Price'] * x['Term Amount']
x = x.drop('Forward Price', axis=1)
x = x.groupby(['Asset Class B', 'Position Name', 'Position Subname', 'Currency']).agg(sum).reset_index()

# Get data for current forwards
z.append('Expiry')
y = o_fx[(o_fx['Date']==end)][z]
y['Fwd x Term'] = y['Forward Price'] * y['Term Amount']

calcs = {'Expiry': lambda l: l.mode(),
         'Term Amount': sum,
         'Fwd x Term': sum}

y = y.groupby(['Asset Class B', 'Position Name', 'Position Subname', 'Currency']).agg(calcs).reset_index()

# Merge MTD and current forward details
x.rename(columns={'Term Amount': 'MS Term Amount'}, inplace=True)
x = y.merge(x, on = ['Asset Class B', 'Position Name', 'Position Subname', 'Currency'], how='left')
x= x.fillna(0)

# Get MTD changes
x['Flow'] = x['Term Amount'] - x['MS Term Amount']
x = x[x['Flow']!=0]
x['Av Price'] = abs(x['Fwd x Term']-x['MS Fwd x Term'])/abs(x['Flow'])

# Add Asset Classes A and B
x['Asset Class A'] = 'FX Hedging'
x['Units Direction'] = float('nan')

# Determine action labels
if len(x) >0:
    
    # Make action
    x['Action'] = x.apply(lambda l: get_action(l['Asset Class B'],
                                               l['Asset Class A'],
                                               l['Flow'],
                                               l['Units Direction'],
                                               fx_flow = 'No'), axis=1)
    
    # Identify as Term Amounts so they don't get mixed with CFs
    x = x.drop('Term Amount', axis=1)
    x.rename(columns={'Flow': 'Term Amount'}, inplace=True)
    
    # Get expired FX forwards
    y = y[(y['Expiry']<=end)&(y['Expiry']>ms_date)]
    y['Asset Class A'] = 'FX Hedging'
    y['Units Direction'] = float('nan')
    y['Action'] = 'Expiration'
    
    
    # Merge in
    x = x.append(y)
    o_mtd_f = o_mtd_f.append(x)

# Pass if there are no new forwards (create Term Amount column to keep consistency for Tableau)
else:
    o_mtd_f['Term Amount'] = float('nan')

# Change term amount to float
o_mtd_f['Term Amount'] = o_mtd_f['Term Amount'].astype(float)

# Set directory to back to current month
os.chdir(dir_current)







### GET CURRENCY PRICES
x = n_eq['Position Name'].apply(lambda l: l[:3] in currencies)
o_curr = n_eq[x][['Date', 'Position Name', 'Price']]







### DROP VALUES PRE DEC 2021
o_sub = o_sub[o_sub['Date']>=limit3]
o_sub_eur = o_sub_eur[o_sub_eur['Date']>=limit3]
o_pos_eur = o_pos_eur[o_pos_eur['Date']>=limit3]
o_pos = o_pos[o_pos['Date']>=limit3]
o_classb_local = o_classb_local[o_classb_local['Date']>=limit3]
o_classb_eur = o_classb_eur[o_classb_eur['Date']>limit3]
o_free = o_free[o_free['Date']>=limit3]
o_geo = o_geo[o_geo['Date']>=limit3]
o_look = o_look[o_look['Date']>=limit3]












### COMBINE LOCAL AND EURO DFS FOR SOME DFS
# Prevents issues with Tableau unions

o_sub_all = o_sub.append(o_sub_eur)
o_pos_all = o_pos.append(o_pos_eur)
o_class_all = o_classb_local.append(o_classb_eur)





### CHECK AND PRINT ERRORS
# Print which position is causing the error in the formulas
# Check position names and subnames are consistently spelled in terms of upper/lower
# ^^ get unique positions names -> make all lowercase -> check duplicates






### WRITE FILES TO HISTORICAL BACK-UP FOLDERS

# Create file prefixes and suffixes
folder = end.strftime('%Y %m')
version = end.strftime('%d %b %y')                  #today = dt.datetime.today().strftime('%y%m%d')
dfnames = ""

# Create folder
newpath = folder + '/0 - Transformed Data'
if not os.path.exists(newpath):
    os.makedirs(newpath)

# Select and name files to export
dfnames = {'o_classb_local': 'Class B Aggregate Local',
           'o_classb_eur': 'Class B Aggregate EUR',
           'o_class_all': 'Class B - all',
           'o_port_h': 'Portfolio Perf Full Historical',
           'o_sub': 'Subpositions', 
           'o_sub_eur': 'Subpositions EUR',
           'o_sub_all': 'Subpositions - all',
           'o_pos': 'Positions',
           'o_pos_eur': 'Positions_EUR',
           'o_pos_all': 'Positions - all',
           'o_geo': 'Geographies',
           'o_geo_agg': 'Geographies_Agg',
           'o_pe': 'PE Summary',
           'o_pe_agg': 'PE Aggregate',
           'o_hf': 'HF Summary',
           'f_inv': 'Flows_Investments',
           'o_mkt': 'Marketable Securities',
           'o_fx': 'FX Positions',
           'o_look': 'Lookthrough',
           'o_fx_sched': 'FX Schedule',
           'o_free': 'Free Liquidity',
           'gain_brk': 'YTD Gain Breakdown',
           'o_vehicle': 'Aggregated Returns',
           'df_det': 'Position Details',
           'totgain_pos': 'Total Gain - Positions',
           'totgain_b': 'Total Gain - Asset Classes',
           'o_mtd_pos': 'MTD Gain - Positions',
           'o_mtd_b': 'MTD Gain - Asset Classes',
           'o_mtd_f': 'MTD Flows',
           'o_exp': 'Expenses',
           'o_curr': 'Currencies',
           'KSHvBench': 'KSH v Benchmarks'}

# Export files
for i in list(dfnames.keys()):
    name = os.path.join(newpath, version + ' - ' + dfnames[i] +'.csv')
    x = locals()[i]
    x.to_csv(name, index = False)




### WRITE FILES TO CURRENT DATA FOLDER

# Export files
for i in list(dfnames.keys()):
    name = os.path.join('0 - Current Data', dfnames[i] +'.csv')
    x = locals()[i]
    x.to_csv(name, index = False)