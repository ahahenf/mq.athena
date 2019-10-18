import pandas as pd
import numpy as np
import sys
import datetime
from datetime import timedelta
import warnings
warnings.simplefilter('ignore')

##ENTER MONTH AND DATES HERE 
mmm = input("Enter Month: ")
ddd = input("Enter Day: ")


#this is to format input and make it all the same

from datetime import datetime as dt

def parse_month(mmm):
#     parse month abbr
    try:
        return dt.strptime(mmm, '%b').strftime('%m')
    except ValueError:
        pass
            
#     parse month full name 
    try: 
        return dt.strptime(mmm, '%B').strftime('%m')
    except ValueError:
        pass
    
#     parse month digit
    return f'{int(mmm):02}'


dd = f'{int(ddd):02}'

mm = parse_month(mmm)
#dd = parse_day(ddd)


#filepaths to grab files & output files

path2 = '/Users/ahahnenfeld/Box/Settlement/Financial Rpt Recon/Consolidated Reporting/2019/Automated_Recon_Output/PULSE/'
path = path2 + mm + '/'

path3 = '/Users/ahahnenfeld/Box/Settlements Ad-Hoc/Oh No Tableau/Pin_Detail/2019/'+ mm+ '/'

path4 = '/Users/ahahnenfeld/Box/Settlement/Financial Rpt Recon/Consolidated Reporting/Report - Networks/NETWORK - PULSE/D10A/'


#finds D10A file name and then creates a variable with the name - then this is ingested in a data frame using pandas

import glob

globpath = '/Users/*/Box/Settlement/Financial Rpt Recon/Consolidated Reporting/Report - Networks/NETWORK - PULSE/D10A/*/*/'


all_files = glob.glob(globpath+ 'D19'+mm+dd+'.PL.HXS.D10A.PRC643.csv')

path_new = all_files[0]

df1 = pd.read_csv(path_new, 
                  dtype = {'swDateTime':'object'
                           ,'posEntryMode':'object'
                           ,'lastFour':'object'
                          ,'stan':'object'
                          ,'acquirerCurrencyCode':'object'})



df1.program = df1.program.str.replace('Kabbage V2','Kabbage2')
df1 = df1[df1.impact != "None"]
df1.fee.fillna(0, inplace=True)
df1.loc[df1.impact == 'Credit', 'balancemultiplier'] = -1
df1.loc[df1.impact=='Debit', 'balancemultiplier'] = 1

df1['total1'] = (df1.amount + df1.fee)*df1.balancemultiplier

df1['locDateTime'] = df1['locDateTime'].str[-8:]
df1 = df1[df1.total1 != 0]
df1.total1 = df1.total1.round(2)

df2 = pd.read_csv(path3 + '2019_'+mm+'_'+dd+'_pulse_pin_detail.csv', 
                  dtype = {'last_four':'object'
                           ,'stan':'object'})


#replace null with zero amount so i can easily filter them out, zero values are no impact

df2.total.fillna(0, inplace=True)

df2 = df2[df2.total != 0]
df2.batchnumber.fillna(0, inplace=True)
df2 = df2[df2.batchnumber == 0]
#df2.batchnumber.unique()
df2.total = df2.total.round(2)
df2.total = df2.total * -1

df2['stan'] = df2['stan'].str[-6:]
df2['localtransactiondate'] = df2['localtransactiondate'].str[-8:]

df1['key'] = df1["program"] + "-" + df1["lastFour"].map(str) + "-" + df1["stan"] + "-" + df1["locDateTime"]+ "-" + df1['total1'].map(str)
df2['key'] = df2['program'] + "-" + df2['last_four'].map(str) + "-" + df2['stan'] + "-" + df2['localtransactiondate'] + "-" + df2['total'].map(str)

df1_match1 = df1[['key','total1']].groupby(['key']).sum()
df2_match1 = df2[['key','total']].groupby(['key']).sum()

df1_match1['total1'] = df1_match1.total1.round(2)
df2_match1['total'] = df2_match1.total.round(2)
#lastfour_list3 = lastfour_list2.last_four.unique().tolist()

#used these to check values 
#df1_match1['total1'].sum().round(2) - df1.total1.sum().round(2)

#df2_match1.total.sum()-df2.total.sum()


match1_merge = pd.merge(df1_match1, df2_match1, on='key', how='outer')
match1_merge['total1'].fillna(0, inplace=True)
match1_merge['total'].fillna(0, inplace=True)
match1_merge['net'] = match1_merge['total1'] - match1_merge['total']
match1_merge['net'] = match1_merge['net'].round(2)

m1m_filter = match1_merge[match1_merge['net'] != 0]
match_list1 = m1m_filter.index.tolist()

df5 = df1[df1['key'].isin(match_list1)]
df6 = df2[df2['key'].isin(match_list1)]


df5['key'] = df5["program"] + "-" + df5["lastFour"].map(str) + "-" + df5["stan"]
df6['key'] = df6['program'] + "-" + df6['last_four'].map(str) + "-" + df6['stan'] 


df6['dup_diva'] = df6.duplicated(subset='key',keep='first')
df5['dup_d10'] = df5.duplicated(subset='key',keep='first')


df5_match1 = df5[['key','total1']].groupby(['key']).sum()
df6_match1 = df6[['key','total']].groupby(['key']).sum()

df5_match1['total1'] = df5_match1.total1.round(2)
df6_match1['total'] = df6_match1.total.round(2)

match2_merge = pd.merge(df5_match1, df6_match1, on='key', how='outer')

match2_merge['total1'].fillna(0, inplace=True)
match2_merge['total'].fillna(0, inplace=True)
match2_merge.head(5)

match2_merge['net'] = match2_merge['total1'] - match2_merge['total']
match2_merge.net.sum()

m2m_filter = match2_merge[match2_merge['net'] != 0]
match_list2 = m2m_filter.index.tolist()

df3 = df5[df5['key'].isin(match_list2)]
df4 = df6[df6['key'].isin(match_list2)]


#df3 = df3.append(df5_duplicate, 'sort=True')
#df4 = df4.append(df6_duplicate, 'sort=True')
#df3.total1.sum()-df4.total.sum()

#to eliminate reversals we need to make a key without the use of total and then check which of those keys net out to zero WITH the total
#then make a list to filter out through remaining data
#this is only needed for MQ data as it shows reversals

df4['rkey'] = df2['program'] + "-" + df2['last_four'].map(str) + "-" + df2['stan'] + "-" + df2['localtransactiondate']


df4_rkey = df4[['rkey','total']].groupby(['rkey']).sum()

df4_rkey2 = df4_rkey[df4_rkey['total'] != 0]

rkey_list = df4_rkey2.index.tolist()


df4_r = df4[df4['rkey'].isin(rkey_list)]
###need to put in another check here after reversals are eliminated. this is to capture fees that did have
###reversals and it messed up the first key/list

df4_r
#next we need to extract all transactions that are equal on all fields with the exception of "fee" 
#this we will need another key but this one will exclude total BUT use amount

df4_r['amount2'] = df4_r.purchases + df4_r.refunds
df4_r.amount2 = df4_r.amount2.abs()
#df4_r.total = df4_r.total * -1

df3['key2'] = df3["lastFour"].map(str) + "-" + df3["amount"].map(str) + "-" + df3["locDateTime"]
df4_r['key2'] = df4_r["last_four"].map(str) + "-" + df4_r["amount2"].map(str) + "-" + df4_r["localtransactiondate"]


df_fees = pd.merge(df3, df4_r, on='key2', how='outer')
df_fees.fillna(0, inplace=True)


overpost = df_fees[df_fees['bin'] == 0]
f2p = df_fees[df_fees['bank'] == 0]
#overpost
#f2p

fees = df_fees[df_fees['total'] - df_fees['total1'] != 0]
fees = fees[fees['bin'] != 0]
fees = fees[fees['bank'] != 0]

duplicates = df_fees[df_fees['total'] - df_fees['total1'] == 0]

overpost = df_fees[df_fees['bin'] == 0]
f2p = df_fees[df_fees['bank'] == 0]
#overpost
#f2p

fees = df_fees[df_fees['total'] - df_fees['total1'] != 0]
fees = fees[fees['bin'] != 0]
fees = fees[fees['bank'] != 0]
#hell yeah fees
#fees.head()

other = df_fees[df_fees['total'] - df_fees['total1'] == 0]

outage1 = other[['bin',
'lastFour',
'program_x',
'transactionType',
'amount',
'impact',
'swDateTime',
'mid',
'terminalId',
'stan_x',
'exceptionCode',
'rejectCode',
'fee',
'feeImpact',
'locDateTime',
'cardBank',
'pulseTerminalNumber',
'sequenceNumber',
'validation',
'posEntryMode',
'acquirerCurrencyCode',
'acquirerCurrencyAmount',
'conversionRate',
'issuerNetworkId',
'acquirerNetworkId',
'caStreet',
'caCity',
'caState',
'caCountry',
'caName',
'total1',
'key2',
'dup_d10']]

outage2 = other[['bank',
'settlementdate',
'pin_network',
'batchnumber',
'program_y',
'bin_description',
'tranlog_id',
'tranlogdate',
'last_four',
'merchant',
'reversal',
'reversalid',
'networkreferenceid',
'retrievalreferencenumber',
'stan_y',
'localtransactiondate',
'purchases',
'refunds',
'pin_debi_assoc_fees',
'total',
'interchange',
'amount2',
'key2',
'dup_diva']]

outage1_duplicates = outage1[outage1['dup_d10'] == True]
outage2_duplicates = outage2[outage2['dup_diva'] == True]

outage1_duplicates = outage1_duplicates[['bin',
'lastFour',
'program_x',
'transactionType',
'amount',
'impact',
'swDateTime',
'mid',
'terminalId',
'stan_x',
'exceptionCode',
'rejectCode',
'fee',
'feeImpact',
'locDateTime',
'cardBank',
'pulseTerminalNumber',
'sequenceNumber',
'validation',
'posEntryMode',
'acquirerCurrencyCode',
'acquirerCurrencyAmount',
'conversionRate',
'issuerNetworkId',
'acquirerNetworkId',
'caStreet',
'caCity',
'caState',
'caCountry',
'caName',
'total1',
'key2']]

outage2_duplicates = outage2_duplicates[['bank',
'settlementdate',
'pin_network',
'batchnumber',
'program_y',
'bin_description',
'tranlog_id',
'tranlogdate',
'last_four',
'merchant',
'reversal',
'reversalid',
'networkreferenceid',
'retrievalreferencenumber',
'stan_y',
'localtransactiondate',
'purchases',
'refunds',
'pin_debi_assoc_fees',
'total',
'interchange',
'amount2',
'key2']]



overpost = df_fees[df_fees['bin'] == 0]
f2p = df_fees[df_fees['bank'] == 0]
#overpost
#f2p

fees = df_fees[df_fees['total'] - df_fees['total1'] != 0]
fees = fees[fees['bin'] != 0]
fees = fees[fees['bank'] != 0]
#hell yeah fees
#fees.head()

other = df_fees[df_fees['total'] - df_fees['total1'] == 0]

outage1 = other[['bin',
'lastFour',
'program_x',
'transactionType',
'amount',
'impact',
'swDateTime',
'mid',
'terminalId',
'stan_x',
'exceptionCode',
'rejectCode',
'fee',
'feeImpact',
'locDateTime',
'cardBank',
'pulseTerminalNumber',
'sequenceNumber',
'validation',
'posEntryMode',
'acquirerCurrencyCode',
'acquirerCurrencyAmount',
'conversionRate',
'issuerNetworkId',
'acquirerNetworkId',
'caStreet',
'caCity',
'caState',
'caCountry',
'caName',
'total1',
'key2']]

outage2 = other[['bank',
'settlementdate',
'pin_network',
'batchnumber',
'program_y',
'bin_description',
'tranlog_id',
'tranlogdate',
'last_four',
'merchant',
'reversal',
'reversalid',
'networkreferenceid',
'retrievalreferencenumber',
'stan_y',
'localtransactiondate',
'purchases',
'refunds',
'pin_debi_assoc_fees',
'total',
'interchange',
'amount2',
'key2']]

outage2 = outage2.drop_duplicates(subset=['tranlog_id'],keep='last')


outage1['key3'] = outage1["lastFour"].map(str) + "-" + outage1["amount"].map(str) + "-" + outage1["locDateTime"] + "-" + outage1["caName"]
outage2['key3'] = outage2["last_four"].map(str) + "-" + outage2["amount2"].map(str) + "-" + outage2["localtransactiondate"] + "-" + outage2['merchant']


inefficient = pd.merge(outage1, outage2, on='key3', how='outer')
inefficient.fillna(0, inplace=True)
inefficient.dtypes

ie_real = inefficient[inefficient['total1']-inefficient['total'] != 0]
ie_real

f2p_final = ie_real[['bin',
'lastFour',
'program_x',
'transactionType',
'amount',
'impact',
'swDateTime',
'mid',
'terminalId',
'stan_x',
'exceptionCode',
'rejectCode',
'fee',
'feeImpact',
'locDateTime',
'cardBank',
'pulseTerminalNumber',
'sequenceNumber',
'validation',
'posEntryMode',
'acquirerCurrencyCode',
'acquirerCurrencyAmount',
'conversionRate',
'issuerNetworkId',
'acquirerNetworkId',
'caStreet',
'caCity',
'caState',
'caCountry',
'caName',
'total1']]


f2p2 = f2p[['bin',
'lastFour',
'program_x',
'transactionType',
'amount',
'impact',
'swDateTime',
'mid',
'terminalId',
'stan_x',
'exceptionCode',
'rejectCode',
'fee',
'feeImpact',
'locDateTime',
'cardBank',
'pulseTerminalNumber',
'sequenceNumber',
'validation',
'posEntryMode',
'acquirerCurrencyCode',
'acquirerCurrencyAmount',
'conversionRate',
'issuerNetworkId',
'acquirerNetworkId',
'caStreet',
'caCity',
'caState',
'caCountry',
'caName',
'total1']]


overpost2 = overpost[['bank',
'settlementdate',
'pin_network',
'batchnumber',
'program_y',
'bin_description',
'tranlog_id',
'tranlogdate',
'last_four',
'merchant',
'reversal',
'reversalid',
'networkreferenceid',
'retrievalreferencenumber',
'stan_y',
'localtransactiondate',
'purchases',
'refunds',
'pin_debi_assoc_fees',
'total',
'interchange',
'amount2']]


overpost_final = ie_real[['bank',
'settlementdate',
'pin_network',
'batchnumber',
'program_y',
'bin_description',
'tranlog_id',
'tranlogdate',
'last_four',
'merchant',
'reversal',
'reversalid',
'networkreferenceid',
'retrievalreferencenumber',
'stan_y',
'localtransactiondate',
'purchases',
'refunds',
'pin_debi_assoc_fees',
'total',
'interchange',
'amount2']]

outage2_duplicates = outage2_duplicates[['bank',
'settlementdate',
'pin_network',
'batchnumber',
'program_y',
'bin_description',
'tranlog_id',
'tranlogdate',
'last_four',
'merchant',
'reversal',
'reversalid',
'networkreferenceid',
'retrievalreferencenumber',
'stan_y',
'localtransactiondate',
'purchases',
'refunds',
'pin_debi_assoc_fees',
'total',
'interchange',
'amount2']]

overpost2 = overpost2[['bank',
'settlementdate',
'pin_network',
'batchnumber',
'program_y',
'bin_description',
'tranlog_id',
'tranlogdate',
'last_four',
'merchant',
'reversal',
'reversalid',
'networkreferenceid',
'retrievalreferencenumber',
'stan_y',
'localtransactiondate',
'purchases',
'refunds',
'pin_debi_assoc_fees',
'total',
'interchange',
'amount2']]

overpost_final = overpost_final[overpost_final['bank'] != 0]
#overpost_final

f2p_final = f2p_final[f2p_final['bin'] != 0]
#f2p_final
f2p_final = f2p_final.append(f2p2)
#f2p_final = f2p_final.append(outage1_duplicates)
overpost_final = overpost_final.append(overpost2)
overpost_final = overpost_final.append(outage2_duplicates)
overpost_final['total'] = overpost_final['total'] * -1
overpost_final

#this copies the column order from overpost2 b/c when you append it changes it to alphabetical sort 
overpost_final = overpost_final[overpost2.columns]
f2p_final = f2p_final[f2p2.columns]


###THREESETI INITIAL ADDED ON HERE - was lazy - it will create more dataframes from the same CSVs 
#rewrite this you lazy piece of shit

pdetail = pd.read_csv(path3+'2019_'+mm+'_'+dd+'_pulse_pin_detail.csv', dtype = {'acquirerCurrencyCode':'object'})
pd10a = pd.read_csv(path_new, dtype = {'swDateTime':'object'})

pd10a.fee.fillna(0, inplace=True)
pd10a.loc[pd10a.impact == 'Credit', 'balancemultiplier'] = -1
pd10a.loc[pd10a.impact=='Debit', 'balancemultiplier'] = 1
pd10a.loc[pd10a.impact=='None', 'balancemultiplier'] = 0
pd10a['program'] = pd10a['program'].replace({'Master Card Demo':'MasterCard Demo'})


pd10a['total'] = (pd10a.amount + pd10a.fee)*pd10a.balancemultiplier


pdetail = pdetail[pdetail.total != 0]
pdetail.batchnumber.fillna(0, inplace=True)
pdetail = pdetail[pdetail.batchnumber == 0]

pd10a = pd10a[pd10a.total != 0]

pd10a = pd10a[['bin','lastFour','program','total']]
pdetail = pdetail[['bank','program','last_four','total']]

pdetail_sutton = pdetail[pdetail.bank == 'Sutton']
pd10a_sutton = pd10a[pd10a.bin.isin([440393,539186,601198,520711,428803,441303,601198,416250])]


pd10a_sutton = pd10a_sutton[['lastFour','program','total']]
pdetail_sutton = pdetail_sutton[['last_four','program','total']]

#pdetail_sutton['Total'] = pdetail_sutton.total
#pdetail_sutton['Program'] = pdetail_sutton.program


pd10a_sutton[['total','program']].groupby(['program']).sum().to_csv('D10A_Drawdown.csv')
pdetail_sutton[['total','program']].groupby(['program']).sum().to_csv('Detail_Drawdown.csv')
pd10a_sutton.total.sum() + pdetail_sutton.total.sum()


dfrecon1 = pd.read_csv('D10A_Drawdown.csv')
dfrecon2 = pd.read_csv('Detail_Drawdown.csv')

df_merge = pd.merge(dfrecon1, dfrecon2, on='program', how='outer')
df_merge.fillna(0, inplace=True)
df_merge['df_net'] = df_merge.total_x + df_merge.total_y
df_merge.total_x.round(2)
df_merge.total_y.round(2)
df_merge.df_net = df_merge.df_net.round(2)
df_merge.to_csv('0_'+mm+'_'+dd+'_threeseti_initial.csv')

df_merge.df_net = df_merge.df_net.round(2)


overpost_final.settlementdate = overpost_final.settlementdate.str.replace('-','/')
overpost_final['settlementdate'] = pd.to_datetime(overpost_final['settlementdate']).dt.strftime('%-m/%-d/%y')


fees.settlementdate = fees.settlementdate.str.replace('-','/')
fees['settlementdate'] = pd.to_datetime(fees['settlementdate']).dt.strftime('%-m/%-d/%y')

fees = fees.drop(columns='balancemultiplier')
fees = fees.drop(columns='key_x')
fees = fees.drop(columns='dup_d10')
fees = fees.drop(columns='Unnamed: 0')
fees = fees.drop(columns='dup_diva')
fees = fees.drop(columns='rkey')
fees = fees.drop(columns='amount2')
fees.total = fees.total* -1

with pd.ExcelWriter(path +mm+'_'+dd+'_'+'Pulse_Sutton_Recon.xlsx') as writer:
    df_merge.to_excel(writer, sheet_name='Pulse_Sutton_DD')
    f2p_final.to_excel(writer, sheet_name='Pulse_Sutton_F2P')
    overpost_final.to_excel(writer, sheet_name='Pulse_Sutton_Overposts')
    fees.to_excel(writer, sheet_name='Pulse_Sutton_Fees')
    
now = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')

print("Pulse report ", mm+'_'+dd+' ', " run completed at ", now)

total_difference = f2p_final.total1.sum() + overpost_final.total.sum() + fees.total1.sum() + fees.total.sum()
total_difference.round(2)

total_difference = total_difference.round(2)

total_difference = str(total_difference)

print('Total Difference: ' + total_difference)
plsbezero = df_merge.df_net.sum().round(2) -(f2p_final.total1.sum() + overpost_final.total.sum() + fees.total1.sum() +fees.total.sum()).round(2)
plsbezero = str(plsbezero)

print('Difference Check: ' + plsbezero)