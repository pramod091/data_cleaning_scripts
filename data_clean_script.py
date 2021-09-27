import numpy as np
import pandas as pd
import csv
import os

## SALES FILE
# pd.options.display.float_format = '{:.0f}'.format
# pd.options.display.float_format = '{:.2f}'.format
# pd.options.display.float_format = '{:20,.2f}'.format
# Read data from csv

data_1 = pd.read_csv('../Music/salesData/ARV_USPA_XSTORE_SALE_13Jul2021.csv_1.csv')
data_2 = pd.read_csv('../Music/salesData/ARV_USPA_XSTORE_SALE_13Jul2021.csv_2.csv')
data_3 = pd.read_csv('../Music/salesData/ARV_USPA_XSTORE_SALE_13Jul2021.csv_3.csv')
data_4 = pd.read_csv('../Music/salesData/ARV_USPA_XSTORE_SALE_13Jul2021.csv_5.csv')
data_5 = pd.read_csv('../Music/salesData/ARV_USPA_XSTORE_SALE_16Jul2021-1.csv')

# Merge multiple file

frames = [data_1, data_2, data_3, data_4, data_5 ] # Add more data
# frames = [data_1]
sales_data = pd.concat(frames)
# Ensure Column L (INVOICEDATE) in range

sales_data['INVOICEDATE'] = pd.to_datetime(sales_data['INVOICEDATE'])
sales_data = sales_data[( '2019-04-01' <= sales_data['INVOICEDATE']) & (sales_data['INVOICEDATE'] <= '2020-02-28')]
import pdb;pdb.set_trace()
# Drop columns
drop = [ 'ITEM', 'DISCOUNT', 'TAXABLE AMOUNT', 'NETAMT', 'BEGIN_DATE_TIME', 'END_DATE_TIME', 'CAPILLARY_REF_NO','SUPPLIERSTYLE', 'SEASON', 'STORE_TYPE', 'OWNERSHIP_TYPE', 'SAP_STORECODE', 'COMPANY', 'NAME', 'AIL_ORDER_ID', 'DAY', 'BRAND', 'SUPPLIERQUALITY', 'MATERIALTYPE', 'MATERIALTYPE', 'SUPPLIERSIZE', 'COLOR', 'SIZE', 'GENDER', 'BASICCORE', 'MANUALDISCREASON', 'TAXRATE', 'TAXAMT', 'External_system', 'ORDER']
sales_data = sales_data.drop(drop, axis = 1)
# Filter Columns

sales_data = sales_data[sales_data['CLASS'] == 'MENS']
sales_data = sales_data[sales_data['SUBCLASS'].isin(['Shirt', 'T-Shirt', 'Trouser', 'Jeans']) ]

# Remove Duplicate

duplicate = sales_data[sales_data.duplicated(['INVOICENO', 'INVOICEDATE', 'BARCODE'])]
sales_data = sales_data.drop_duplicates(['INVOICENO', 'INVOICEDATE', 'BARCODE'])

#Save Duplicate Values
duplicate.to_csv('sales_duplicate.csv')

# Save the data
sales_data.to_csv('sales_data.csv')

## ARTICLE FILE
import glob

all_files = glob.glob("Article_master/*.csv")
li = []
for filename in all_files:
    df = pd.read_csv(filename)
    li.append(df)

article_data = pd.concat(li)

# Drop columns

drop = ['Old Article No', 'Sec Size', 'Sale Price', 'HSN', 'Weave', 'Design', 'Comp', 'Collar', 'Comp', 'Collar', 'Basic Mat', 'Theme Desc', 'Pattern',	'Coll Desc', 'Prod Memo', 'Val Type', 'Curr', 'Cat Val', 'Manufacturer', 'Fit', 'Target', 'Temp', 'Cogs', 'Sub Item Quality', 'Entity', 'Merchandise Hierarchy L1',	'Merchandise Hierarchy L2 ',	'Article Status']
article_data = article_data.drop(drop, axis = 1)

# Save the data
article_data.to_csv('article_data.csv')

# Merge the sales and articlee file
result = pd.merge( sales_data, article_data[['Season', 'Color', 'Size', 'Season Yr' , 'EAN No']],how='inner',left_on=['BARCODE'],right_on=['EAN No'])
result = result.drop( ['EAN No'], axis = 1)

# Create Seperate columns
result['INVOICETYPE_SALES'] = np.where(result['INVOICETYPE']=='SALES',result['QUANTITY'],0)
result['INVOICETYPE_SALES RETURN'] = np.where(result['INVOICETYPE']=='SALES RETURN',abs(result['QUANTITY']), 0)
result = result.drop(['INVOICETYPE', 'QUANTITY'], axis=1)

# Map City to Code
city_data = pd.read_csv('Xstore code and city tierv2.csv')
city_data['XSTORE_CODE'] = city_data['XSTORE_CODE'].astype('int64')

print(result.shape)
result = pd.merge( result, city_data[['XSTORE_CODE','Tier', 'CITY']],how='inner',left_on=['XSTORE_STORECODE'],right_on=['XSTORE_CODE'])
result = result.drop( ['XSTORE_STORECODE'], axis = 1)
print(result.shape)
result['STATE'] = result['STATE'].replace('WEST BEGAL', 'WEST BENGAL')
result['STATE'] = result['STATE'].replace('JAMMU AND KASHMIR', 'JAMMU & KASHMIR')
result['STATE'] = result['STATE'].replace('TELANGANA', 'TELENGANA')
result['STATE'] = result['STATE'].replace('TAMILNADU', 'TAMIL NADU')
result['STATE'] = result['STATE'].replace('CHHATTISGARH', 'CHATTISGARH')
result['STATE'] = result['STATE'].replace('Rajasthan', 'RAJASTHAN')

data = {'REGIONNEW': ['SOUTH','SOUTH','SOUTH','SOUTH','SOUTH','SOUTH','NORTH', 'NORTH', 'NORTH', 'NORTH', 'NORTH', 'NORTH', 'NORTH', 'NORTH', 'NORTH', 'NORTH', 'NORTH', 'NORTH','EAST', 'EAST', 'EAST', 'EAST', 'EAST', 'EAST', 'EAST', 'EAST', 'EAST','WEST', 'WEST', 'WEST', 'WEST'],
        'STATE': ['ANDHRA PRADESH', 'KERALA', 'KARNATAKA', 'TAMIL NADU', 'TELENGANA', 'PONDICHERRY', 'CHANDIGARH', 'UTTAR PRADESH', 'NEW DELHI', 'HARYANA', 'PUNJAB', 'UTTARAKHAND', 'HIMACHAL PRADESH', 'JAMMU & KASHMIR', 'MADHYA PRADESH', 'BIHAR', 'CHATTISGARH', 'JHARKHAND', 'ASSAM', 'WEST BENGAL', 'MANIPUR', 'MEGHALAYA', 'NAGALAND', 'SIKKIM', 'TRIPURA', 'ARUNACHAL PRADESH', 'ORISSA', 'GUJARAT', 'MAHARASHTRA', 'GOA', 'RAJASTHAN']}
region_df = pd.DataFrame.from_dict(data)
result = pd.merge( result, region_df[['STATE','REGIONNEW']],how='inner',left_on=['STATE'],right_on=['STATE'])
result = result.drop( ['REGION'], axis = 1)
result.rename(columns = {'REGIONNEW': 'REGION'}, inplace = True)
print(result.shape)
result['MRP'] = result['MRP'].astype(int).abs()

# Last month data
temp = result.groupby([result['INVOICEDATE'].dt.strftime('%Y-%m'),'XSTORE_CODE', 'BARCODE'])[['INVOICETYPE_SALES','INVOICETYPE_SALES RETURN']].sum().reset_index()

temp_clone = temp.copy(deep = True)
temp.rename(columns = {'INVOICETYPE_SALES': 'LAST_MONTH_SALES', 'INVOICETYPE_SALES RETURN':'LAST_MONTH_SALES_RETURN' }, inplace = True)
temp['INVOICEDATE'] = pd.to_datetime(temp['INVOICEDATE'])

final_data = pd.merge(result, temp, how='left',left_on=[pd.to_datetime((result['INVOICEDATE'] - pd.DateOffset(months=1)).dt.strftime('%Y-%m')+'-01'),'XSTORE_CODE', 'BARCODE'], right_on=['INVOICEDATE','XSTORE_CODE', 'BARCODE'],suffixes=('', '_DROP'))

final_data = final_data.drop( ['INVOICEDATE_DROP'], axis = 1)

#current month

temp = temp_clone

temp.rename(columns = {'INVOICETYPE_SALES': 'CURRENT_MONTH_SALES', 'INVOICETYPE_SALES RETURN':'CURRENT_MONTH_SALES_RETURN' }, inplace = True)
temp['INVOICEDATE'] = pd.to_datetime(temp['INVOICEDATE'])

final_data = pd.merge(final_data, temp, how='left',left_on=[pd.to_datetime((final_data['INVOICEDATE'] - pd.DateOffset(months=0)).dt.strftime('%Y-%m')+'-01'),'XSTORE_CODE', 'BARCODE'], right_on=['INVOICEDATE','XSTORE_CODE', 'BARCODE'],suffixes=('', '_DROP'))

final_data = final_data.drop( ['INVOICEDATE_DROP'], axis = 1)

final_data['LAST_MONTH_SALES'] = final_data['LAST_MONTH_SALES'].replace(np.nan, 0)
final_data['CURRENT_MONTH_SALES'] = final_data['CURRENT_MONTH_SALES'].replace(np.nan, 0)
final_data['LAST_MONTH_SALES_RETURN'] = final_data['LAST_MONTH_SALES_RETURN'].replace(np.nan, 0)
final_data['CURRENT_MONTH_SALES_RETURN'] = final_data['CURRENT_MONTH_SALES_RETURN'].replace(np.nan, 0)
final_data['SLEEVE'] = final_data['SLEEVE'].replace(np.nan, 'No Sleeve')
final_data['Color'] = final_data['Color'].replace(np.nan, 'No Color')

# Save the master file
final_data.to_csv('final_data.csv')
final_data['WEEK'] = result['INVOICEDATE'].dt.strftime('%U')
final_data['MONTH'] = result['INVOICEDATE'].dt.strftime('%m')
final_data['YEAR'] = result['INVOICEDATE'].dt.strftime('%Y')
final_data['MONTH NAME'] = result['INVOICEDATE'].dt.strftime('%b')

final_data['Color'] = final_data['Color'].replace('R05', 'No Color')
final_data['Color'] = final_data['Color'].replace('A08', 'No Color')
final_data['Color'] = final_data['Color'].replace('R06', 'No Color')
final_data['Color'] = final_data['Color'].replace('Ecru', 'No Color')
final_data['Color'] = final_data['Color'].replace('NA1', 'No Color')
final_data['Color'] = final_data['Color'].replace('MULTI2', 'No Color')

final_data_clone = final_data.copy(deep = True)
print(final_data.shape)

# for group_name, df_group in final_data[final_data.MRP.isin([0,1])].groupby(['XSTORE_CODE', 'BARCODE', 'MONTH']):
#     for row_index, row in df_group.iterrows():
#         import pdb;pdb.set_trace()


#Train data
final_data = final_data[ ('2019-04-01' <= final_data['INVOICEDATE']) & (final_data['INVOICEDATE'] <= '2020-01-31')]
print("monthly")
print("weekly")
params = {'LAST_MONTH_SALES':'max','LAST_MONTH_SALES_RETURN':'max','CURRENT_MONTH_SALES':'max','CURRENT_MONTH_SALES_RETURN':'max',
          'REGION':'first','STATE':'first','CHANNEL':'first','SUBBRAND':'first','CLASS':'first','SUBCLASS':'first','MRP':'last',
          'SLEEVE':'first','STYLECODE':'first','Season':'first','Color':'first','Size':'first','Season Yr':'first','Tier':'first', 'CITY':'first'}
monthly = final_data.groupby(['MONTH','MONTH NAME','YEAR', 'XSTORE_CODE', 'BARCODE']).agg(params).reset_index()
# monthly = pd.merge( monthly, final_data[['WEEK', 'MONTH', 'YEAR','REGION', 'STATE', 'CHANNEL', 'INVOICENO', 'INVOICEDATE', 'SUBBRAND', 'CLASS', 'SUBCLASS', 'BARCODE', 'TOTAL MRP', 'Season', 'Color', 'Size', 'Season Yr', 'INVOICETYPE_SALES', 'INVOICETYPE_SALES RETURN', 'XSTORE_CODE', 'Tier']],how='inner',left_on=['XSTORE_CODE', 'BARCODE', 'MONTH', 'YEAR'],right_on=['XSTORE_CODE', 'BARCODE', 'MONTH', 'YEAR'])


# weekly.to_csv('weekly_train_data.csv')
print("traindata")
mrp_df = pd.read_excel('Corrected MRP values.xlsx')
mrp_new_df = pd.read_excel('More MRP values.xlsx')
pd.options.display.float_format = '{:.0f}'.format
for row in monthly[monthly.MRP.isin([0,1])][['MRP', 'XSTORE_CODE', 'BARCODE', 'MONTH']].iterrows():
    try:
        mrp_new_val = mrp_df[(mrp_df['BARCODE'] == row[1].BARCODE) & (mrp_df['XSTORE_CODE'] == row[1].XSTORE_CODE) & (mrp_df['MONTH'] == int(row[1].MONTH))]['MRP CORRECTED'].to_list()[0]
    except:
        print((row[1].BARCODE, row[1].XSTORE_CODE, row[1].MONTH))
        mrp_new_val = mrp_new_df[(mrp_new_df['BARCODE'] == row[1].BARCODE) & (mrp_new_df['XSTORE_CODE'] == row[1].XSTORE_CODE) & (mrp_new_df['MONTH'] == int(row[1].MONTH))]['MRP'].to_list()[0]
    mr = monthly[(monthly['BARCODE'] == row[1].BARCODE) & (monthly['XSTORE_CODE'] == row[1].XSTORE_CODE) & (monthly['MONTH'] == row[1].MONTH)]
    monthly.at[mr.index.to_list()[0], 'MRP'] = mrp_new_val



monthly.to_csv('monthly_train_data_latest.csv')
import pdb;pdb.set_trace()

# Test Data
final_data = final_data_clone[ ('2020-02-01' <= final_data_clone['INVOICEDATE']) & (final_data_clone['INVOICEDATE'] <= '2020-02-28')]

# daily  = final_data.groupby(['INVOICEDATE','XSTORE_CODE', 'BARCODE']).agg({'LAST_MONTH_SALES':'sum','LAST_MONTH_SALES_RETURN':'sum','CURRENT_MONTH_SALES':'sum','CURRENT_MONTH_SALES_RETURN':'sum','REGION':'max','STATE':'max','CHANNEL':'max','SUBBRAND':'max','CLASS':'max','SUBCLASS':'max','TOTAL MRP':'max','Season':'max','Color':'max','Size':'max','Season Yr':'max','Tier':'max'}).reset_index()
#
# weekly = final_data.groupby(['WEEK','YEAR','XSTORE_CODE', 'BARCODE']).agg({'LAST_MONTH_SALES':'sum','LAST_MONTH_SALES_RETURN':'sum','CURRENT_MONTH_SALES':'sum','CURRENT_MONTH_SALES_RETURN':'sum','REGION':'max','STATE':'max','CHANNEL':'max','SUBBRAND':'max','CLASS':'max','SUBCLASS':'max','TOTAL MRP':'max','Season':'max','Color':'max','Size':'max','Season Yr':'max','Tier':'max'}).reset_index()

monthly  = final_data.groupby(['MONTH','YEAR','XSTORE_CODE', 'BARCODE']).agg(params).reset_index()

# daily.to_csv('daily_test_data.csv')

monthly.to_csv('monthly_test_data.csv')

# weekly.to_csv('weekly_test_data.csv')
print("testdata")
