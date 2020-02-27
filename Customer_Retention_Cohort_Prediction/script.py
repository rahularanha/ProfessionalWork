#!/usr/bin/env python
# coding: utf-8

# In[1]:

#Import all libraries
import smtplib as smt
import pandas as pd
import numpy as np
import psycopg2 as ps2
import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from datetime import date, datetime, timedelta
import os
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
import _pickle as cPickle
##### EMAILING THE TABLES #####
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from sklearn.metrics import r2_score
import io
import boto3

# In[2]:


#########Formatting for the mail##################
def prephtml(df):
    dfhtml = df.to_html(index=False)
    return dfhtml

def format_html(df, title=''):
    '''
    Write an entire dataframe to an HTML file with nice formatting.
    '''

    result = '''
<html>
<head>
<style>
    h2 {
        text-align: left;
        font-family: Helvetica, Arial, sans-serif;
    }
    table {
        margin-left: auto;
        margin-right: auto;
    }
    table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
    }
    th, td {
        padding: 5px;
        align: middle;
        text-align: left;
        font-family: Helvetica, Arial, sans-serif;
        font-size: 90%;
    }
    table tbody tr:hover {
        background-color: #dddddd;
    }
    .wide {
        width: 90%;
    }
</style>
</head>
<body>
    '''
    result += '<h3> %s </h3>\n' % title
    result += df.to_html(index = False, na_rep=0,float_format='{0:.0f}'.format, classes='wide', escape=False)
    result += '''
</body>
</html>
'''
    return result

#connection variables
pe_dl_dbname = os.environ.get("SKULL_DBNAME")
pe_dl_host = os.environ.get("SKULL_HOST")
pe_dl_port = os.environ.get("SKULL_PORT")
pe_dl_user = os.environ.get("SKULL_USER")
pe_dl_password = os.environ.get("SKULL_PASSWORD")
pe_aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
pe_aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

#cnxn = create_engine('postgresql://kartiksinghvi:pbHKq9xTQm@dp-prod-redshift.cvsmnufhrlzh.ap-south-1.redshift.amazonaws.com:5439/skull')
cnxn = create_engine('postgresql://'+ pe_dl_user +':'+ pe_dl_password+'@'+pe_dl_host+':'+'5439'+'/' +pe_dl_dbname)
train_data_all = pd.DataFrame(columns=['flag', 'Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution', 'acquired_users_count', 'retention_percent_7', 'retention_percent_14' ,'retention_percent_21', 'retention_percent_28', 'retention_percent'])
final_data = pd.DataFrame(columns=['flag', 'Acquisition_month', 'Latest_Order_Month', 'chronic_flag_old', 'base_discount_order', 'supplier_city_name', 'is_courier', 'install_source_attribution', 'acquired_users_count', 'retention_percent'])
prediction_month = str(date.today().replace(day=1))
list_low_R2_values = []
rf_parameter_dict = { 9: {'min_samples_leaf': 2, 'min_samples_split': 6, 'n_estimators' : 150},
                     10: {'min_samples_leaf': 2, 'min_samples_split': 4, 'n_estimators' : 150},
                     11: {'min_samples_leaf': 2, 'min_samples_split': 2, 'n_estimators' : 100},
                     12: {'min_samples_leaf': 2, 'min_samples_split': 4, 'n_estimators' : 150},
                     13: {'min_samples_leaf': 2, 'min_samples_split': 4, 'n_estimators' : 150}
                     }
# In[3]:


#print(df_all_users_count.shape, df_retained_users.shape, raw_df.shape, pivot_raw_data.shape, pivot_raw_data_v2.shape, pivot_raw_data_v3.shape, train_data_all.shape, outliers.shape, train_data_all_with_outliers.shape, train_data_all_outliers_removed.shape, train_data.shape)
def prepare_monthly_data(date):
    delivery_temp = open('train_data_query.sql','r')
    str1 = delivery_temp.read()
    str1 = str1.replace("month_date", date)
    cnxn.execute(str1)
    
    all_users_count = cnxn.execute("select * from t1")
    df_all_users_count = pd.DataFrame(all_users_count)
    df_all_users_count.columns = ['flag', 'Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution', 'acquired_users_count']

    retained_users = cnxn.execute("select * from t2")
    df_retained_users = pd.DataFrame(retained_users)
    df_retained_users.columns = ['flag', 'Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution', 'day_of_next_order','count', 'cumulative_sum']

    #Data Manipulations
    #1. sorting the dataset
    df_retained_users.sort_values(['flag', 'Acquisition_month', 'Latest_Order_Month', 'chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution', 'day_of_next_order'], ascending=True, inplace=True)
    raw_df = df_retained_users.drop('count', axis = 1)

    #2. pivoting
    pivot = pd.pivot_table(raw_df, values='cumulative_sum', index=['flag', 'Acquisition_month', 'Latest_Order_Month', 'chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution'], columns=['day_of_next_order'], aggfunc=np.sum)
    pivot_raw_data = pivot.reset_index()

    #3. finding the final count of retention for month and filtering the required dataset
    pivot_raw_data['retained_customer_count'] = pivot_raw_data.iloc[:, 8:39].max(axis = 1)
    pivot_raw_data.iloc[:,8].fillna(0, inplace=True)
    pivot_raw_data = pivot_raw_data.fillna(method='ffill', axis=1)
    pivot_raw_data_v2 = pivot_raw_data[['flag', 'Acquisition_month', 'Latest_Order_Month', 'chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution',  7, 14, 21, 28, 'retained_customer_count']]
    pivot_raw_data_v3 = pd.merge(pivot_raw_data_v2, df_all_users_count, how = 'inner', left_on = ['flag', 'Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution'], right_on = ['flag', 'Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution'])

    #3. joining with the other dataset to get the acquired user count and calcuting retention figures at milestone days
    pivot_raw_data_v3['retention_percent'] = pivot_raw_data_v3.retained_customer_count*100/pivot_raw_data_v3.acquired_users_count
    pivot_raw_data_v3['retention_percent_7'] = pivot_raw_data_v3[7]*100/pivot_raw_data_v3.acquired_users_count
    pivot_raw_data_v3['retention_percent_14'] = pivot_raw_data_v3[14]*100/pivot_raw_data_v3.acquired_users_count
    pivot_raw_data_v3['retention_percent_21'] = pivot_raw_data_v3[21]*100/pivot_raw_data_v3.acquired_users_count
    pivot_raw_data_v3['retention_percent_28'] = pivot_raw_data_v3[28]*100/pivot_raw_data_v3.acquired_users_count
    train_data_all = pivot_raw_data_v3[['flag', 'Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution', 'acquired_users_count', 'retention_percent_7', 'retention_percent_14' ,'retention_percent_21', 'retention_percent_28', 'retention_percent']]
    train_data_all.fillna(0, inplace=True)
    cnxn.execute("drop table t1")
    cnxn.execute("drop table t2")
    return train_data_all


# In[4]:


#training_data_months = ["'2019-09-01'","'2019-10-01'", "'2019-11-01'", "'2019-12-01'" ]
number_of_training_months=4
for i in range(0,number_of_training_months):
    train_data_all = train_data_all.append(prepare_monthly_data("'" +str((date.today().replace(day=1) + relativedelta(months=-i-1)).strftime("%Y-%m-%d"))+"'"))
    print(i)


# In[6]:


def model_creation(train_data_all, j):
#######################----------------------Outlier Removal----------------------#######################
    parameter_dict = {9: 'retention_percent_7', 10: 'retention_percent_14', 11: 'retention_percent_21', 12: 'retention_percent_28', 13: 'retention_percent'}
    install_sources = list(train_data_all.install_source_attribution.unique())
    chronic_flag = list(train_data_all.chronic_flag_old.unique())
    base_discount = list(train_data_all.base_discount_order.unique())
    
    empty_df = pd.DataFrame(columns=train_data_all.columns)

    for i in install_sources:    
        Q1 = train_data_all[train_data_all['install_source_attribution']==i].iloc[:,j].quantile(0.25)
        Q3 = train_data_all[train_data_all['install_source_attribution']==i].iloc[:,j].quantile(0.75)
        IQR = Q3 - Q1
        filtered_data = train_data_all[train_data_all.install_source_attribution==i]
        tempdf = filtered_data[((filtered_data.iloc[:,j] < (Q1 - 1.5 * IQR)) | (filtered_data.iloc[:,j] > (Q3 + 1.5 * IQR)))]
        empty_df = empty_df.append(tempdf)
        #print(IQR, Q1, Q3)

    for i in chronic_flag:    
        Q1 = train_data_all[train_data_all['chronic_flag_old']==i].iloc[:,j].quantile(0.25)
        Q3 = train_data_all[train_data_all['chronic_flag_old']==i].iloc[:,j].quantile(0.75)
        IQR = Q3 - Q1
        filtered_data = train_data_all[train_data_all.chronic_flag_old==i]
        tempdf = filtered_data[((filtered_data.iloc[:,j] < (Q1 - 1.5 * IQR)) |(filtered_data.iloc[:,j] > (Q3 + 1.5 * IQR)))]
        empty_df = empty_df.append(tempdf)
        #print(IQR, Q1, Q3)

    for i in base_discount:    
        Q1 = train_data_all[train_data_all['base_discount_order']==i].iloc[:,j].quantile(0.25)
        Q3 = train_data_all[train_data_all['base_discount_order']==i].iloc[:,j].quantile(0.75)
        IQR = Q3 - Q1
        filtered_data = train_data_all[train_data_all.base_discount_order==i]
        tempdf = filtered_data[((filtered_data.iloc[:,j] < (Q1 - 1.5 * IQR)) |(filtered_data.iloc[:,j] > (Q3 + 1.5 * IQR)))]
        empty_df = empty_df.append(tempdf)
        #print(IQR, Q1, Q3)

    outliers = empty_df.drop_duplicates()

    train_data_all_with_outliers = pd.merge(train_data_all, outliers[['Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution', parameter_dict[j]]], how = 'left', left_on = ['Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution'], right_on = ['Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution'])
    train_data_all_outliers_removed = train_data_all_with_outliers[train_data_all_with_outliers.iloc[:,-1].notnull()==False]

    #Select which retention_percent you are predicting - in this case we take overall retention_percent
    train_data = train_data_all_outliers_removed[['flag', 'Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution', 'acquired_users_count', parameter_dict[j]+'_x']]
    print(train_data.columns)
    train_data.columns = ['flag', 'Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution', 'acquired_users_count', 'retention_percent']

    #######################----------------------Categorical conversions----------------------#######################
    final_data = train_data.copy()
    final_data['supplier_city_name'] = final_data['supplier_city_name'].astype('category')
    final_data['install_source_attribution'] = final_data['install_source_attribution'].astype('category')
    final_data['Acquisition_month'] = final_data['Acquisition_month'].astype('category')
    final_data['Latest_Order_Month'] = final_data['Latest_Order_Month'].astype('category')

    dummies = pd.get_dummies(final_data,prefix=['AM', 'LOM', 'SC', 'IS'], columns=['Acquisition_month', 'Latest_Order_Month', 'supplier_city_name', 'install_source_attribution'], prefix_sep='-')
    dummies = dummies.reset_index()

    df = dummies[['chronic_flag_old', 'base_discount_order', 'is_courier',
           'AM-M1', 'AM-M2', 'AM-M3', 'AM-M4', 'AM-M5', 'AM-M6', 'AM-M6+', 'LOM-M1', 'LOM-M2', 'LOM-M3',
           'LOM-M4', 'LOM-M5', 'LOM-M6', 'LOM-M6+', 'SC-Ahmedabad', 'SC-Bengaluru', 'SC-Chennai', 'SC-Gurgaon', 'SC-Hyderabad', 'SC-Jaipur', 'SC-Kolkata',
           'SC-Mumbai', 'IS-Affiliates', 'IS-Facebook', 'IS-Google', 'IS-Organic', 'IS-Others', 'acquired_users_count', 'retention_percent']]

    #Filter cohorts for only more than 75 acquired users
    df = df[df.acquired_users_count>=75]

    X = df.loc[:,df.columns!='retention_percent']
    y = df[['retention_percent']]
    
    #######################----------------------Random Forest Regression - Save Models----------------------#######################

    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
    import _pickle as cPickle

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    rf = RandomForestRegressor(n_estimators = rf_parameter_dict[j]['n_estimators'], min_samples_split=rf_parameter_dict[j]['min_samples_split'], min_samples_leaf = 2, max_features = 'sqrt')
    rf.fit(X_train, y_train.values.ravel())
    predictions = rf.predict(X_test)
    #errors = abs(predictions - np.array(pd.Series(y_test.retention_percent.values)))
    #ssr = np.sum((errors)**2)
    #sst = np.sum((np.array(pd.Series(y_test.retention_percent.values)) - np.mean(np.array(pd.Series(y_test.retention_percent.values))))**2)
    #r2_score = 1 - (ssr/sst)
    r2_score_f = r2_score(y_test, predictions)
    adjusted_r_squared = 1 - (1-r2_score_f)*(len(y_test)-1)/(len(y_test)-X_test.shape[1]-1)
    
    if adjusted_r_squared>=0.85:
        rf.fit(X,y.values.ravel())
        directory_pe = 'Cohort_Prediction_Models/' + parameter_dict[j]+'_'+date.today().replace(day=1).strftime("%Y-%m-%d") + '.pkl'
        s3_resource = boto3.resource('s3',aws_access_key_id = pe_aws_access_key_id,aws_secret_access_key = pe_aws_secret_access_key)
        with open(parameter_dict[j]+'_'+date.today().replace(day=1).strftime("%Y-%m-%d") + '.pkl', 'wb') as f:
            cPickle.dump(rf, f)
        s3_resource.Bucket('pe-prod-redshift').upload_file(parameter_dict[j]+'_'+date.today().replace(day=1).strftime("%Y-%m-%d") + '.pkl', 'Cohort_Prediction_Models/' + parameter_dict[j]+'_'+date.today().replace(day=1).strftime("%Y-%m-%d") + '.pkl')
    else:
        list_low_R2_values.append(parameter_dict[j])
        # In[7]:

var_pred = [9, 10, 11, 12, 13]
for j in var_pred:
    model_creation(train_data_all, j)

#######################----------------------Send Email for low ----------------------#######################
if len(list_low_R2_values)>0:    
    me = "sender_email_id"
    you = "receiver_email_id"
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Retention Cohort Prediction low R2 models - ' + date.today().replace(day=1).strftime("%Y-%m-%d")
    msg['From'] = me
    #msg["Cc"] = cc
    msg['To'] = you
    htmlbody = '<body><br>Hello Team,<br><br>R2 values for following models have fallen below threshold(0.85) for current month. Please check.<br><br>'
    htmlbody = htmlbody + '<br><br> ' + list_low_R2_values + '<br>'
    emailbody = (htmlbody)
    part3 = MIMEText(emailbody, 'html')
    msg.attach(part3)
    s = smt.SMTP_SSL('smtp.gmail.com', 465)
    s.login(me, "26B5922F2BC94SZ1ME4B158474BF9")
    s.sendmail(me,you.split(','),msg.as_string())
    s.quit()
else:
    #######################----------------------Predict using Models ----------------------#######################
    delivery_temp = open('query_predict_next_month.sql','r')
    str1 = delivery_temp.read()
    str1 = str1.replace("month_date", "'" +str((date.today().replace(day=1).strftime("%Y-%m-%d"))+"'"))
    cnxn.execute(str1)

    all_users_count = cnxn.execute("select * from t1")
    df_all_users_count = pd.DataFrame(all_users_count)
    df_all_users_count.columns = ['flag', 'Acquisition_month', 'Latest_Order_Month','chronic_flag_old','base_discount_order','supplier_city_name','is_courier','install_source_attribution', 'acquired_users_count']

    next_month_data=df_all_users_count

    next_month_data['supplier_city_name'] = next_month_data['supplier_city_name'].astype('category')
    next_month_data['install_source_attribution'] = next_month_data['install_source_attribution'].astype('category')
    next_month_data['Acquisition_month'] = next_month_data['Acquisition_month'].astype('category')
    next_month_data['Latest_Order_Month'] = next_month_data['Latest_Order_Month'].astype('category')

    dummies = pd.get_dummies(next_month_data,prefix=['AM', 'LOM', 'SC', 'IS'], columns=['Acquisition_month', 'Latest_Order_Month', 'supplier_city_name', 'install_source_attribution'], prefix_sep='-')
    dummies = dummies.reset_index()

    df = dummies[['chronic_flag_old', 'base_discount_order', 'is_courier',
           'AM-M1', 'AM-M2', 'AM-M3', 'AM-M4', 'AM-M5', 'AM-M6', 'AM-M6+', 'LOM-M1', 'LOM-M2', 'LOM-M3',
           'LOM-M4', 'LOM-M5', 'LOM-M6', 'LOM-M6+', 'SC-Ahmedabad', 'SC-Bengaluru', 'SC-Chennai', 'SC-Gurgaon', 'SC-Hyderabad', 'SC-Jaipur', 'SC-Kolkata',
           'SC-Mumbai', 'IS-Affiliates', 'IS-Facebook', 'IS-Google', 'IS-Organic', 'IS-Others', 'acquired_users_count']]

    #Filter cohorts for only more than 75 acquired users
    df2 = df[df.acquired_users_count>=75]

    df_for_result = df2.copy()
    for k in var_pred:
        parameter_dict = {9: 'retention_percent_7', 10: 'retention_percent_14', 11: 'retention_percent_21', 12: 'retention_percent_28', 13: 'retention_percent'}
        with open(parameter_dict[k]+'_'+date.today().replace(day=1).strftime("%Y-%m-%d") + '.pkl', 'rb') as file:
            pickle_model = cPickle.load(file)

        predictions_nm = pickle_model.predict(df2)
        df_for_result[parameter_dict[k]+'.pkl'] = predictions_nm
    df_for_result['prediction_month'] = str(date.today().replace(day=1))
    df_for_result = df_for_result.reset_index()
    
    AM_x = df_for_result[['AM-M1', 'AM-M2', 'AM-M3', 'AM-M4', 'AM-M5', 'AM-M6', 'AM-M6+']].stack()
    AM = pd.Series(pd.Categorical(AM_x[AM_x!=0].index.get_level_values(1)))

    LOM_x = df_for_result[['LOM-M1', 'LOM-M2', 'LOM-M3','LOM-M4', 'LOM-M5', 'LOM-M6', 'LOM-M6+']].stack()
    LOM = pd.Series(pd.Categorical(LOM_x[LOM_x!=0].index.get_level_values(1)))

    SX_x = df_for_result[['SC-Ahmedabad', 'SC-Bengaluru', 'SC-Chennai', 'SC-Gurgaon', 'SC-Hyderabad', 'SC-Jaipur', 'SC-Kolkata','SC-Mumbai']].stack()
    SC = pd.Series(pd.Categorical(SX_x[SX_x!=0].index.get_level_values(1)))

    IS_x = df_for_result[['IS-Affiliates', 'IS-Facebook', 'IS-Google', 'IS-Organic', 'IS-Others']].stack()
    IS = pd.Series(pd.Categorical(IS_x[IS_x!=0].index.get_level_values(1)))

    df_for_result['Acquisition_month'] = AM
    df_for_result['Latest_order_month'] = LOM
    df_for_result['Supplier_city'] = SC
    df_for_result['Installation_source'] = IS

    df_for_result['Acquisition_month'] = df_for_result['Acquisition_month'].str.replace('AM-', '')
    df_for_result['Latest_order_month'] = df_for_result['Latest_order_month'].str.replace('LOM-', '')
    df_for_result['Supplier_city'] = df_for_result['Supplier_city'].str.replace('SC-', '')
    df_for_result['Installation_source'] = df_for_result['Installation_source'].str.replace('IS-', '')
    df_for_result['created_at'] = str(datetime.today())
    
    final_data = df_for_result[['created_at', 'prediction_month', 'chronic_flag_old', 'base_discount_order', 'is_courier', 'Acquisition_month', 'Latest_order_month', 'Supplier_city','Installation_source', 'acquired_users_count', 'retention_percent_7.pkl', 'retention_percent_14.pkl', 'retention_percent_21.pkl','retention_percent_28.pkl', 'retention_percent.pkl']]
    final_data.to_sql('retention_cohort_prediction', schema='data_logs', con = cnxn, if_exists='append',method='multi', index=False, chunksize= 50000)

mydir = os.getcwd()
xllist = [ f for f in os.listdir(mydir) if f.endswith(".pkl") ]
for f in xllist:
    os.remove(os.path.join(mydir, f))
