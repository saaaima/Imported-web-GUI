import os
import pandas as pd
import numpy as np
import sqlite3
import time
import datetime
import operator
import collections
import json

from Importer_Step1 import prepare_data
from update_missing import update_airings_func
from importer_test_new import matching_func_new_sql_test
from display_ambigu import display_ambigu_func
from test_commercials import matching_commercials_func
from change_time import time_change_func
from anothercase import anothercase_func
from drop_dup_sql import drop_dup_sql_func
from match_airings import match_airings_func
from missing_airings_sql_new import missing_airings_func_new
from missing_airing_sql import missing_airings_func
from fuzzywuzzy_sql_GUI import fuzzywuzzy_func
from flask import Flask, render_template,request,session,redirect,url_for
from werkzeug import secure_filename

import sys  
reload(sys)  
sys.setdefaultencoding('utf8')

app = Flask(__name__)
app.secret_key='hello'

UPLOAD_FOLDER = '/Users/masai/Documents/wywy/uploads/'
SAVE_FOLDER= '/Users/masai/Documents/wywy/wywy/static/'
time_format='%Y-%m-%d %H:%M:%S'

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/input_data')
def input_data():
   return render_template('read_parameters.html')



@app.route('/apply_api', methods = ['POST'])
def read_api():
    infor=request.form
    
    session['customer_id']=infor['customer_id']
    date_start=infor['date_start']
    date_end=infor['date_end']
    timezone=infor['Timezone']
    logfile=request.files['postlog']
    logfile.save(os.path.join(app.config['UPLOAD_FOLDER'],'POSTLOG.csv'))
    read_path=UPLOAD_FOLDER+'POSTLOG.csv'
    prepare_data(session['customer_id'],date_start,date_end,read_path,timezone)
  
    return redirect(url_for('reminder_spots'))

@app.route('/reminder_spots')
def reminder_spots():
    df1=pd.read_csv(UPLOAD_FOLDER+'POSTLOG.csv')
   
    
    
    df1=df1.sort_values(['timestamp utc','Sender'])
    
    channel_lst=df1['Sender'].unique().tolist()
    charts=[]
    for channel in channel_lst:
        data=df1[df1['Sender']==channel]
        timeaxis=data['airing time utc'].apply(lambda x: datetime.datetime.strptime(x,'%m/%d/%Y %H:%M:%S')).tolist()
        xx=data[:-1]['timestamp utc'].apply(lambda x: int(x)).tolist()
        yy=data[1:]['timestamp utc'].apply(lambda x: int(x)).tolist()
        
        z=abs(np.array(xx)-np.array(yy))
        
        dic={timeaxis[i]:z[i] for i in range(len(data)-1)}
        drawdict={k:dic[k] for k in dic.keys() if dic[k]<120}
        if drawdict!={}:
           
            data=[
                    go.Scatter(
                            x=drawdict.keys(),
                            y=drawdict.values(),
                            mode = 'markers'
                            )
                            ]
            
            layout = go.Layout(
            title=channel,
            yaxis=dict(title='possible reminder spots distribution'),
            xaxis=dict(title='time')
            )
            
            fig = go.Figure(data=data,layout=layout)
            my_div=plotly.offline.plot(fig,output_type='div')
            charts.append(Markup(my_div))
                

    return render_template('charts.html',My_div=charts)
   


@app.route('/drop_duplications',methods=['POST'])
def drop_dup():
    infor=request.form
    time_duration1=infor['timestamp1']
    time_duration2=infor['timestamp2']
    clean_results=drop_dup_sql_func(time_duration1,time_duration2)
    df1=clean_results[2]
    df2=clean_results[3]
    common=[val for val in clean_results[0] if val in clean_results[1]]
    for i in common:
        ID=(df2.loc[df2['channel_name']==i,'channel_id'].unique().tolist())[0]
        df1.loc[df1['Sender']==i,'channel_id']=ID
    
    
    session['unique_post']=list(set(clean_results[0])-set(common))
    unique_tva=list(set(clean_results[1])-set(common))
    
    ###################################################################################
    
    conn=sqlite3.connect('channelNames.db')
    c=conn.cursor()
    conn.text_factory = str
    
    counts=c.execute('SELECT COUNT(*) FROM CHANNEL_MAPPING')
    if counts!=0:
        c.execute('SELECT channel_postlog FROM CHANNEL_MAPPING WHERE customerID=?',(session['customer_id'],))
        column_1=[]
        for row in c.fetchall():
            column_1.append(row[0])
        session['history']=[i for i in session['unique_post'] if i in column_1]
        
        session['compl_history']=list(set(session['unique_post'])-set(session['history']))
        
        
        if session['history']!=[] and session['compl_history']!=[]:
            for i in session['history']: 
                c.execute('SELECT channelID,channel_tva FROM CHANNEL_MAPPING WHERE customerID=? AND channel_postlog=?',(session['customer_id'],i,))
                mapTva=c.fetchone()
                
                df1.loc[df1['Sender']==i,'channel_id']=mapTva[0]
                df1.loc[df1['Sender']==i,'Sender']=mapTva[1]
                conn.commit()
            
            df1.to_csv(UPLOAD_FOLDER+'POSTLOG_trimed.csv',index=False)
                
            return render_template('test_selected.html',senders_lst=session['compl_history'],\
                                   channels_lst=unique_tva)
            
        elif session['history']!=[] and session['compl_history']==[]:
            for i in session['history']: 
                c.execute('SELECT channelID,channel_tva FROM CHANNEL_MAPPING WHERE customerID=? AND channel_postlog=?',(session['customer_id'],i,))
                mapTva=c.fetchone()
                
                df1.loc[df1['Sender']==i,'channel_id']=mapTva[0]
                df1.loc[df1['Sender']==i,'Sender']=mapTva[1]
                conn.commit()
            
            df1.to_csv(UPLOAD_FOLDER+'POSTLOG_trimed.csv',index=False)
                
            return redirect(url_for('matchmissing_airings'))
        else:
            return render_template('test_selected.html',senders_lst=session['unique_post'],\
                                   channels_lst=unique_tva)
    ########################################################################################        
    else:
        return render_template('test_selected.html',senders_lst=session['unique_post'],\
                                   channels_lst=unique_tva)
       
    
    
   
   
####################################################################################################################  
match_results=0 
@app.route('/change_name', methods = ['POST'])      
def change_name():  
    df2=pd.read_csv(UPLOAD_FOLDER+'TVA_trimed.csv')
    df1=pd.read_csv(UPLOAD_FOLDER+'POSTLOG_trimed.csv')
    
    conn=sqlite3.connect('channelNames.db')
    c=conn.cursor()
    conn.text_factory = str
    change_name=request.form.getlist('senders')
    changed_NAME=[i.encode('utf-8') for i in change_name]
    
    if any(changed_NAME) is True:

        if session['history']==[]:
            for i in range(len(changed_NAME)):
                if changed_NAME[i]!='':
                    t=changed_NAME[i]
                    ID=(df2.loc[df2['channel_name']==t,'channel_id'].unique().tolist())[0]
                    
                    df1.loc[df1['Sender']==session['unique_post'][i],'channel_id']=ID
                    s=session['unique_post'][i]
                    t_end=int(time.time())
                    updated_time=time.strftime(time_format,time.gmtime(t_end))
                    
                    c.execute('INSERT INTO CHANNEL_MAPPING VALUES (?,?,?,?,?)',(session['customer_id'],s,t,ID,updated_time))
            conn.commit()
            global match_results
            match_results=matching_func_new_sql_test(session['unique_post'],changed_NAME)
            return redirect(url_for('matchmissing_airings'))
        #####################################################################################
        elif session['history']!=[] and session['compl_history']!=[]:
            for i in range(len(changed_NAME)):
                if changed_NAME[i]!='':
                    t=changed_NAME[i]
                    ID=(df2.loc[df2['channel_name']==t,'channel_id'].unique().tolist())[0]
                    
                    df1.loc[df1['Sender']==session['compl_history'][i],'channel_id']=ID
                    
                    s=session['compl_history'][i]
                    t_end=int(time.time())
                    updated_time=time.strftime(time_format,time.gmtime(t_end))
                    c.execute('INSERT INTO CHANNEL_MAPPING VALUES (?,?,?,?,?)',(session['customer_id'],s,t,ID,updated_time))
            conn.commit()
            
            global match_results
            match_results=matching_func_new_sql_test(session['compl_history'],changed_NAME)
            return redirect(url_for('matchmissing_airings'))
    
    else:
        
        session['second_round']=fuzzywuzzy_func(session['customer_id'],session['compl_history'])
        second_df=pd.DataFrame(session['second_round'].items(),columns=['POSTLOG','API'])
        
        

        second_df['channel ID']=[i[1] for i in session['second_round'].values()]
        second_df['API']=[i[0] for i in session['second_round'].values()]
        
        for keys in session['second_round'].keys():
            t=session['second_round'][keys][0]
            ID=session['second_round'][keys][1]
        
            df1.loc[df1['Sender']==keys,'channel_id']=ID
        
            s=keys
            t_end=int(time.time())
            updated_time=time.strftime(time_format,time.gmtime(t_end))
            c.execute('INSERT INTO CHANNEL_MAPPING VALUES (?,?,?,?,?)',(session['customer_id'],s,t,ID,updated_time))
        conn.commit()
        conn.close()
        return render_template('second_change.html',data1=second_df)
    
###############################################################################################################################


results=0  
verified_airings_df1=0
verified_airings_df2=0
dup_dict={}

@app.route('/matchmissing_airings', methods = ['GET','POST'])  
def matchmissing_airings():
    
    global match_results
    df1_match=match_results[0]
    df2_match=match_results[1]
    
    #Matching unpaired airings
    ###############################################################################
   
    
    
    rough_MATCH=match_airings_func(df1_match,df2_match)
            
    
    dup_TVA=[item for item, count in collections.Counter(rough_MATCH.values()).items() if count > 1]
    
    global dup_dict
    dup_dict={k:rough_MATCH[k] for k in rough_MATCH.keys() if rough_MATCH[k] in dup_TVA}
    
    verified_match={k:rough_MATCH[k] for k in rough_MATCH.keys() if k not in dup_dict.keys()}
    
    
    global verified_airings_df1
    verified_airings_df1=df1_match[df1_match['label'].isin (verified_match.keys())]

  
    global verified_airings_df2  
    verified_airings_df2=df2_match[df2_match['label'].isin (verified_match.values())]
   
    
    free_airings=df2_match[~df2_match['label'].isin (set(rough_MATCH.values()))]
    free_airings.to_csv(SAVE_FOLDER+'Free airings.csv',index=False)

    length_missing=len(df1_match)-len(df2_match)+len(free_airings)
    length_ambi=len(dup_TVA)
    
    return render_template('showRoughMissing.html',NoOfFree=len(free_airings),NoOfMissing=length_missing,\
                          NoOfAmbi=length_ambi)
    
    

@app.route('/show_commercials', methods = ['POST','GET'])      
def show_commercials(): 
    df1=pd.read_csv(UPLOAD_FOLDER+'POSTLOG_namechanged.csv')
   

    if df1['commercial name'].all() is not np.nan:
      
        global verified_airings_df1
        post_commercial=verified_airings_df1['commercial name'].unique().tolist()
       
    ###############################################################################       
        post_commercial_counts=dict(verified_airings_df1['commercial name'].value_counts())
        sorted_df1_dict = sorted(post_commercial_counts.items(), key=operator.itemgetter(1))
        post_commercial_counts=dict(sorted_df1_dict)
        session['post_commercial_lst']=post_commercial_counts.keys()
        
    ###############################################################################    
        global verified_airings_df2 
        tva_commercial_counts=dict(verified_airings_df2['commercial_name'].value_counts())
        sorted_df2_dict = sorted(tva_commercial_counts.items(), key=operator.itemgetter(1))
        tva_commercial_counts=dict(sorted_df2_dict)
    ############################################################################### 
    
        post_name=verified_airings_df1['Sender'].unique().tolist()
        dic_1={k: verified_airings_df1.loc[verified_airings_df1['Sender']==k,'commercial name'].value_counts() for k in post_name}
        df1_new=pd.DataFrame(dic_1)
        df1_new=df1_new.sort_values(post_name,ascending=True)
    ############################################################################### 
    
        tva_name=verified_airings_df2['channel_name'].unique().tolist()
        dic_2={k: verified_airings_df2.loc[verified_airings_df2['channel_name']==k,'commercial_name'].value_counts() for k in tva_name}
        df2_new=pd.DataFrame(dic_2)
        df2_new=df2_new.sort_values(tva_name,ascending=True)       
    ###############################################################################
        
        conn=sqlite3.connect('commercialNames.db')
        conn.text_factory = str
        c=conn.cursor()
    ###############################################################################   
        counts=c.execute('SELECT COUNT(*) FROM COMMERCIAL_MAPPING')
        if counts!=0:
            c.execute('SELECT commercial_postlog FROM COMMERCIAL_MAPPING WHERE customerID=?',(session['customer_id'],))
            column_1=[]
            for row in c.fetchall():
                column_1.append(row[0])
            session['record']=[i for i in post_commercial if i in column_1]
            session['compl_record']=list(set(post_commercial)-set(session['record']))
       
            if session['record']!=[] and session['compl_record']!=[]:
                rule_lst=[]
                for i in session['record']: 
                    c.execute('SELECT commercialID,commercial_tva FROM COMMERCIAL_MAPPING WHERE customerID=? AND commercial_postlog=?',(session['customer_id'],i,))
                    mapTva=c.fetchone()
                
                    df1.loc[df1['commercial name']==i,'commercial_id']=mapTva[0]
                    df1.loc[df1['commercial name']==i,'commercial name']=mapTva[1]
                    rule_lst.append(mapTva[1])
                    conn.commit()
                    
    #################################################################################################                 
                df1.to_csv(UPLOAD_FOLDER+'POSTLOG_commercials.csv',index=False)
              
                df1_slice=verified_airings_df1[verified_airings_df1['commercial name'].isin (session['compl_record'])]
                df1_slice_dict=dict(df1_slice['commercial name'].value_counts())
                sorted_df1_slice_dict = sorted(df1_slice_dict.items(), key=operator.itemgetter(1))
                df1_slice_dict=dict(sorted_df1_slice_dict)
               
                session['df1_slice_dict_lst']=df1_slice_dict.keys()
                
                
                
                name1_slice=df1_slice['Sender'].unique().tolist()
                dic_1_slice={k: df1_slice.loc[df1_slice['Sender']==k,'commercial name'].value_counts() for k in name1_slice}
                df1_new_slice=pd.DataFrame(dic_1_slice)
                df1_new_slice=df1_new_slice.sort_values(name1_slice,ascending=True)
                
                
                
                df2_slice=verified_airings_df2[~verified_airings_df2['commercial_name'].isin (rule_lst)]
                df2_slice_dict=dict(df2_slice['commercial_name'].value_counts())
                sorted_df2_slice_dict = sorted(df2_slice_dict.items(), key=operator.itemgetter(1))
                df2_slice_dict=dict(sorted_df2_slice_dict)
                
                
                
                
                name2_slice=df2_slice['channel_name'].unique().tolist()
                dic_2_slice={k: df2_slice.loc[df2_slice['channel_name']==k,'commercial_name'].value_counts() for k in name2_slice}
                df2_new_slice=pd.DataFrame(dic_2_slice)
                df2_new_slice=df2_new_slice.sort_values(name2_slice,ascending=True)
                    
                    
                return render_template('change_commercials.html',data1=df1_new_slice,data2=df2_new_slice,\
                            df1_DIC=df1_slice_dict,df2_DIC=df2_slice_dict)
        #######################################################################################################################
            #In this case, all the commercial name can be changed from the back-end database, so no more needs for commercial changing.
            #Therefore, we can check ambiguous airings directly.              
            elif session['record']!=[] and session['compl_record']==[]:
                for i in session['record']: 
                    c.execute('SELECT commercialID,commercial_tva FROM COMMERCIAL_MAPPING WHERE customerID=? AND commercial_postlog=?',(session['customer_id'],i,))
                    mapTva=c.fetchone()
                
                    df1.loc[df1['commercial name']==i,'commercial_id']=mapTva[0]
                    df1.loc[df1['commercial name']==i,'commercial name']=mapTva[1]
                conn.commit()
                    
                df1.to_csv(SAVE_FOLDER+'POSTLOG_commercials.csv',index=False)
                global dup_dict
                 
                display=display_ambigu_func(dup_dict)
                checked_match_lst=[display.iloc[i]['label'] for i in range(len(display)) if display.iloc[i]['airing time utc-TVA'] is not np.nan]
                
                
                missing=df1[~df1['label'].isin(verified_airings_df1['label'].tolist())]
                total_missing=missing[~missing['label'].isin(checked_match_lst)]
                
                total_missing.to_csv(SAVE_FOLDER+'Total missing airings.csv',index=False)
                return render_template('ambiguous.html',data1=display)
                    
            
            
         #######################################################################################################################            
            elif session['record']==[]:
                
                return render_template('change_commercials.html',data1=df1_new, data2=df2_new,\
                               df1_DIC=post_commercial_counts,df2_DIC=tva_commercial_counts)
            
        else:
            return render_template('change_commercials.html', data1=df1_new, data2=df2_new,\
                                   df1_DIC=post_commercial_counts,df2_DIC=tva_commercial_counts)
    else:
        global dup_dict
         
        display=display_ambigu_func(dup_dict)
        #checked_match_lst=[display.iloc[i]['label'] for i in range(len(display)) if display.iloc[i]['airing time utc-TVA'] is not np.nan]
        
        
        missing=df1[~df1['label'].isin(verified_airings_df1['label'].tolist())]
        #total_missing=missing[~missing['label'].isin(checked_match_lst)]
        
        missing.to_csv(SAVE_FOLDER+'Part missing airings.csv',index=False)
        
        return render_template('noCommercials.html',data1=display)
 
 ##################################################################################
@app.route('/change_commercials',methods=['POST'])
def change_commercials():
    df2=pd.read_csv(UPLOAD_FOLDER+'TVA_trimed.csv')
    conn=sqlite3.connect('commercialNames.db')
    c=conn.cursor()
    conn.text_factory = str
    
    change_commercials=request.form.getlist('commercials')
    
    choice=request.form.get('status')
    
    change_Com=[i.decode('utf-8') for i in change_commercials]
    if session['record']==[]:
        df1=pd.read_csv(UPLOAD_FOLDER+'POSTLOG_namechanged.csv')
        
        for i in range(len(change_Com)):
            if change_Com[i]=='':
                df1.loc[df1['commercial name']==session['post_commercial_lst'][i],'commercial name']=session['post_commercial_lst'][i]
            else:
                df1.loc[df1['commercial name']==session['post_commercial_lst'][i],'commercial name']=change_Com[i]
        df1.to_csv(SAVE_FOLDER+'POSTLOG_commercials.csv',index=False)
        
        
        
        
        if choice=='Yes':
        
            for i in range(len(change_Com)):
                if  change_Com[i]!='':
                    t=change_Com[i]
                    ID=(df2.loc[df2['commercial_name']==t,'commercial_id'].unique().tolist())[0]
                    Length=(df2.loc[df2['commercial_name']==t,'Length'].unique().tolist())[0]
                    
                    s=session['post_commercial_lst'][i]
                    t_end=int(time.time())
                    updated_time=time.strftime(time_format,time.gmtime(t_end))
                 
                    c.execute('INSERT INTO COMMERCIAL_MAPPING VALUES (?,?,?,?,?,?)',(session['customer_id'],s,t,ID,Length,updated_time))
            conn.commit()
            conn.close()
        
        global dup_dict
                 
        display=display_ambigu_func(dup_dict)
        checked_match_lst=[display.iloc[i]['label'] for i in range(len(display)) if display.iloc[i]['airing time utc-TVA'] is not np.nan]
                
                
        missing=df1[~df1['label'].isin(verified_airings_df1['label'].tolist())]
        total_missing=missing[~missing['label'].isin(checked_match_lst)]
                
        total_missing.to_csv(SAVE_FOLDER+'Total missing airings.csv',index=False)
        return render_template('ambiguous.html',data1=display)
    elif session['record']!=[] and session['compl_record']!=[]:
        if choice=='Yes':
            for i in range(len(change_Com)):
                if  change_Com[i]!='':
                    t=change_Com[i]
                    ID=(df2.loc[df2['commercial_name']==t,'commercial_id'].unique().tolist())[0]
                    Length=(df2.loc[df2['commercial_name']==t,'Length'].unique().tolist())[0]
                    
                    s=session['df1_slice_dict_lst'][i]
                    t_end=int(time.time())
                    updated_time=time.strftime(time_format,time.gmtime(t_end))
                   
                    c.execute('INSERT INTO COMMERCIAL_MAPPING VALUES (?,?,?,?,?,?)',(session['customer_id'],s,t,ID,Length,updated_time))
            conn.commit()
            conn.close()
        global dup_dict
                 
        display=display_ambigu_func(dup_dict)
        checked_match_lst=[display.iloc[i]['label'] for i in range(len(display)) if display.iloc[i]['airing time utc-TVA'] is not np.nan]
                
                
        missing=df1[~df1['label'].isin(verified_airings_df1['label'].tolist())]
        total_missing=missing[~missing['label'].isin(checked_match_lst)]
                
        total_missing.to_csv(SAVE_FOLDER+'Total missing airings.csv',index=False)
        return render_template('ambiguous.html',data1=display)
       
   
   
        
        
        
        
        
        
        
        
                
        
       
       
   
   if __name__=='__main__':
    app.run(debug=True) 
