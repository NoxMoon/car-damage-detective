import configparser
import os, sys, glob
import psycopg2
import redshift_tool
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict, Counter
import operator
import gc
from IPython.display import HTML

config = configparser.ConfigParser()
config.read(os.path.expanduser('~/db_config.ini'))

def db_connection(database_name='DB'):
    conn = psycopg2.connect(
            database=config[database_name]['DATABASE'],
            host=config[database_name]['HOST'],
            port=config[database_name]['PORT'],
            user=config[database_name]['USERNAME'],
            password=config[database_name]['PASSWORD']
      )
    return conn
    
def dataframe_from_sql_query(connection, sql_query):
    cursor = connection.cursor()
    cursor.execute(sql_query)
    column_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(list(rows), columns=column_names)

def run_sql(connection, sql_query):
    cursor = connection.cursor()
    cursor.execute(sql_query)
    connection.commit()
    if cursor.description is not None:
        column_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        re = pd.DataFrame(list(rows), columns=column_names)
    else:
        re = None
    return re

def dataframe_to_database(df, table_name, schema = 'data_science_dev'):
    run_sql(db_connection(), 'drop table if exists ' + schema + '.' + table_name)
    redshift_tool.query(data=df, method='append',
                    redshift_auth={'db': config['DB']['database'], 
                                   'port': config['DB']['port'],
                                   'user': config['DB']['username'], 
                                   'pswd': config['DB']['password'], 
                                   'host': config['DB']['host']},
    s3_auth={'accesskey': config['AWS']['accesskey'],
                             'secretkey': config['AWS']['secretkey'],
                             'bucket': config['AWS']['bucket']},
                    schema=schema, table=table_name)


def compute_lr_by_group(df_, group, loss='ult_loss', premium='ol_premium'):
    N = np.exp(1.3)
    df = df_.copy(deep=True)
    for col in group:
        if df[col].isnull().sum()>0:
            df[col].fillna('nan', inplace=True)
    gr_df = df.groupby(group).agg({loss:sum, premium:sum})
    gr_df['lr'] = gr_df[loss]/gr_df[premium]
    gr_df['count'] = df.groupby(group).size()
    gr_df['std'] = N/gr_df['count'].apply(np.sqrt)
    gr_df['res'] = gr_df[['lr','std']].apply(lambda x: str(round(x[0],2))+'('+str(round(x[1],2))+')', axis=1)
    return gr_df


hide_cell = '''<script>
code_show=true; 
function code_toggle() {
    if (code_show){
        $('div.cell.code_cell.rendered.selected div.input').hide();
    } else {
        $('div.cell.code_cell.rendered.selected div.input').show();
    }
    code_show = !code_show
} 

$( document ).ready(code_toggle);
</script>

To show/hide this cell's raw code input, click <a href="javascript:code_toggle()">here</a>.'''

hide_all = '''
<script>
    code_show=true; 
    function code_toggle() {
     if (code_show){
     $('div.input').hide();
     } else {
     $('div.input').show();
     }
     code_show = !code_show
    } 
    $( document ).ready(code_toggle);
</script>
<form action="javascript:code_toggle()">
    <input type="submit" value="Click here to toggle on/off the raw code.">
</form>'''
