# -*- coding: utf-8 -*-
#歧視無邊，回頭是岸。鍵起鍵落，情真情幻。
from lxml.html import etree
from lxml.html import parse
from io import StringIO, BytesIO
import requests
import codecs
import os

path_data = u'../data'

data_src_url = u'http://unstats.un.org/unsd/methods/m49/m49regin.htm'
data_src_path, data_src_local = os.path.split(data_src_url)
encoding_source="ISO-8859-1"

fn_output1 = os.path.join (path_data, 'm49regin.tsv')
fn_output2 = os.path.join (path_data, 'm49regin_country.tsv')
fn_output3 = os.path.join (path_data, 'm49regin_country_no.tsv')

## Parsing data from remote or local sources
try:
    tree = parse(data_src_local) #, parser=etree.XMLParser(recover=True)
except:
    r = requests.get(data_src_url, stream=True)
    #r.raw.decode_content = True

    if not( r.status_code == 200):
        print ("Downloading the data from {0} failed. Plese check Internet connections.".format(data_src_url))
        exit()
        
    r.encoding = 'iso-8859-1'
    XML_src=r.text #content# r.raw.read()#r.raw#r.text

    # Missing <tr> in the source just after Saint-Barth&eacute;lemy
    XML_src = XML_src.replace("Saint-Barth&eacute;lemy</p></td>\r\n        </tr>","Saint-Barth&eacute;lemy</p></td>\r\n        </tr><tr>")
    
    with codecs.open(data_src_local, mode="w",  encoding=r.encoding) as file:
        file.write(XML_src) #.decode(XML_encoding)

    tree = parse(data_src_local)

import pandas as pd
import numpy as np

## To get data:  UN m49
_xpath='''//*/table[2]/tbody/tr/td[3]/table[4]/tbody/tr'''
_xpath='''//*/table[2]//*/td[3]/table[4]//tr'''

list_matched = tree.xpath(_xpath)
list_processed=[]
category_current=""
flag_economic_regions = False

for i,t in enumerate(list_matched):
    sel=list_matched[i].findall
    item_code = list_matched[i].findall("td")[0].text_content().strip()
    item_content = list_matched[i].findall("td")[1].text_content().strip().split('\r\n')[0].strip()

    # print ([item_code,item_content]) # debugging

    try:
        category_or_not=len(list(list_matched[i].findall("td")[1].iterfind(".//b")))
    except:
        category_or_not=False

    #print (category_or_not)


    if category_or_not:
        category_current = item_code

    if item_content=="Developing regions":
        category_current = "developing"
    
    if item_content=="Developed regions":
        category_current = "developed"
        

    if "excluding" in item_content:
        flag_excluding=True
    else:
        flag_excluding=False

    if item_content=="Developed and developing regions c/":
        flag_economic_regions=True


    # Fixing double spaces with one
    item_content = item_content.replace("  "," ")  #China,  Hong Kong Special Administrative Region

    row = (item_code, item_content, category_current, flag_economic_regions, flag_excluding)

    #print ("{},".format((i,item_code)), end='\t')
    if item_code=='659': # Checking and debugging
        print(row)

    if item_code=='' and item_content=='':
        pass
    else:
        list_processed.append(row)


df__=pd.DataFrame(list_processed)
df__.columns=["numeric", "countryname", "region", "economic", "excluding"]

df = df__[1:].set_index("numeric")


#df = parse_UN_m49region('////*//table[2]//tbody//tr//td[3]//table[4]//tbody//tr')

#print df_basic['comments']['AX']
#print df_basic['comments']['TW']
print (len(df))


# Saving output 1
df.to_csv(fn_output1, sep='\t', encoding="utf8")

## Region mapping
df_mapping=df['countryname']
df_mapping=df_mapping.reset_index().drop_duplicates(subset='numeric', keep='first').set_index('numeric')

#df[df.category==df.index]
df=df[df.region!=df.index]

# Saving output 2
df_country=df[df.economic==False]
df_country.to_csv(fn_output2, sep='\t', encoding="utf8")


# Saving output 3 
df_cat = df[df.economic==False].groupby(by="region")

df_cat = df_cat.count()['countryname']
df_cat = df_cat[df_cat>1].to_frame()
df_cat.columns=["count"]

df_cat['r_long_d']=[df_mapping['countryname'][x] for x in df_cat.index]

df_cat.sort_values(by='r_long_d').to_csv(fn_output3, sep='\t', encoding="utf8")

#Exampler operations
df[df.region=='015']  # query on countries in Northern Africa [015]
df[df.region=='029']  # query on countries in Caribbean [029]
df[df.index=='728']  # query on country 728 South Sudan
df[df.index=='659']  # query on country 659 Saint Kitts and Nevis
df[df.index=='248']  # query on country 248  Åland Islands
'''
                   countryname region economic excluding
numeric                                                 
659      Saint Kitts and Nevis    029    False     False
659      Saint Kitts and Nevis    722     True     False
'''
