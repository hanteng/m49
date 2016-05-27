# -*- coding: utf-8 -*-
#歧視無邊，回頭是岸。鍵起鍵落，情真情幻。
from lxml.html import etree
from io import StringIO, BytesIO
import requests
import codecs
import os

path_data = u'../data'

data_src_url = u'http://unicode.org/repos/cldr/trunk/common/supplemental/supplementalData.xml'
data_src_path, data_src_local = os.path.split(data_src_url)
encoding_source = "utf-8"

fn_output1 = os.path.join (path_data, 'CLDR_web.tsv')
fn_output3 = os.path.join (path_data, 'CLDR_web_regin_country_no.tsv')

## Parsing data from remote or local sources
try:
    tree = etree.parse(data_src_local) #etree.parse was used to parse xml
except:
    r = requests.get(data_src_url, stream=True)

    if not(r.status_code == 200):
        print ("Downloading the data from {} failed. Plese check Internet connections.".format(data_src_url))
        exit()
        
    r.encoding = 'utf-8'
    XML_src = r.text #content unicode....
       
    with codecs.open(data_src_local, mode="w",  encoding="utf-8") as file:
        file.write(XML_src)
        
    tree = etree.parse(data_src_local)  

import pandas as pd
import numpy as np

def parse_generic(_xpath, _com):
    list_matched = tree.xpath(_xpath)
    list_processed=[]
    for i,t in enumerate(list_matched):
        data_dict=dict(zip(t.keys(),t.values()))
        if _com=="getnext":
            data_dict['comments'] = t.getnext().text.strip() #unicode(t.getnext().text.strip())
        else:
            if _com=="getchildren":
                data_dict['comments'] = t.getchildren()[0].text.strip() #unicode(t.getchildren()[0].text.strip())
        #debug
        if i==0:
            print ("Debug:{}".format(data_dict))
        list_processed.append(data_dict)
    df__=pd.DataFrame(list_processed)
    return df__

## To get data:  c_name gdp literacyPercent population
df_basic = parse_generic('//territoryInfo/territory',"getchildren" ).set_index('type')
#print df_basic['comments']['AX']
#print df_basic['comments']['TW']
print (len(df_basic))

## To get data: UN categorization
df_containment_UN= parse_generic('//territoryContainment/group[not(@grouping="true")]', "getnext")

categorization_UN=dict()
map_left=[x.split(" ") for x in list(df_containment_UN.contains)]
map_right=list(df_containment_UN.comments.replace("Southern Europe, XK not in UN data","Southern Europe") ) #.type
map_right_type=list(df_containment_UN.type) #.type
for i,left in enumerate(map_left):
    for item_left in left:
        if pd.isnull(map_right_type[i]) or pd.isnull(map_right[i]):
            categorization_UN[item_left]={"code":map_right_type[i] , "cat": map_right[i] }
        else:
            categorization_UN[item_left]={"code":map_right_type[i] , "cat": map_right[i] }
        
#print categorization_UN['TW']
df_cat_UN=pd.DataFrame(categorization_UN).transpose()
print (len(df_cat_UN))


## To get data: Code mapping
df_mapping=parse_generic('//codeMappings/territoryCodes',"" )
df_mapping23=df_mapping.set_index('type')['alpha3']
df_mapping32=df_mapping.set_index('alpha3')['type']
df_mappingn2=df_mapping.set_index('numeric')['type']
df_mappingn2=df_mapping.set_index('numeric')['type']
df_mapping2n=df_mapping.set_index('type')['numeric']
df_mapping3n=df_mapping.set_index('alpha3')['numeric']

## Constructing working integrated dataframe
df=df_basic.copy()
df['alpha3']=[df_mapping23[x] for x in df.index]
df['numeric']=[df_mapping2n[x] for x in df.index]
df['cat_UN']=[df_cat_UN["code"].get(x, np.nan) for x in df.index]
df['categorization_UN']=[df_cat_UN["cat"].get(x, np.nan) for x in df.index]

df=df.reset_index()


## UN higher category just under the World
under_the_World_UN=df_cat_UN[df_cat_UN.cat=="World"].index
df_cat_UN[df_cat_UN.code.isin(under_the_World_UN)].index

##>>> list(df.columns)
##['typ2','comments', 'gdp', 'literacyPercent', 'population', 'alpha3', 'numeric', 'cat_UN', 'categorization_UN']
df.columns=['countrycode2','countryname', 'gdp', 'literacyPercent', 'population', 'countrycode', 'numeric', 'region', 'r_long_d']


## Dealing with those without ISO alpha_3 http://www.fact-index.com/i/is/iso_3166_1_alpha_2.html
##>>> df[pd.isnull(df.countrycode)]
##    countrycode2      countryname          gdp literacyPercent population  \
##65            EA  Ceuta & Melilla   4364000000            97.7     150000   
##104           IC   Canary Islands  61060000000            97.7    2098590 
df.loc[pd.isnull(df.countrycode),'countrycode']="_"+df[pd.isnull(df.countrycode)]['countrycode2']


##
fileds_selected_categories=['countrycode', 'countryname', 'countrycode2', 'numeric', 'region', 'r_long', 'r_long_d']

df['r_long']=[df_cat_UN[df_cat_UN.code.isin(under_the_World_UN)]['cat'].get(x, None) for x in df['region'] ]

df=df[fileds_selected_categories].set_index("countrycode")

df.to_csv(fn_output1, sep='\t', encoding="utf8")

#Unicode Checking
#print(df["countryname"]["ALA"])
#print(df.loc["CIV"])

## Region mapping
df_mapping=df[['region','r_long_d']]
df_mapping=df_mapping.reset_index().drop_duplicates(subset='region', keep='first').set_index('region')
df_mapping=df_mapping[['r_long_d']]


df_cat = df.groupby(by="region")

df_cat = df_cat.count()['countryname']
df_cat = df_cat.to_frame() #Series to Frame
df_cat.columns=["count"] 

df_cat['r_long_d']=[df_mapping['r_long_d'][x] for x in df_cat.index]
df_cat.sort_values(by='r_long_d').to_csv(fn_output3, sep='\t', encoding="utf8")


#df[df.r_long_d=='Northern Africa']
