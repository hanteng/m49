# -*- coding: utf-8 -*-
#歧視無邊，回頭是岸。鍵起鍵落，情真情幻。
import os
import pandas as pd

path_data = u'../data'

fn_input1 = os.path.join (path_data, 'm49regin_country.tsv')
fn_input2 = os.path.join (path_data, 'CLDR_web.tsv')

## Data Preprocessing
df = dict()
df['m49']  = pd.read_csv(fn_input1, dtype = {'numeric':object, 'region':object}, sep='\t', encoding="utf8", keep_default_na=False, na_values=[''])           
df['cldr'] = pd.read_csv(fn_input2, dtype = {'numeric':object, 'region':object}, sep='\t', encoding="utf8", keep_default_na=False, na_values=[''])  

# Finding missing values
def find_missing(dataf, column_missing, column_order_list):
    df=dataf[dataf[column_missing].isnull()]
    df=df[column_order_list]
    return df

# Finding missing values "numeric" in CLDR 
df['in_cldr_without_numeric'] = find_missing(df['cldr'], 'numeric', ['numeric','countrycode2','countrycode','countryname','region','r_long'])   #6        
df['in_m49_without_numeric'] = find_missing(df['m49'], 'numeric', ['numeric','economic','excluding'])                                           #0

# Checking the filled outcomes
print (len(find_missing(df['cldr'], 'numeric', ['numeric','countrycode2','countrycode','countryname','region','r_long'])))

# Filling missing values "numeric" in CLDR 
for i in df['in_cldr_without_numeric'].index:
    #df['cldr']['numeric'][i] = i*-1
    df['cldr'].loc[(i,'numeric')] = i * -1

# Checking the filled outcomes
print (len(find_missing(df['cldr'], 'numeric', ['numeric','countrycode2','countrycode','countryname','region','r_long'])))

#df['m49_missing_codes_but_included_in_cldr'] = find_missing(df['cldr'], 'numeric', ['numeric','countrycode2','countrycode','countryname','region','r_long'])
#>>> None


import math

# Preprocessing, coverting values "numeric" and "region" in CLDR and m49 from integer to 3-digit strings
def filter_3_digit(val):
    if (val=="QO"):
        return "QO"
    if pd.isnull(val):
        return "NaN"
        print ("{}\t".format(val))
    else:
        return '{0:0{width}}'.format(int(val), width=3)


for d in ['cldr','m49']:
    for col in ['numeric', 'region']:
        df[d][col] = [filter_3_digit(x) for x in df[d][col]]
#df['cldr']['numeric'] = ['{0:0{width}}'.format(x, width=3) for x in df['cldr']['numeric']]
#df['m49']['numeric'] = ['{0:0{width}}'.format(x, width=3) for x in df['cldr']['numeric']]


## Joining the two datasets
df['_join']=pd.merge(df['m49'], df['cldr'], on='numeric', suffixes=('_left', '_right'), how="outer")
df['_join_inner']=pd.merge(df['m49'], df['cldr'], on='numeric', suffixes=('_left', '_right'), how="inner")
df['_join']=df['_join'].set_index('numeric')#.sort_values(['numeric','region_right','countrycode2'])


## Comparing the two datasets
# region categorization
list_col=["countrycode", "countrycode2", "countryname_left", "countryname_right", "region_left", "region_right", ]
df_=df['_join'].sort_values(['region_right','countrycode2'])[list_col]
df['_region_categorization_diff']=df_[df_.region_right!=df_.region_left]

def len_(x):
    import math

    try:
        return len(x)
    except:
        if isinstance(x, float) and math.isnan(float(x)):
            #print(x)
            return 0
            
# Names
# length
df['_join']["len_l"]=[len_(x) for x in df['_join'].countryname_left]
df['_join']["len_r"]=[len_(x) for x in df['_join'].countryname_right]

#
#df["_longer_name_same"]=df['_join'].query('countryname_left==countryname_right')    #194
#df["_longer_name_same_length_only"]=df['_join'].query('len_l==len_r and countryname_left!=countryname_right')    #2
#df["_longer_name_cldr"]=df['_join'].query('len_l<len_r and len_l>0')    # 5
#df["_longer_name_m49"]=df['_join'].query('len_r<len_l and len_r>0')     #38 	

# Length queries and assigning values to df["_join"]["len_cat"]
# Four categories:
dict_len_cat={99:"misc", 1:"exactly same", 2:"same length only", 3:"cldr longer", 4:"m49 longer",}
dict_len_cat_query={1:'countryname_left==countryname_right', 2:'len_l==len_r and countryname_left!=countryname_right', 3:'len_l<len_r and len_l>0', 4:'len_r<len_l and len_r>0',}

categorization_results={}
for k in dict_len_cat_query.keys():
    list_numeric = list(df['_join'].query(dict_len_cat_query[k]).index)
    for x in list_numeric:
        categorization_results[x] = k 

print(len(categorization_results))


df['_join']['len_cat'] = 99
df['_join']['len_cat'].update(pd.Series(categorization_results))


print ("Exactly the same name: {}".format(len(df['_join'].query('len_l==len_r & countryname_left==countryname_right'))))
#print ("CLDR name longer: {}".format(len(df_q)))

print (df['_join'].query('len_l==len_r & countryname_left!=countryname_right'))
df['_join']['len_cat'].update(pd.Series(categorization_results))

grouped_table = df['_join'].reset_index().groupby(['len_cat'])['numeric'].apply(list).to_frame()
grouped_table["Category"]=[dict_len_cat[x] for x in grouped_table.index]

#dict_numeric_alpha3 = df['cldr'][['numeric', 'countrycode']].set_index('numeric')['countrycode'].to_dict()
#dict_numeric_alpha2 = df['cldr'][['numeric', 'countrycode2']].set_index('numeric')['countrycode2'].to_dict()
dict_numeric_name = df['cldr'][['numeric', 'countryname']].set_index('numeric')['countryname'].to_dict()

def reporting_numeric(somelist):
    try:
        somelist.sort()
    except:
        pass
    try:
        outcomes=["{}[{}]".format(dict_numeric_name.get(x,""),x) for x in somelist]

    except:
        pass
    return outcomes

grouped_table["Countries"]=[", ".join(reporting_numeric(x)) for x in list(grouped_table.numeric)]
grouped_table["Number"]=[len(x) for x in list(grouped_table.numeric)]
label="name_comparison"
grouped_table.to_csv(os.path.join (path_data,"_cf_m49_cldr_{}.tsv".format(label)), sep='\t', encoding="utf8", index=False, na_rep='(missing)', columns=['Category','Number','Countries'])


# Reporting
print ("The lengths of m49, CLDR, and the joined dataset are {}, {}, {}".format(len(df['m49']),len(df['cldr']),len(df['_join']) ) )

#print (df['m49_missing_codes_but_included_in_cldr'])     # missing UN numeric codes, but included in CLDR, possibly because of the ISO country codes/top-level domain names
label='in_cldr_without_numeric'
df[label].sort_values(['region','countrycode2']).to_csv(os.path.join (path_data, "_cf_m49_cldr_{}.tsv".format(label)), sep='\t', encoding="utf8", index=False, na_rep='(missing)')



label='_join'
list_col=["numeric","countrycode", "countrycode2", "countryname_left", "countryname_right", "region_left", "region_right", ]
df_=df[label].reset_index().sort_values(['region_right','countrycode2'])[list_col]
df_.to_csv(os.path.join (path_data, "_cf_m49_cldr_{}.tsv".format(label)), sep='\t', encoding="utf8", index=True, na_rep='(missing)')

list_col_zh=["數字代碼","三字母碼", "二字母碼", "M49國家英文名", "CLDR國家英文名", "M49國家分類", "CLDR國家分類", ]
df_.columns=list_col_zh
df_.to_csv(os.path.join (path_data, "_cf_m49_cldr_{}_zh.tsv".format(label)), sep='\t', encoding="utf8", index=False, na_rep='缺失值')


label='_region_categorization_diff'
list_col=["countrycode", "countrycode2", "countryname_left", "countryname_right", "region_left", "region_right", ]
df_=df[label].sort_values(['region_left','region_right','countrycode2'])
df_.to_csv(os.path.join (path_data, "_cf_m49_cldr_{}.tsv".format(label)), sep='\t', encoding="utf8", index=False, na_rep='(missing)')

list_col_zh=["三字母碼", "二字母碼", "M49國家英文名", "CLDR國家英文名", "M49國家分類", "CLDR國家分類", ]
df_.columns=list_col_zh
df_.to_csv(os.path.join (path_data, "_cf_m49_cldr_{}_zh.tsv".format(label)), sep='\t', encoding="utf8", index=False, na_rep='缺失值')



#exit()



def set_compare(x, y):
    set_x = set (x)
    set_y = set (y)
    set_inter = set_x.intersection(set_y)
    set_diffxy = set_x.difference(set_y)
    set_diffyx = set_y.difference(set_x)
   
    return [set_inter, set_diffxy, set_diffyx]

cf = set_compare(df['m49']['numeric'], df['cldr']['numeric'])
#print (cf)
cf_outcomes = [sorted(list(x)) for x in cf][0]

print (cf_outcomes)
print ([len(x) for x in cf])


dict_numeric_alpha3 = df['cldr'][['numeric', 'countrycode']].set_index('numeric')['countrycode'].to_dict()
dict_numeric_alpha2 = df['cldr'][['numeric', 'countrycode2']].set_index('numeric')['countrycode2'].to_dict()
dict_numeric_name = df['cldr'][['numeric', 'countryname']].set_index('numeric')['countryname'].to_dict()
print ([dict_numeric_alpha3[x] for x in [y for y in cf_outcomes]])
print ([dict_numeric_alpha2[x] for x in cf_outcomes])
print ([dict_numeric_name[x] for x in cf_outcomes])

def countryname_lr(x, lr):
    if x=="countryname":
        return x+lr
    else:
        return x 

## Adding TWN and XKK for comparisons?

left  = df['m49'].set_index("numeric")
left.columns= [countryname_lr(x,"_left") for x in left.columns]
right = df['cldr'].set_index("numeric")
right.columns= [countryname_lr(x,"_right") for x in right.columns]
result = pd.concat([left, right], axis=1, join='inner')
print ("{}, {}, {}".format(len(left), len(right), len(result)))
result["len_l"]=[len(x) for x in result.countryname_left]
result["len_r"]=[len(x) for x in result.countryname_right]

#print result.query('len_l!=len_r & countryname_left!=countryname_right')
print (result.query('len_l==len_r & countryname_left!=countryname_right'))

result.to_csv(os.path.join (path_data, "_cf_m49_cldr.tsv"), sep='\t', encoding="utf8", index=False)

print (result[result.len_l<result.len_r])
print (len(result[result.len_l<result.len_r]))
print (result[result.len_l>result.len_r])
print (len(result[result.len_l>result.len_r]))
print (len(result[result.countryname_left==result.countryname_right]))

print (len(result[result.len_l==result.len_r]))

result = pd.concat([left, right], axis=1, join='outer')
print ("{}, {}, {}".format(len(left), len(right), len(result)))

