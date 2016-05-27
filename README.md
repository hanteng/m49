# m49
M49 resources (data including codes, names and categorization) based on UN and Unicode CLDR publicly available sources.

# Basic Descriptions of the files in /scripts folder

## m49regin.py

Retrieve UN M.49 data directly from a web page hosted by 
[United Nations Statistics Division official on UN M.49 regions called m49regin.htm](http://unstats.un.org/unsd/methods/m49/m49regin.htm)

Note: this m49regin.htm file is also linked by the Unicode CLDR Territory Containment (UN M.49) to be described below.

## supplementalData.py
Retrieve UN M.49 data from a structured XML file hosted and compiled by 
[Unicode Common Locale Data Repository (CLDR) Project in its supplementalData.xml](http://unicode.org/repos/cldr/trunk/common/supplemental/supplementalData.xml)

Note: Unicode CLDR provides human-readable descriptions regarding the UN M.49 data in its 
[Territory Containment (UN M.49)](http://www.unicode.org/cldr/charts/latest/supplemental/territory_containment_un_m_49.html)
page, citing the UN M.49 as its main data source with the m49regin.htm link.

## _cf_m49region_Unicode_UN.py

Compare the retrived UN M.49 data provided by the United Nations Statistics Division with that by the Unicode CLDR.

# Descriptions of the input/output files 
## m49regin.py
Retrieve m49regin.htm from the Web if there is no local file stored, and generate the following data files in tsv format
'm49regin.tsv' 
'm49regin_country.tsv'
'm49regin_country_no.tsv'

## supplementalData.py
Retrieve supplementalData.htm from the Web if there is no local file stored, and generate the following data files in tsv format
'CLDR_web.tsv' 
'CLDR_web_regin_country_no.tsv'

## _cf_m49region_Unicode_UN.py
Retrieve following stored tsv files, 
'm49regin_country.tsv' 
'CLDR_web.tsv'

and generate the following data files in tsv format
'_cf_m49_cldr_name_comparison.tsv' which details the NAMEs of the two data sources into four categories: "exactly same", "same length only", "cldr longer", "m49 longer", and "misc"
'_cf_m49_cldr_in_cldr_without_numeric.tsv' details those CLDR data entries without UN M.49 Numeric data (some of them have top-level domain names) 
'_cf_m49_cldr__region_categorization_diff' which details the differences and missing values of the REGION CATEGORIZATIONS
'_cf_m49_cldr.tsv' and '_cf_m49_cldr__join.tsv' both of which integrates both data sources with information of both, only in slightly different column names and order.

Note: Some files are generated with suffix of "_zh" which provide data with column names in Chinese language.


