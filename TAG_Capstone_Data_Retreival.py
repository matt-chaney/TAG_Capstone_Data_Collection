# -*- coding: UTF-8 -*-

import glob2
import glob
import zipfile
import re
import pandas as pd
import wget

# Downloads OES data files directly from BLS
# Documentation and other data formats: http://www.bls.gov/oes/tables.htm
for i in range(11, 16):
    url = "{0}{1}{2}".format("http://www.bls.gov/oes/special.requests/oesm", i, "all.zip")
    print "Downloading", url
    filename = wget.download(url)

# unzip OES data
zips = glob.glob('*.zip')

for i in zips:
    print "Unzipping", i
    zipfile.ZipFile(i).extractall()

# define dictionary
year_files = {}
# regex for two digit date in OES filenames
regex = re.compile(".*(\d\d).*")

# filter parameters
area_list = [12020,12060,12260,17980,13,42340,99,47580]
stem_list = ['11-3021', '11-9041', '11-9121', '15-1111', '15-1121', '15-1122', '15-1131', '15-1132', '15-1133',
             '15-1134', '15-1141', '15-1142', '15-1143', '15-1151', '15-1152', '15-1199', '15-2011', '15-2021',
             '15-2031', '15-2041', '15-2091', '15-2099', '17-1021', '17-1022', '17-2011', '17-2021', '17-2031',
             '17-2041', '17-2051', '17-2061', '17-2071', '17-2072', '17-2081', '17-2111', '17-2112', '17-2121',
             '17-2131', '17-2141', '17-2151', '17-2161', '17-2171', '17-2199', '17-3011', '17-3012', '17-3013',
             '17-3019', '17-3021', '17-3022', '17-3023', '17-3024', '17-3025', '17-3026', '17-3027', '17-3029',
             '17-3031', '19-1011', '19-1012', '19-1013', '19-1021', '19-1022', '19-1023', '19-1029', '19-1031',
             '19-1032', '19-1041', '19-1042', '19-1099', '19-2011', '19-2012', '19-2021', '19-2031', '19-2032',
             '19-2041', '19-2042', '19-2043', '19-2099', '19-4011', '19-4021', '19-4031', '19-4041', '19-4051',
             '19-4091', '19-4092', '19-4093', '25-1021', '25-1022', '25-1032', '25-1041', '25-1042', '25-1043',
             '25-1051', '25-1052', '25-1053', '25-1054', '41-4011', '41-9031', '11-3021']
index_list = [0,1000,2000,4000,5000]

finalColumns=['Area FIPS','Location','Area Type','Occupation','Employed','Mean Hourly Wage','Mean Annual Salary','Median Hourly Wage','Median Annual Salary','Metric Year','Index Component','Score','Rank (Of 380)']


# find all *.xlsx files under working directory
xlsxs = glob2.glob('**\*.xlsx')

# Match two digit years in filenames, convert to 20* 4-digit year, then create a year:filename lookup table
for i in xlsxs:
    m = regex.match(i)
    yr = m.group(1)
    yr = "20" + yr
    year_files[yr] = i

first = 1
# Read the first sheet of each excel workbook into a dataframe, then write to csv. Skip headers after first file.
for year, filen in year_files.iteritems():
    print "Filtering and Processing:", year, filen
    df = pd.read_excel(filen)
    # Column names to lowercase, remove underscores
    # Required due to BLS changes in 2012
    df.columns = df.columns.str.strip().str.lower().str.replace('_', ' ')

    #Keep only required columns
    df=df[['area','area title','area type','occ title','tot emp','h mean','a mean','h median','a median','occ code']]

    #Filter by Area List and STEM list
    filt_df = df[df.area.isin(area_list)]
    filt_df = filt_df[filt_df['occ code'].isin(stem_list)]

    #Change 12060 MSA Title to reflect update in FIPS definition during reporting timeframe
    filt_df.replace({'Atlanta-Sandy Springs-Marietta, GA': 'Atlanta-Sandy Springs-Marietta-Rowswell, GA'}, regex=True)
    filt_df.replace({'Atlanta-Sandy Springs-Roswell, GA': 'Atlanta-Sandy Springs-Marietta-Rowswell, GA'}, regex=True)

    filt_df['year'] = year
    if first == 1:
        # Write to CSV - Removed to work in DataFrames
        # filt_df.to_csv("C:\\code\\tag-data-11\\oes\\test4.csv",index=False,columns=['area','area title','area type','occ title','tot emp','h mean','a mean','h median','a median','year'])

        # Write to OES-Final dataframe
        print "Writing " + str(year) + " to Filtered DataFrame"
        global oes_filt
        oes_filt = filt_df.copy(deep=True)
        first = 0
    else:
        # Write to CSV - Removed to work in DataFrames
        # filt_df.to_csv("C:\\code\\tag-data-11\\oes\\test4.csv", mode='a',index=False,columns=['area','area title','area type','occ title','tot emp','h mean','a mean','h median','a median','year'], header=False)

        # append to #oes_filt
        print "Writing " + str(year) + " OES Data to Filtered DataFrame"
        oes_filt = oes_filt.append(filt_df)

#Drop occ-code after filtering
oes_filt=oes_filt[['area','area title','area type','occ title','tot emp','h mean','a mean','h median','a median','year']]
#Rename columns to final format
oes_named_columns=['Area FIPS','Location','Area Type','Occupation','Employed','Mean Hourly Wage','Mean Annual Salary','Median Hourly Wage','Median Annual Salary','Metric Year']
oes_filt.columns=oes_named_columns
oes_filt.reindex(columns=finalColumns)

# Begin Innovation Index retrieval and processing
print "Downloading Innovation Index from: https://www.statsamerica.org/ii2/docs/downloads/Metros.xlsx"

innovation_index_filename = wget.download('https://www.statsamerica.org/ii2/docs/downloads/Metros.xlsx')
ii_df = pd.read_excel(innovation_index_filename)

#Keep only relevant columns


# Filter by Area List
filt_ii_df = ii_df[ii_df.metrofips.isin(area_list)]

#Rename Area Code columns in dataframes to be joined to create common key name
#oes_filt.rename(columns={'area': 'fips'}, inplace=True)
#filt_ii_df.rename(columns={'metrofips': 'Area FIPS'}, inplace=True)

# Filter By index Code list
filt_ii_df=filt_ii_df[ii_df.code_id.isin(index_list)]

filt_ii_df=filt_ii_df[['vintage_year','metrofips','description','index value','rank (out of 380)','code_description']]
filt_ii_df.columns=[['Year','Area FIPS','Location','Score','Rank (Out of 380)','Index']]
    # Replace unicode (non-ascii) right-quote with ascii apostrophe in all records
    # -- Removed this line after changing encoding of output and source: May revert to be ASCII-only
    # filt_ii_df.replace({u'\u2019','\''})

# Change Index code names to preferred output names
filt_ii_df.replace({'Atlanta-Sandy Springs-Marietta, GA': 'Atlanta-Sandy Springs-Marietta-Rowswell, GA'}, regex=True)
filt_ii_df.replace({'Atlanta-Sandy Springs-Roswell, GA': 'Atlanta-Sandy Springs-Marietta-Rowswell, GA'}, regex=True)



# Join - 'Left outer join' - Append Innovation Index Scores w/ FIPS to end of DataFrame
#merged = filt_ii_df.copy()
#merged.merge(oes_filt, on='fips', how='outer')

merged=merged.rename(
    columns={'fips': 'area_fips', 'area title': 'Location', 'area type': 'Area Type', 'occ title': 'Occupation',
             'tot emp': 'Employed', 'h mean': 'Mean Hourly Wage', 'a mean': 'Mean Annual Salary',
             'h median': 'Median Hourly Wage', 'a median': 'Median Annual Salary', 'year': 'Metric Year',
             'description': 'Location2', 'code_id': 'index_id', 'code_description': 'Index Component',
             'vintage_year': 'vYear', 'index value': 'Score', 'rank (out of 380)': 'Rank (Of 380)',
             'median value': 'Median Index Value'})

print "Writing final csv to merged_final_oes_and_ii.csv"
# merged.to_csv("C:\\code\\tag-data-11\\oes\\merged.csv",encoding='utf-8')
merged.to_csv("merged_final_oes_and_ii.csv", index=False,
              columns=['Area FIPS', 'Location', 'Area Type', 'Occupation', 'Employed', 'Mean Hourly Wage',
                       'Mean Annual Salary', 'Median Hourly Wage', 'Median Annual Salary', 'Metric Year',
                       'Index Component', 'Score', 'Rank (Of 380)'], encoding='utf-8')