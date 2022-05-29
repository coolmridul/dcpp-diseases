#!/usr/bin/env python
# coding: utf-8

# # DCPP Gourp Assignment Code
# 
# Ajith Reddy 12120006
# 
# Anjana Rajan 12120088
# 
# Mridul Agarwal 12120075
# 
# Rohini Singh 12120059
# 
# Shantanu Srivastava 12120061

# In[1]:


# Importing required packages to scrape data and create dataframe
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from pprint import pprint
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import urllib
import os


# In[2]:


#Setting the main website from where data on diseases is to be extracted
disease_url = 'https://www.mayoclinic.org/diseases-conditions/index'
#Defining of URL to fetch data
base_url = "https://www.mayoclinic.org"
wiki_base_url = 'https://en.wikipedia.org/wiki/'


# In[3]:


#We have used requests.adater lib to simulate sleep in between successive request to the server
my_headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OSX 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/71.0.3578.98 Safari/537.36", 
          "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"}

def get_data_from_url(url):
    session = requests.Session()
    retry = Retry(connect= 3, backoff_factor= 0.5)
    adapter = HTTPAdapter(max_retries= retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    resp = session.get(url,headers=my_headers)
    return resp


# In[4]:


#Parse the html page to extract text using BeautifulSoup library
resp = get_data_from_url(disease_url)
doc = BeautifulSoup(resp.text, "html.parser")


# In[5]:


#The website contains data of diseases categorized alphabetically. 
#The URLs for the parent pages (categorized alphabetically) containing corresponding diseases starting with parent page alphabet are present under html tag 'href' which is in-turn present under parent hierarchy tags: ('ol class:"acces-alpha"' -> 'a'). 
#Hence, as a first step, we retrieve all tags under 'ol class:"acces-alpha"'
tags = doc.find('ol',{'class' : 'acces-alpha'})

#Next, we find all 'a' tags under the parent tag 'ol class:"acces-alpha"'
final_tag = tags.find_all('a')

#Since the href tag is part of the 'a' tag, we loop through each 'a' tag and fetch the corresponding href text. 
#Finally we concatnate the URL retrieved with the base URL of website. Resulting URL is the URL of parent pages (categorized alphabetically)
diseases_links = []
for disease_tag in tags.find_all('a'):
    links = disease_tag.get("href")
    diseases_links.append(base_url + links)


# In[6]:


#Now that we have the parent URL, we access each URL and fetch list of diseases and their corresponding links. 
#All disease URL are present within 'a' tag which is under parent tag ['div',{'id': 'index', 'class' : 'index content-within']
#So, we first get each URL and look for parent tag ['div',{'id': 'index', 'class' : 'index content-within'], then find all 'a' tags under this parent tag and then get the text of 'href' tags within each of the 'a' tags.
#Finally we concatnate the URL retrieved with the base URL of website. Resulting URL is the URL of the disease page

alpha_diseases_links1 = {}
for k in range(0,len(diseases_links)):
    try:
        response1 = get_data_from_url(diseases_links[k])
        doc1 = BeautifulSoup(response1.text, "html.parser")
        alphatags = doc1.find('div',{'id': 'index', 'class' : 'index content-within'})
        for alpha_disease_tag in alphatags.find_all('a'):
            alphalinks = alpha_disease_tag.get("href")
            alpha_diseases_links1[alpha_disease_tag.text.split(', also')[0]] =  base_url + alphalinks
    except:
        pass

#Appending the name of the disease and link to a dictionary, where key is the disease name and link is the value. We convert it to a dataframe.
#Then we group by on Link and append disease name with a ',' so that we dont have duplicate URL
alpha_diseases_links_df = pd.DataFrame({'Disease_Name' : alpha_diseases_links1.keys() , 'Link' : alpha_diseases_links1.values() })
alpha_diseases_links_df = pd.DataFrame(alpha_diseases_links_df.groupby('Link')['Disease_Name'].apply(','.join).reset_index())


# In[7]:


#Creating an empty dataframe with reuquired columns to store the data fetched from each disease page
#We are interested in retrieving data on 'Symptoms', 'Causes', 'Risk Factors', 'Compliations', 'Prevention', 'Diagnosis' and 'Treatment'. Hence creating a dataframes with these columns
disease_dataframe = pd.DataFrame(columns=['Disease_Name', 'URL', 'Symptoms','Causes','Complications','Prevention','Diagnosis','Treatment'])


# In[8]:


def get_data_from_link(data,data1):
    check_litext = data.find_next('ul').find_next('h2')
    a = []
    if check_litext:
        if (check_litext.getText() == data1.getText()):
            li_text = data.find_next('ul').find_all('li')
            if li_text:
                for text in li_text:
                    next_text = text.find('strong')
                    if next_text:
                        a.append(next_text.getText())
                    else:
                        a.append(text.getText()) 
        else:
            p_text = data.find_next('p')
            a.append(p_text.getText())
    
    return a


# In[9]:


#This is our main code to pick eack disease URL and fetch data under sections of interest (present in empty dataframe created earlier)

#Looping through each disease URL
alpha_diseases_links = alpha_diseases_links_df['Link'].tolist()
for loop in range(0,len(alpha_diseases_links)):
    try:
        #Creating an empty list for each category of interest
        sym_real_text = []
        cau_real_text = []
        risk_real_text = []
        comp_real_text =[]
        prev_real_text =[]
        dia_real_text = []
        treat_real_text = []
        
        
        response2 = get_data_from_url(alpha_diseases_links[loop])
        doc2 = BeautifulSoup(response2.text, "html.parser")
        
        #Retrieving disease name from 'a' tag present under parent 'h1' tag and storing it is a variable
        maintag = doc2.find('h1').find_next('a').getText()
        
        #Next, we are finding all heading of sections of interest. Heading of sections are present under 'h2' tag
        diseasetags = doc2.find_all('h2')
        
        #We then pick each 'h2' tag and check if it matches the section of interest.
        for k in range(0,len(diseasetags)):
            #First, we check for 'Symptoms' and then retrieve data under the section. Data is sometimes present as a paragraph under 'p' tag or as bullet points under 'li' tag which is under 'ul' tag
            #both 'p' tag and 'ul' tags are not child tags of parent 'h2' tag. Hence we need to have additional logic to check of type of tag present and ensure they hold the data for the required 'h2' tag
            #Logic used is as follows:
            # 1. Find the next 'ul' tag after the 'h2' = 'Symptoms' and the next 'h2' tag after the retrieved 'ul' tag 
            # 2. Then we check if the 'h2' tag retrieved in step:1 matches the next 'h2' tag is the list of 'h2' tags we fetched earlier (which is 'diseasetags')
            #    a. If they are same, then we are sure that there is a 'ul' tag after 'h2' tag = 'Symptoms'. Which means data is present in bullet points
            #    b. If they are not same, it means data is not present in bullet points, but present as a paragraph under 'p' tag
            # 3. Also, in few cases, the bullet point data are present directly under 'li' tag or under 'strong' tag under 'li' tag (which is in-turn present under 'ul' tag). So we check for this condition as well (if we confirm presence of 'ul' tag under the required 'h2' tag in step:2)
            if (diseasetags[k].getText() == 'Symptoms'):
                sym_real_text = get_data_from_link(diseasetags[k],diseasetags[k+1])
            
            #Next, we check for 'Causes' and then retrieve data under the section. Data is sometimes present directly under 'h2' tag (if 'h3' tag is not present after required 'h2' tag) or under 'h3' tag (if present after required 'h2' tag) as a paragraph under 'p' tag or as bullet points under 'li' tag which is under 'ul' tag
            #'h3', 'p' and 'ul' tags are not child tags of parent 'h2' tag. Hence we need to have additional logic to check type of tag present and ensure they hold the data for the required 'h2' tag
            #Logic used is as follows:
            # 1. Find next 'h3' tag after 'h2' = 'Causes'
            # 2. If 'h3' tag is present, we find the next 'ul' tag after the 'h3' = 'Causes' and the next 'h2' tag after the retrieved 'ul' tag 
            # 3. If 'h3' tag is not present, we find the next 'ul' tag after the 'h2' = 'Causes' and the next 'h2' tag after the retrieved 'ul' tag 
            # 4. Then we check if the 'h2' tag retrieved in step:2 or 3 matches the next 'h2' tag is the list of 'h2' tags we fetched earlier (which is 'diseasetags')
            #    a. If they are same, then we are sure that there is a 'ul' tag after 'h2' tag = 'Causes'. Which means data is present in bullet points. 
            #    b. If they are not same, it means data is not present in bullet points, but present as a paragraph under 'p' tag
            # 5. Also, in few cases, the bullet point data are present directly under 'li' tag or under 'strong' tag under 'li' tag (which is in-turn present under 'ul' tag). So we check for this condition as well (if we confirm presence of 'ul' tag under the required 'h2' tag in step:2)    
            if (diseasetags[k].getText() == 'Causes'):
                h3 = diseasetags[k].find_next('h3')
                if ("Causes" in h3.getText()):
                    cau_real_text = get_data_from_link(diseasetags[k],diseasetags[k+1])
                else:
                    cau_real_text = get_data_from_link(diseasetags[k],diseasetags[k+1])
            #Next, we check for 'Risk Factors' and then retrieve data under the section. Data is sometimes present as a paragraph under 'p' tag or as bullet points under 'li' tag which is under 'ul' tag
            #both 'p' tag and 'ul' tags are not child tags of parent 'h2' tag. Hence we need to have additional logic to check type of tag present and ensure they hold the data for the required 'h2' tag
            #Logic used is as follows:
            # 1. Find the next 'ul' tag after the 'h2' = 'Risk Factors' and the next 'h2' tag after the retrieved 'ul' tag 
            # 2. Then we check if the 'h2' tag retrieved in step:1 matches the next 'h2' tag is the list of 'h2' tags we fetched earlier (which is 'diseasetags')
            #    a. If they are same, then we are sure that there is a 'ul' tag after 'h2' tag = 'Risk Factors'. Which means data is present in bullet points
            #    b. If they are not same, it means data is not present in bullet points, but present as a paragraph under 'p' tag
            # 3. Also, in few cases, the bullet point data are present directly under 'li' tag or under 'strong' tag under 'li' tag (which is in-turn present under 'ul' tag). So we check for this condition as well (if we confirm presence of 'ul' tag under the required 'h2' tag in step:2)
            if (diseasetags[k].getText() == 'Risk factors'):
                risk_real_text = get_data_from_link(diseasetags[k],diseasetags[k+1])
            
            #Next, we check for 'Complications' and then retrieve data under the section. Data is sometimes present as a paragraph under 'p' tag or as bullet points under 'li' tag which is under 'ul' tag
            #both 'p' tag and 'ul' tags are not child tags of parent 'h2' tag. Hence we need to have additional logic to check of type of tag present and ensure they hold the data for the required 'h2' tag
            #Logic used is as follows:
            # 1. Find the next 'ul' tag after the 'h2' = 'Symptoms' and the next 'h2' tag after the retrieved 'ul' tag 
            # 2. Then we check if the 'h2' tag retrieved in step:1 matches the next 'h2' tag is the list of 'h2' tags we fetched earlier (which is 'diseasetags')
            #    a. If they are same, then we are use that there is a 'ul' tag after 'h2' tag = 'Complications'. Which means data is provided in bullet points
            #    b. If they are not same, it means data is not provipresentded in bullet points, but present as a paragraph under 'p' tag
            # 3. Also, in few cases, the bullet point data are present directly under 'li' tag or under 'strong' tag under 'li' tag (which is in-turn present under 'ul' tag). So we check for this condition as well if we confirm presence of 'ul' tag under the required 'h2' tag in step:2
            if (diseasetags[k].getText() == 'Complications'):
                comp_real_text = get_data_from_link(diseasetags[k],diseasetags[k+1])
            
            #Next, we check for 'Prevention' and then retrieve data under the section. Data is sometimes present as a paragraph under 'p' tag or as bullet points under 'li' tag which is under 'ul' tag
            #both 'p' tag and 'ul' tags are not child tags of parent 'h2' tag. Hence we need to have additional logic to check  ype of tag present and ensure they hold the data for the required 'h2' tag
            #Logic used is as follows:
            # 1. Find the next 'ul' tag after the 'h2' = 'Symptoms' and the next 'h2' tag after the retrieved 'ul' tag 
            # 2. Then we check if the 'h2' tag retrieved in step:1 matches the next 'h2' tag is the list of 'h2' tags we fetched earlier (which is 'diseasetags')
            #    a. If they are same, then we are sure that there is a 'ul' tag after 'h2' tag = 'Prevention'. Which means data is present in bullet points
            #    b. If they are not same, it means data is not present in bullet points, but present as a paragraph under 'p' tag
            # 3. Also, in few cases, the bullet point data are present directly under 'li' tag or under 'strong' tag under 'li' tag (which is in-turn present under 'ul' tag). So we check for this condition as well (if we confirm presence of 'ul' tag under the required 'h2' tag in step:2)
            if (diseasetags[k].getText() == 'Prevention'):
                prev_real_text = get_data_from_link(diseasetags[k],diseasetags[k+1])
                
                
        #For 'Diagnosis' and 'Treatment', we need to traverse to another link 'Diagnosis & treatment' within each disease page.
        toc = doc2.find('div',{'class' : 'tableofcontents'}).find('ul').find_all('li')
        for data in toc:
            if data.find('a').getText() == 'Diagnosis & treatment':
                morelinks = base_url+data.find('a').get("href")
        
        response3 = get_data_from_url(morelinks)
        doc3 = BeautifulSoup(response3.text, "html.parser")
        moretags = doc3.find_all('h2')
        for m in range(0,len(moretags)):
            try:
            #We check for 'Disgnosis' and then retrieve data under the section. Data is sometimes present as a paragraph under 'p' tag or as bullet points under 'li' tag which is under 'ul' tag
            #both 'p' tag and 'ul' tags are not child tags of parent 'h2' tag. Hence we need to have additional logic to check type of tag present and ensure they hold the data for the required 'h2' tag
            #Logic used is as follows:
            # 1. Find the next 'ul' tag after the 'h2' = 'Symptoms' and the next 'h2' tag after the retrieved 'ul' tag 
            # 2. Then we check if the 'h2' tag retrieved in step:1 matches the next 'h2' tag is the list of 'h2' tags we fetched earlier (which is 'diseasetags')
            #    a. If they are same, then we are use that there is a 'ul' tag after 'h2' tag = 'Disgnosis'. Which means data is present in bullet points
            #    b. If they are not same, it means data is not present in bullet points, but present as a paragraph under 'p' tag
            # 3. Also, in few cases, the bullet point data are present directly under 'li' tag or under 'strong' tag under 'li' tag (which is in-turn present under 'ul' tag
                if (moretags[m].getText() == 'Diagnosis'):
                    dia_real_text = get_data_from_link(moretags[m],moretags[m+1])
            #Next, check for 'Treatment' and then retrieve data under the section. Data is sometimes present as a paragraph under 'p' tag or as bullet points under 'li' tag which is under 'ul' tag
            #both 'p' tag and 'ul' tags are not child tags of parent 'h2' tag. Hence we need to have additional logic to check type of tag present and ensure they hold the data for the required 'h2' tag
            #Logic used is as follows:
            # 1. Find the next 'ul' tag after the 'h2' = 'Symptoms' and the next 'h2' tag after the retrieved 'ul' tag 
            # 2. Then we check if the 'h2' tag retrieved in step:1 matches the next 'h2' tag is the list of 'h2' tags we fetched earlier (which is 'diseasetags')
            #    a. If they are same, then we are use that there is a 'ul' tag after 'h2' tag = 'Treatment'. Which means data is present in bullet points
            #    b. If they are not same, it means data is not present in bullet points, but present as a paragraph under 'p' tag
            # 3. Also, in few cases, the bullet point data are present directly under 'li' tag or under 'strong' tag under 'li' tag (which is in-turn present under 'ul' tag
                if (moretags[m].getText() == 'Treatment'):
                    treat_real_text = get_data_from_link(moretags[m],moretags[m+1])
            except:
                pass
        
        #Once all the required data are fetched from a disease URL, the data is added as a row into the dataframe
        disease_dataframe = disease_dataframe.append({'Disease_Name':alpha_diseases_links_df['Disease_Name'].iloc[loop],'URL':alpha_diseases_links[loop],'Symptoms':sym_real_text,'Causes':cau_real_text,'Risk_Factor':risk_real_text,'Complications':comp_real_text,'Prevention':prev_real_text,'Diagnosis':dia_real_text,'Treatment':treat_real_text,'Category':maintag},ignore_index=True)
    except:
        pass


# In[10]:


#'Disease_Name' column Split by string, convert it to series and stack them
#After that drop the last index in multi-index series, each disease_name which were comma separated will be in seapare row and have the same index as original dataframe
# name the series and join it with dataframe, as its a one to many match, we get the desired outcome
s =  disease_dataframe['Disease_Name'].str.split(',').apply(pd.Series,1).stack()
s.index = s.index.droplevel(-1)
s.name = 'Disease_Name'
del disease_dataframe['Disease_Name']
disease_dataframe = disease_dataframe.join(s)


# In[11]:


#To extend dataset, we would like to add columns 'Other Names', 'Specialty', ' Deaths' and Frequency'. This we pick from wikipedia by taking each disease fetched and access corresponding wikipedia page for the disease.
# First, we create a new dataset to store the data retrieved from wikipedia
wiki_dataframe = pd.DataFrame(columns=['Disease_Name','Other_Names','Specialty','Deaths','Frequency'])


# In[12]:


#From each disease URL (fetched above), we pick the disease name and construct the corresponsding wikipedia page URL
#Few disease names have '(' and blank spaces whcih need to be removed to construct wikipedia URL. We ensure to do this as well
for loops in range(0,len(alpha_diseases_links)):
    try:
        response4 = requests.get(alpha_diseases_links[loops])
        doc4 = BeautifulSoup(response4.text, "html.parser")
        main = doc4.find('h1').find_next('a').getText()
        wiki_dataframe.at[loops,'Disease_Name']=main
        if '(' in main:
            head, sep, tail = main.partition('(')
            all_heading = head.strip().replace(" ","_")
        else:
            all_heading = main.strip().replace(" ","_")
        wikiurl = wiki_base_url + all_heading
        response5 = requests.get(wikiurl)
        doc5 = BeautifulSoup(response5.text, "html.parser")
        #data of interest is stored under 'th' tag in wikipedia html page.
        #So, we find all 'th' tags and then we check if the tag matches the required section ('Other Names', 'Specialty', 'Deaths', 'Frequency')
        #If present, we pick the text under 'td' tag which is under the 'th' tag 
        #We store the retrieved data in the dataframe
        wikitags = doc5.find_all('th')
        for k in range(0,len(wikitags)):
            try:
                if (wikitags[k].getText() == 'Other names'):
                    othername = wikitags[k].find_next('td').getText()  
                    wiki_dataframe.at[loops,'Other_Names']=othername
                if (wikitags[k].find_next('a').getText() == 'Specialty'):
                    specialty = wikitags[k].find_next('td').find('a').getText()
                    wiki_dataframe.at[loops,'Specialty']=specialty
                if (wikitags[k].getText() == 'Deaths'):
                    deaths = wikitags[k].find_next('td').text
                    wiki_dataframe.at[loops,'Deaths']=deaths
                if (wikitags[k].getText() == 'Frequency'):
                    freq = wikitags[k].find_next('td').text      
                    wiki_dataframe.at[loops,'Frequency']=freq
            except:
                pass
    except:
        pass


# In[13]:


#Merging both the dataframes (one with data retrieved from main website and the other with data from wikipedia) based on disease name
final_disease_dataframe = pd.merge(disease_dataframe, wiki_dataframe,on='Disease_Name',how="left")


# In[14]:


#Printing content of merged dataframe
final_disease_dataframe


# In[15]:


#saving data to xlsx
final_disease_dataframe.to_excel('anjanasfinaldataset.xlsx')


# In[16]:


##########  Source 3 ################


# In[17]:


#setting path to the current directory
os.chdir(os.getcwd())

# Getting Main URL to Extract file
r = requests.get('https://www.ema.europa.eu/en/medicines/download-medicine-data#european-public-assessment-reports-(epar)-section')
 
# Parsing the HTML to get File link
soup = BeautifulSoup(r.content, 'html.parser')
lines = soup.find('span',  class_='file')
link = lines.find('a', class_='ecl-link')
dls = link.get('href')

# Creating Website File Path 
drugs_data = 'https://ema.europa.eu'+dls

# Downloading File 
urllib.request.urlretrieve(drugs_data,"medicine2.xlsx")

#reading the file downloaded
df = pd.read_excel(r'medicine2.xlsx', skiprows=8)
newdf = df[(df.Category == "Human" )]


#Convert disease cell to multiple rows  
#splitting and indexing each value
#merging new indexed columns with existing dataframe
#dropping existing column
#transform numeric columns into separate rows 
#drop new variable column
newdf1 = newdf['Therapeutic area'].str.split(';').apply(pd.Series).merge(newdf, left_index=True, right_index=True).drop(['Therapeutic area'],axis=1).melt(id_vars = ['Category','Medicine name','International non-proprietary name (INN) / common name',
                 'Active substance','Product number','Patient safety','Authorisation status','ATC code','Additional monitoring',
                 'Generic','Biosimilar','Conditional approval','Exceptional circumstances','Accelerated assessment',
                 'Orphan medicine','Marketing authorisation date','Date of refusal of marketing authorisation',
                 'Marketing authorisation holder/company name','Human pharmacotherapeutic group','Vet pharmacotherapeutic group',
                 'Date of opinion','Decision date','Revision number','Condition / indication','Species','ATCvet code',
                 'First published','Revision date','URL'], value_name = "disease name").drop(["variable"],axis=1).dropna(subset=['disease name'])#drop na values from disease name

newdf2 = newdf1['disease name'].str.split(',').apply(pd.Series).merge(newdf1, left_index=True, right_index=True).drop(['disease name'],axis=1).melt(id_vars = ['Category','Medicine name','International non-proprietary name (INN) / common name',
                 'Active substance','Product number','Patient safety','Authorisation status','ATC code','Additional monitoring',
                 'Generic','Biosimilar','Conditional approval','Exceptional circumstances','Accelerated assessment',
                 'Orphan medicine','Marketing authorisation date','Date of refusal of marketing authorisation',
                 'Marketing authorisation holder/company name','Human pharmacotherapeutic group','Vet pharmacotherapeutic group',
                 'Date of opinion','Decision date','Revision number','Condition / indication','Species','ATCvet code',
                 'First published','Revision date','URL'], value_name = "disease name new").drop(["variable"],axis=1).dropna(subset=['disease name new'])#drop na values from disease name

newdf2.rename(columns = {'URL':'Drug_URL'}, inplace = True)


# In[47]:


newdf2 = newdf2.drop(['Category'],axis=1)


# In[20]:


############### Merge with Source 3 (Drug and its details) ##############


# In[81]:


final_merged_ra = pd.merge(final_disease_dataframe,newdf2,left_on="Disease_Name",right_on="disease name new",how="left")


# In[63]:


final_merged_ra


# In[64]:


#Removing [] from dataframe
col_name = ['Complications','Prevention','Diagnosis','Treatment','Risk_Factor','Causes','Symptoms']
for i in col_name:
    final_merged_ra[i] = [','.join(map(str, l)) for l in final_merged_ra[i]]


# In[65]:


#Replacing "" and " " values with nan
#Replacing nan with "NA"
final_merged_ra = final_merged_ra.replace('', np.nan)
final_merged_ra = final_merged_ra.replace(' ', np.nan)
final_merged_ra.fillna('NA', inplace=True)
final_merged_ra


# In[70]:


#drop duplicate rows
final_merged_ra = final_merged_ra.drop_duplicates()


# In[92]:


#cleaning the data
col_name = ['ATC code','Marketing authorisation date','Date of refusal of marketing authorisation','Vet pharmacotherapeutic group','ATCvet code','Species']
final_merged_ra = final_merged_ra.drop(col_name,axis=1)


# In[78]:


final_merged_ra = final_merged_ra.reset_index(drop=True)


# In[90]:


#saving as excel
final_merged_ra.to_excel('diseases.xlsx',index=False)


# In[59]:


#Converting to json format
result = final_merged_ra.to_json('disease.json',orient='records')

