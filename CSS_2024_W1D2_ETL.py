#E - EXTRACT

import pandas as pd #("pd" can be anything you want to call it)

#Accessing a csv file that is in the working directory
file_country = pd.read_csv("country_data_index.csv")

#Accessing a csv file in a folder in the working directory
file_country_diff_fold = pd.read_csv("data_02/iris.csv") #Additional file path argument data_02 "Relative path"


#copying paths

"""
#Absolute path - This is the full path on the pc

C:/Users/Rouss/My Drive/Werk/2024/Masters/CSS/Week 1/2/CSS_day2_ETL/data_02/country_data_index.csv


#Relative path - This is the path in the current working directory

data_02/country_data_index.csv
"""

#Reading file from a URL
df_url = pd.read_csv("https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data")

#You can also open URLs from Github

#There are no names for the columns so we specify them in a list

column_names = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class']

df_url = pd.read_csv("https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data", 
                     header = None, names = column_names)

#You can also store the url as a variable to eliminate clutter in the code eg

url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"

df_url = pd.read_csv(url, header = None, names = column_names)

#Look at pandas dcoumentation for different file formats

"https://pandas.pydata.org/docs/user_guide/io.html"

#Text file with different delimitter
file_geo = pd.read_csv("data_02/Geospatial Data.txt", sep=";")

#Open excel file
file_doc = pd.read_excel("data_02/residentdoctors.xlsx", sheet_name="residentdoctors") #add sheet_name 
                                                        #if there are different sheets in excel folder
#Open json file
file_student = pd.read_json("data_02/student_data.json")


#Github example
#Solution from gitbuh is to go to repository and click "raw" where the csv file is stored
url="https://raw.githubusercontent.com/Asabele240701/css4_day02/main/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
df_covid = pd.read_csv(url)


#T - TRANSFORM


"""
Recap on parameters of read_csv function

https://canvas.instructure.com/courses/7413444/pages/2-extract-transform-load?module_item_id=100843346
"""

#index column
df_country = pd.read_csv("data_02/country_data_index.csv", index_col=0) #The first column is redudant

#another way to remove columns with drop method (look this up)
#df_country.drop([""]) 

#There isnt any consistency in this dataframe
df_doc = pd.read_excel("data_02/residentdoctors.xlsx", sheet_name="residentdoctors")

df_doc.info()

"""
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 161 entries, 0 to 160
Data columns (total 9 columns):
 #   Column               Non-Null Count  Dtype  
---  ------               --------------  -----  
 0   AGE                  161 non-null    int64  
 1   ghqscore_sum         161 non-null    float64
 2   jobsatisfaction_sum  161 non-null    float64
 3   workload_sum         161 non-null    float64
 4   AGEDIST              161 non-null    object 
 5   MARITALSTATUS        161 non-null    object <---- note that although most is int there is also string "other"
 6   CHILDREN             158 non-null    float64
 7   female               161 non-null    int64  
 8   HOURSWORKED          161 non-null    float64
dtypes: float64(5), int64(2), object(2)
memory usage: 11.4+ KB
"""

"""
Regular expressions
"""

df_doc["LOWER_AGE"]  = df_doc["AGEDIST"] #Copying the information from age distribution 
                                            #to a new column LOWER_AGE

df_doc["LOWER_AGE"]  = df_doc["AGEDIST"].str.extract('(\d+)-') #Extracting the lower value in the range with a 
                                                                    #str operation (search for a number that is
                                                                    #positive and ends with a hyphen "-")
                                                                    
df_doc["UPPER_AGE"]  = df_doc["AGEDIST"].str.extract('-(\d+)') #extracting upper range value in age distribution                                                       

df_doc.info()

"""
 #   Column               Non-Null Count  Dtype  
---  ------               --------------  -----  
 0   AGE                  161 non-null    int64  
 1   ghqscore_sum         161 non-null    float64
 2   jobsatisfaction_sum  161 non-null    float64
 3   workload_sum         161 non-null    float64
 4   AGEDIST              161 non-null    object 
 5   MARITALSTATUS        161 non-null    object 
 6   CHILDREN             158 non-null    float64
 7   female               161 non-null    int64  
 8   HOURSWORKED          161 non-null    float64
 9   LOWER_AGE            161 non-null    object  <---- why is this still an object? (Extracted as a string)
dtypes: float64(5), int64(2), object(3)
memory usage: 12.7+ KB
"""

df_doc["LOWER_AGE"]  = df_doc["LOWER_AGE"].astype(int) #Change the data type for lower age and save in lower age
                                                        #column of df

df_doc.info()

"""
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 161 entries, 0 to 160
Data columns (total 10 columns):
 #   Column               Non-Null Count  Dtype  
---  ------               --------------  -----  
 0   AGE                  161 non-null    int64  
 1   ghqscore_sum         161 non-null    float64
 2   jobsatisfaction_sum  161 non-null    float64
 3   workload_sum         161 non-null    float64
 4   AGEDIST              161 non-null    object 
 5   MARITALSTATUS        161 non-null    object 
 6   CHILDREN             158 non-null    float64
 7   female               161 non-null    int64  
 8   HOURSWORKED          161 non-null    float64
 9   LOWER_AGE            161 non-null    int32  <------- FIXED
dtypes: float64(5), int32(1), int64(2), object(2)
memory usage: 12.1+ KB
"""

"""
Working with dates

10-01-2024 - UK

01-10-2024 - US

"""

df_time_series = pd.read_csv("data_02/time_series_data.csv", index_col=0)

print(df_time_series.info())

"""
<class 'pandas.core.frame.DataFrame'>
Index: 366 entries, 0 to 365
Data columns (total 2 columns):
 #   Column       Non-Null Count  Dtype  
---  ------       --------------  -----  
 0   Date         366 non-null    object 
 1   Temperature  366 non-null    float64
dtypes: float64(1), object(1)
memory usage: 8.6+ KB
None
"""

#Convert date column to date-time format
# df_time_series['Date'] = pd.to_datetime(df_time_series['Date'], format = "%d-%m-%Y") format doesn't work???

df_time_series['Date'] = pd.to_datetime(df_time_series['Date'])

print(df_time_series.info())

"""
<class 'pandas.core.frame.DataFrame'>
Index: 366 entries, 0 to 365
Data columns (total 2 columns):
 #   Column       Non-Null Count  Dtype         
---  ------       --------------  -----         
 0   Date         366 non-null    datetime64[ns]
 1   Temperature  366 non-null    float64       
dtypes: datetime64[ns](1), float64(1)
memory usage: 8.6 KB
None
"""

#Generating new columns that extracts year, month and day values
df_time_series['Year'] = df_time_series['Date'].dt.year #dt - date time

df_time_series['Month'] = df_time_series['Date'].dt.month

df_time_series['Day'] = df_time_series['Date'].dt.day


"""
.str
.extract
.astype
"""


#NANs and Wrong Formats

df_patient = pd.read_csv("data_02/patient_data_dates.csv", index_col=0)

print(df_patient.info())

"""
<class 'pandas.core.frame.DataFrame'>
Index: 32 entries, 0 to 31
Data columns (total 5 columns):
 #   Column    Non-Null Count  Dtype  
---  ------    --------------  -----  
 0   Duration  32 non-null     int64  
 1   Date      31 non-null     object <---- want to convert to date format
 2   Pulse     32 non-null     int64  
 3   Maxpulse  32 non-null     int64  
 4   Calories  30 non-null     float64
dtypes: float64(1), int64(3), object(1)
memory usage: 1.5+ KB
None
"""

#My_solution
# new_column = df_patient["Date"]

# print(new_column[0:3])

# for i to (iterate and make new list with first 3 values)

df_patient.drop(index=26, inplace=True)

df_patient['Date'] = pd.to_datetime(df_patient['Date'])

print(df_patient.info())

"""
<class 'pandas.core.frame.DataFrame'>
Index: 31 entries, 0 to 31
Data columns (total 5 columns):
 #   Column    Non-Null Count  Dtype         
---  ------    --------------  -----         
 0   Duration  31 non-null     int64         
 1   Date      30 non-null     datetime64[ns] <----- fixed
 2   Pulse     31 non-null     int64         
 3   Maxpulse  31 non-null     int64         
 4   Calories  29 non-null     float64       
dtypes: datetime64[ns](1), float64(1), int64(3)
memory usage: 1.5 KB
None
"""

avg_cal = df_patient["Calories"].mean()

#Example of filling NaN values
#df_patient["Calories"].fillna(avg_cal, inplace = True) #Here we fill NaN with the average found in valid values
                                                            # of the calories column

"""
Best practices
"""

#getting rid of NaN
df_patient.dropna(inplace = True) #entire row is removed with the index

#Fixing the indices that are now missing 
df_patient = df_patient.reset_index(drop=True)


# df_patient.loc[7, 'Duration'] = 45

#There is an outlier in the duration column of the patient data "450" 
df_patient['Duration'] = df_patient['Duration'].replace([450], 50)

print(df_patient)

#if you want to display all rows (not good if you have too many)
#pd.set_option('display.max_rows', None)

#APPLYING DATA TRANSFORMATIONS


df_iris = pd.read_csv("data_02/iris.csv")

col_names = df_iris.columns.tolist() #generate new list using column names in dataframe

print(df_iris.columns)

print(col_names)

df_iris["sepal_length_sq"] = df_iris["sepal_length"]**2

#df_iris["sepal_length_sq_2"] = df_iris["sepal_length"].apply(lamda x: x**2) This is extra and will be included
                                                                    #in notes later

grouped = df_iris.groupby("class")

msv = grouped['sepal_length_sq'].mean() #msv - mean squared values

print(msv) 

#Taking out redundant info in the class column
df_iris["class"] = df_iris["class"].str.replace("Iris-", "")

##########################
#Concatination of 2 dataframes

df1 = pd.read_csv("data_02/person_split1.csv")
df2 = pd.read_csv("data_02/person_split2.csv")

df_con = pd.concat([df1, df2], ignore_index=True) #add additional info via rows


##########################

df1 = pd.read_csv("data_02/person_education.csv")
df2 = pd.read_csv("data_02/person_work.csv")

#df merge inner join
df_merge_inner = pd.merge(df1, df2, on = "id") #add additional info via columns

#df outer join
df_merge_outer = pd.merge(df1, df2, on = "id", how = 'outer')

"""
outer: use union of keys from both frames, similar to a SQL full outer join; 
sort keys lexicographically. 

inner: use intersection of keys from both frames, similar to a SQL inner join; 
preserve the order of the left keys.

"""

#FILTERING

# Filtering data from the first day in dataframes
# print(df[df['age'] > 30])

# Filter data for females (class == 'versicolor') (Now we use classes) (we took out the redundant information "Iris")
iris_versicolor = df_iris[df_iris['class'] == 'versicolor']

# Calculate the average iris_versicolor_sep_length
avg_iris_versicolor_sep_length = iris_versicolor['sepal_length'].mean()

#Only rows where Sepal length is above 5
df_iris = df_iris[df_iris['sepal_length'] > 5]

#only rows that contain class virginica
# df_iris = df_iris[df_iris["class"] == "virginica"] (filtering two different classes leads
                                                    #to empty df)


#LOAD

#Here we load the iris data frame that has been filtered/cleaned 
#as a csv in the working directory

df_iris.to_csv("pulsar.csv")

#To a specific folder in directory
df_iris.to_csv("Output/pulsar.csv")

#######################################################################################

#Notes review

















