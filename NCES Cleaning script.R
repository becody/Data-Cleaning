################ NCES Cleaning and Updating #######################
# Date: 9/1/2020
# Goal: import, clean, reformat, and export updated NCES information for schools 
# Author: Brittany Cody
# Contact: brittany.cody@learnplatform.com

# # Work on NCES datasets:
install.packages("arsenal")
install.packages("tidyr")
install.packages("readr")
install.packages("Rtools")
install.packages("data.table")
install.packages("parallel")
install.packages("naniar")
install.packages("anchors")
install.packages("svMisc")
install.packages("rowr")
devtools::install_github('UrbanInstitute/education-data-package-r')
library(dplyr)
library(tidyverse)
library(arsenal)
library(stringr)
library(readr)
library(tidyr)
library(educationdata)
library(parallel)
library(plyr)
library(forcats)
library(naniar)
library(data.table)
library(svMisc)
library(rowr)

# Check encoding pre-reading in data 
Encoding()

# Before reading in, I recommend changing the names of the demographic columns of each dataset
# to be the same, either manually or in code. This makes the merging process a lot smoother as well
# as the cleaning process for each of the demographic variables. 

# Merge public and private schools
  # Read in datasets 
private <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\Public Equity Dashboard\\private.csv",
                      colClasses = c("character"),
                      na.strings = c("","NA"))
public <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\Public Equity Dashboard\\ALL SCHOOLS 18_19.csv",
                            colClasses = c("character"),
                            na.strings = c("","NA"))

  # Delete rows from both that have almost all NAs 
public <- public[which(rowMeans(!is.na(public)) > 0.98), ]
private <- private[which(rowMeans(!is.na(private)) > 0.98), ]

  # Merge the two datasets: public and private, then check to make sure the merge was successful 
updated_schools = bind_rows(public, private)
glimpse(updated_schools)

  # Check duplicates
dupes <- updated_schools[duplicated(updated_schools$School.ID...NCES.Assigned..Public.School..Latest.available.year),]

#################################### Column formatting and naming ###############################
# Change the name of the df to work
work <- updated_schools

# Get rid of periods in column names and replace with _ 
colnames(work) <- gsub(pattern = "[.]",
                    replacement = "_",
                    x = colnames(work))

# Check unique values in nces_id and id
unique_nces <- unique(work$nces_id)
length(unique_nces)

unique_id <- unique(work$id)
length(unique_id)

# # Replace NA's with 0s in dataframe 
work[is.na(work)] <- 0

# # Check frequency of values in male_students and female_students
# work$Male_Students <- as.factor(work$Male_Students)
# work$Female_Students <- as.factor(work$Female_Students)
# 
# count <- count(work, 'Male_Students') # 2140 have 0 for Males
# count <- count(work, 'Female_Students') # 2497 have 0 for Females

# add male and female in front of the values in the columns 
work$Male_Students <- sub("^", "male:", work$Male_Students__Public_School__2018_19)
work$Male_Students

work$Female_Students <- paste("female", work$Female_Students__Public_School__2018_19, sep = ":")
work$Female_Students

# then add values of male and female columns together
work$Students_by_gender <- paste(work$Male_Students, work$Female_Students, sep = ",")
work$Students_by_gender

# # Ensure that the counts of M and F are correct
count <- count(work, 'Students_by_gender') #1949 have 0 for both M and F

### Race/Ethnicity Variables ###

# Repeat this type of work with students by race variable
# American Indian Alaskan
work$American_Indian_Alaska_Native_female <- sub("^", "female:", work$American_Indian_Alaska_Native___female__Public_School__2018_19)
work$American_Indian_Alaska_Native_male <-  sub("^", "male:", work$American_Indian_Alaska_Native___male__Public_School__2018_19)
work$American_Indian_Alaska_Native_all <- paste(work$American_Indian_Alaska_Native_male, work$American_Indian_Alaska_Native_female, sep = ",")
work$American_Indian_Alaska_Native_all <- paste0("{", work$American_Indian_Alaska_Native_all, "}")
work$American_Indian_Alaska_Native_all <- sub("^", "American Indian Alaskan:", work$American_Indian_Alaska_Native_all)
work$American_Indian_Alaska_Native_all

# Asian 
work$Asian_or_Asian_Pacific_Islander_female <- sub("^", "female:", work$Asian_or_Asian_Pacific_Islander___female__Public_School__2018_19)
work$Asian_or_Asian_Pacific_Islander_male <-  sub("^", "male:", work$Asian_or_Asian_Pacific_Islander___male__Public_School__2018_19)
work$Asian_or_Asian_Pacific_Islander_all <- paste(work$Asian_or_Asian_Pacific_Islander_male, work$Asian_or_Asian_Pacific_Islander_female, sep = ",")
work$Asian_or_Asian_Pacific_Islander_all <- paste0("{", work$Asian_or_Asian_Pacific_Islander_all, "}")
work$Asian_or_Asian_Pacific_Islander_all <- sub("^", "Asian:", work$Asian_or_Asian_Pacific_Islander_all)
work$Asian_or_Asian_Pacific_Islander_all

# Hispanic
work$Hispanic_female <- sub("^", "female:", work$Hispanic___female__Public_School__2018_19)
work$Hispanic_male <-  sub("^", "male:", work$Hispanic___male__Public_School__2018_19)
work$Hispanic_all <- paste(work$Hispanic_male, work$Hispanic_female, sep = ",")
work$Hispanic_all <- paste0("{", work$Hispanic_all, "}")
work$Hispanic_all <- sub("^", "Hispanic:", work$Hispanic_all)
work$Hispanic_all

# Black
work$Black_female <- sub("^", "female:", work$Black___female__Public_School__2018_19)
work$Black_male <-  sub("^", "male:", work$Black___male__Public_School__2018_19)
work$Black_all<- paste(work$Black_male, work$Black_female, sep = ",")
work$Black_all<- paste0("{", work$Black_all, "}")
work$Black_all<- sub("^", "Black:", work$Black_all)
work$Black_all

# White
work$White_female <- sub("^", "female:", work$White___female__Public_School__2018_19)
work$White_male <-  sub("^", "male:", work$White___male__Public_School__2018_19)
work$White_all <- paste(work$White_male, work$White_female, sep = ",")
work$White_all <- paste0("{", work$White_all, "}")
work$White_all <- sub("^", "White:", work$White_all)
work$White_all

# Two Race
work$Two_or_More_Races_Female <- sub("^", "female:", work$Two_or_More_Races___female__Public_School__2018_19)
work$Two_or_More_Races_male <-  sub("^", "male:", work$Two_or_More_Races___male__Public_School__2018_19)
work$Two_or_More_Races_all <- paste(work$Two_or_More_Races_male, work$Two_or_More_Races_Female, sep = ",")
work$Two_or_More_Races_all <- paste0("{", work$Two_or_More_Races_all, "}")
work$Two_or_More_Races_all <- sub("^", "2race:", work$Two_or_More_Races_all)
work$Two_or_More_Races_all


# Pacific
work$Hawaiian_Nat_Pacific_Isl_female <- sub("^", "female:", work$Hawaiian_Nat__Pacific_Isl____female__Public_School__2018_19)
work$Hawaiian_Nat_Pacific_Isl_male <-  sub("^", "male:", work$Hawaiian_Nat__Pacific_Isl____male__Public_School__2018_19)
work$Hawaiian_Nat_Pacific_Isl_all <- paste(work$Hawaiian_Nat_Pacific_Isl_male, work$Hawaiian_Nat_Pacific_Isl_female, sep = ",")
work$Hawaiian_Nat_Pacific_Isl_all <- paste0("{", work$Hawaiian_Nat_Pacific_Isl_all, "}")
work$Hawaiian_Nat_Pacific_Isl_all <- sub("^", "Pacific:", work$Hawaiian_Nat_Pacific_Isl_all)
work$Hawaiian_Nat_Pacific_Isl_all 

# Paste columns together in correct order 
cols <- c("American_Indian_Alaska_Native_all","Asian_or_Asian_Pacific_Islander_all",
          "Black_all","Hispanic_all","White_all","Hawaiian_Nat_Pacific_Isl_all",
          "Two_or_More_Races_all")

work$students_by_gender_and_race <- apply( work[, cols], 1, paste, collapse=",")
work <- work[, !(names(work) %in% cols)]

# Ensure that the counts of gender and race are correct
count <- count(work, 'students_by_gender_and_race') #4167 have 0 for gender and race all

# Final variable 
work$students_by_gender_and_race

################# Students by grade level #######
work$Pre_K <- sub("^", "Prek:", work$Prekindergarten_Students__Public_School__2018_19)
work$K <- sub("^", "K:", work$Kindergarten_Students__Public_School__2018_19)
work$One <- sub("^", "1:", work$Grade_1_Students__Public_School__2018_19)
work$Two <- sub("^", "2:", work$Grade_2_Students__Public_School__2018_19)
work$Three <- sub("^", "3:", work$Grade_3_Students__Public_School__2018_19)
work$Four <- sub("^", "4:", work$Grade_4_Students__Public_School__2018_19)
work$Five <- sub("^", "5:", work$Grade_5_Students__Public_School__2018_19)
work$Six <- sub("^", "6:", work$Grade_6_Students__Public_School__2018_19)
work$Seven <- sub("^", "7:", work$Grade_7_Students__Public_School__2018_19)
work$Eight <- sub("^", "8:", work$Grade_8_Students__Public_School__2018_19)
work$Nine <- sub("^", "9:", work$Grade_9_Students__Public_School__2018_19)
work$Ten <- sub("^", "10:", work$Grade_10_Students__Public_School__2018_19)
work$Eleven <- sub("^", "11:", work$Grade_11_Students__Public_School__2018_19)
work$Twelve <- sub("^", "12:", work$Grade_12_Students__Public_School__2018_19)

# Paste columns together in correct order 
cols <- c("Pre_K", "K","One","Two","Three","Four","Five","Six","Seven",
          "Eight","Nine","Ten","Eleven","Twelve")

work$students_by_grade_level <- apply( work[, cols], 1, paste, collapse=",")
work <- work[, !(names(work) %in% cols)]
work$students_by_grade_level <- paste0("{", work$students_by_grade_level, "}")
work$students_by_grade_level

work$students_by_grade_level <- na_if(work$students_by_grade_level,"{Prek:0,K:0,1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0}")

# # Ensure that the counts of grade level are correct
count <- count(work, 'students_by_grade_level') #4609 have 0's for all grades and now NAs

# Final variable 
work$students_by_grade_level

########################## Students by race ##############
work$AIAS <- sub("^", "American Indian Alaskan:", work$American_Indian_Alaska_Native_Students__Public_School__2018_19)
work$AsianS<- sub("^", "Asian:", work$Asian_or_Asian_Pacific_Islander_Students__Public_School__2018_19)
work$BlackS <- sub("^", "Black:", work$Black_Students__Public_School__2018_19)
work$HispanicS <- sub("^", "Hispanic:", work$Hispanic_Students__Public_School__2018_19)
work$WhiteS <- sub("^", "White:", work$White_Students__Public_School__2018_19)
work$PacS <- sub("^", "Pacific:", work$Hawaiian_Nat__Pacific_Isl__Students__Public_School__2018_19)
work$TwoS <- sub("^", "2race:", work$Two_or_More_Races_Students__Public_School__2018_19)

# Paste columns together in correct order 
cols <- c("AIAS", "AsianS","BlackS","HispanicS","WhiteS","PacS","TwoS")

work$students_by_race <- apply( work[, cols], 1, paste, collapse=",")
work <- data[, !(names(work) %in% cols)]
work$students_by_race <- paste0("{", work$students_by_race, "}")

work$students_by_race <- na_if(work$students_by_race,"{American Indian Alaskan:0,Asian:0,Black:0,Hispanic:0,White:0,Pacific:0,2race:0}")                                      

# # Ensure that the counts of race are correct
count <- count(work, 'students_by_race') #5076 have NA's

# Final variable 
work$students_by_race

####################### Filter dataset and change column names ###########################
# Change values that are just 0 or "" to NA in order to combine columns
work[work == ""] = NA
work[work == "NA"] = NA
work[work == 0] = NA

# Check names of the work dataset before combining
names(work)

# Check unique values to see that we have mostly or all unique values for nces_id and id
unique_nces <- unique(work$nces_id)
length(unique_nces)
work$nces_id <- work$School_ID___NCES_Assigned__Public_School__Latest_available_year

unique_id <- unique(work$id)
length(unique_id)

# Public and private columns were combined earlier with rbind (renamed private school same columns with names from public to ensure binding of same columns from
# both datasets)
# Select the columns that we will need for final cleaning steps
work2 <- work %>%
# Drop columns 
  dplyr::select(nces_id,
                # county_id,
                ï__School_Name,                                                        
                Location_Address_1__Public_School__2018_19,                            
                Location_City__Public_School__2018_19,                                 
                Location_State_Abbr__Public_School__2018_19,                           
                Location_ZIP__Public_School__2018_19,                                  
                Mailing_Address_1__Public_School__2018_19,                             
                Mailing_City__Public_School__2018_19,                                  
                Mailing_ZIP__Public_School__2018_19,
                Mailing_State_Abbr__Public_School__2018_19,
                Phone_Number__Public_School__2018_19,                                  
                School_Type__Public_School__2018_19,                                   
                Charter_School__Public_School__2018_19,                                
                Magnet_School__Public_School__2018_19,                                 
                Shared_Time_School__Public_School__2018_19,                            
                Urban_centric_Locale__Public_School__2018_19,                          
                Title_I_School_Status__Public_School__2018_19,                         
                Title_I_Eligible_School__Public_School__2018_19,                       
                Latitude__Public_School__2018_19,                                      
                Longitude__Public_School__2018_19,                                     
                State_School_ID__Public_School__2018_19,                               
                Virtual_School_Status__SY_2018_19_onward___Public_School__2018_19,     
                Total_Students_All_Grades__Includes_AE___Public_School__2018_19,       
                Free_Lunch_Eligible__Public_School__2018_19,                           
                Reduced_price_Lunch_Eligible_Students__Public_School__2018_19,         
                Full_Time_Equivalent__FTE__Teachers__Public_School__2018_19,           
                Pupil_Teacher_Ratio__Public_School__2018_19,                           
                Year_of_Update,                                                        
                Public_Private,                                                        
                Library_or_Library_Media_Center__Private_School__2017_18,              
                Coeducational__Private_School__2017_18,                                
                School_s_Religious_Affiliation_or_Orientation__Private_School__2017_18,
                School_Community_Type__Private_School__2017_18,                        
                Students_by_gender,                                                    
                students_by_gender_and_race,                                           
                students_by_grade_level,                                               
                students_by_race) 
# Rename columns 

namekey = c(ï__School_Name = "name",                                                        
            Location_Address_1__Public_School__2018_19 = "physical_address_line_1",                            
            Location_City__Public_School__2018_19 = "physical_city",                                 
            Location_State_Abbr__Public_School__2018_19 = "state",                           
            Location_ZIP__Public_School__2018_19 = "physical_postal_code",                                  
            Mailing_Address_1__Public_School__2018_19 = "mailing_address_line_1",                             
            Mailing_City__Public_School__2018_19 = "mailing_city",                                  
            Mailing_ZIP__Public_School__2018_19 = "mailing_postal_code",
            Mailing_State_Abbr__Public_School__2018_19 = "mailing_state_id",
            Phone_Number__Public_School__2018_19 = "phone_number",                                  
            School_Type__Public_School__2018_19 = "school_type",                                   
            Charter_School__Public_School__2018_19 = "is_charter_school",                                
            Magnet_School__Public_School__2018_19 = "is_magnet_school",                                 
            Shared_Time_School__Public_School__2018_19 = "is_shared_time_school",                            
            Urban_centric_Locale__Public_School__2018_19 = "urban_centric_locale",                          
            Title_I_School_Status__Public_School__2018_19 = "is_title_1_school_wide",                         
            Title_I_Eligible_School__Public_School__2018_19 = "is_title_1_school",                       
            Latitude__Public_School__2018_19 = "physical_latitude",                                      
            Longitude__Public_School__2018_19 = "physical_longitude",                                     
            State_School_ID__Public_School__2018_19 = "state_school_id",                               
            Virtual_School_Status__SY_2018_19_onward___Public_School__2018_19 = "virtual_school_status",     
            Total_Students_All_Grades__Includes_AE___Public_School__2018_19 = "total_students",       
            Free_Lunch_Eligible__Public_School__2018_19 = "students_eligible_for_free_lunch",                           
            Reduced_price_Lunch_Eligible_Students__Public_School__2018_19 = "students_eligible_for_reduced_lunch",         
            Full_Time_Equivalent__FTE__Teachers__Public_School__2018_19 = "full_time_equivalent_teachers",           
            Pupil_Teacher_Ratio__Public_School__2018_19 = "pupil_teacher_ratio",                           
            Year_of_Update = "year_of_update",                                                        
            Public_Private = "public_private",                                                        
            Library_or_Library_Media_Center__Private_School__2017_18 = "has_library",              
            Coeducational__Private_School__2017_18 = "is_coed_school",                                
            School_s_Religious_Affiliation_or_Orientation__Private_School__2017_18 = "religious_affiliation",
            School_Community_Type__Private_School__2017_18 = "school_community_type",                        
            Students_by_gender = "students_by_gender") 

work2 <- plyr::rename(work2, replace = namekey, warn_missing = T)
names(work2)

# Check unique values of nces_id 
dupes <- unique(work2[duplicated(work2$nces_id),]) #0 duplicates

################### Finishing up the dataset ######################
# String matching id's from LP_schools with the nces dataset 
  # Read in the LP_schools dataset 
LP_schools <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\Public Equity Dashboard\\LP name and id.csv",
                 colClasses = c("character"),
                 na.strings = c("","NA"),
                 stringsAsFactors = FALSE)
LP_phone <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\Public Equity Dashboard\\id_and_phone_number.csv",
                    colClasses = c("character"),
                    na.strings = c("","NA"),
                    stringsAsFactors = FALSE)
# # nces_id and zipcode and id
LP_nces <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\Public Equity Dashboard\\LP_nces_zip.csv",
                    colClasses = c("character"),
                    na.strings = c("","NA"),
                    stringsAsFactors = FALSE)

# # nces_id and id and county_id 
LP_county_id_nces <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\Public Equity Dashboard\\LP_county_id_NCES.csv",
                    colClasses = c("character"),
                    na.strings = c("","NA"),
                    stringsAsFactors = FALSE)
# # id and address
LP_address <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\Public Equity Dashboard\\LP_address.csv",
                              colClasses = c("character"),
                              na.strings = c("","NA"),
                              stringsAsFactors = FALSE)

###########################
# Check from scratch the nces_id matches 
#nces
nces <- work2$nces_id

#nces with zeros removed
work2$nces_nozero <- str_remove(work2$nces_id, "^0+")
nces_nozero <- str_remove(work2$nces_id, "^0+")

# matches
# without zeros
length(intersect(nces, LP_nces$nces_id)) #83203
length(match(nces, LP_nces$nces_id)) # 122054
length(intersect(nces_nozero, LP_nces$nces_id)) #99976
length(match(nces_nozero, LP_nces$nces_id)) # 122054

length(intersect(nces, nces_nozero)) #103121
103121-99976 # About 3145 are new from nces without leading zeros 

########################### Matching for id ############################

##### Match on nces_id #########
# Making sure each name column is formatted for merging in both LP_schools and work2 datasets 
LP_schools$name <- as.character(LP_schools$ï..name)
LP_schools$LP <- as.character(LP_schools$ï..name)
LP_schools$LP <- tolower(LP_schools$LP)
LP_schools$LP <- str_replace_all(LP_schools$LP, fixed(" "), "")
LP_schools$LP <- str_replace_all(LP_schools$LP, "[^[:alnum:]]", " ")

work2$ids <- as.character(work2$name)
work2$ids <- tolower(work2$ids)
work2$ids <- str_replace_all(work2$ids, fixed(" "), "")
work2$ids <- str_replace_all(work2$ids, "[^[:alnum:]]", " ")

# Merge all of the LP related ids, county_ids, and physical postal codes into one dataset: new
new <- merge(LP_schools, LP_nces, by.x = c("id"), by.y = c("ï..id"))
new <- merge(new, LP_county_id_nces, by= c("id"))
new <- merge(new, LP_address, by.x= c("id"), by.y = c("ï..id"))

# Remove NA nces_id's in work2 before merging 
work2 <- work2[!is.na(work2$nces_id),]

# New method 
df <- merge(work2, new, by.x = "nces_nozero",by.y = "nces_id", all.x = TRUE)

# df <- merge(work2, new, by.x = c("nces_nozero", "ids","physical_city")
#             ,by.y = c("nces_id","LP","physical_city"), all.x = TRUE)

names(df)
df <- df %>%
  select(-c(ï..name,Count.x, Count.y, name.y,
             physical_postal_code.y,ï..nces_id, physical_city.y, nces_nozero))
# names(df)
# df2 <- merge(df, new, by.x = "ids",by.y = "LP", all.x = TRUE)

# Check the amount of unique nces_id's and ids that are in df after merging 
length(unique(df$nces_id)) #122049
length(unique(df$id)) #100008

# Remove extra columns and duplicate nces_id's and ids from the merged dataset
df2 <- df %>%
  distinct(nces_id, .keep_all = TRUE) %>%
  distinct(id, .keep_all = TRUE) 

# Check how many id's remain after removing duplicates
length(which(!is.na(df2$id))) # 99976

# Re-order the columns of the dataset to match LP schools (for reference)
# col_order <- c("county_id", "full_time_equivalent_teachers","has_library",
#                "id", "is_charter_school","is_coed_school","is_magnet_school",
#                "public_private","is_shared_time_school","is_title_1_school",
#                "is_title_1_school_wide","mailing_address_line_1","mailing_city",
#                "mailing_postal_code","mailing_state_id","name","nces_id",
#                "phone_number","physical_address_line_1","physical_city",
#                "physical_latitude","physical_longtiude","physical_postal_code",
#                "state","religious_affiliation","school_type","state_school_id",
#                "students_by_gender","students_by_gender_and_race",
#                "students_by_grade_level","students_by_race",
#                "students_eligible_for_free_lunch","students_eligible_for_reduced_lunch",
#                "total_students","year_of_update","virtual_school_status",
#                "urban_centric_locale","pupil_teacher_ratio","school_community_type")
names(df2)
df2 <- df2[, c(41,26,30,39,13,31,14,29,15,18,17,
               7,8,9,10,2,1,11,3,4,19,20,6,5,
               32,12,21,34,35,36,37,24,25,23,
               28,22,16,27,33)]
names(df2)

# Old Method 
# work2 <- work2[, c(3, 27, 31, 39, 14, 32, 15, 30,
#                    16, 19, 18, 8, 9, 10, 11, 3, 1, 12,
#                    4, 5, 20, 21, 2, 7, 6, 33, 13, 22, 35, 
#                    36, 37, 38, 25, 26, 24, 29, 23, 
#                    17, 28, 34)]
# names(work2)
  
# Change title 1 variable and combine factors to be boolean
df2$is_title_1_school <- as.factor(df2$is_title_1_school)
levels(df2$is_title_1_school)

  # Check levels to ensure they're correct
levels <- levels(df2$is_title_1_school)
levels[length(levels) + 1] <- "2-No"
df2$is_title_1_school <- factor(df2$is_title_1_school, levels = levels)
df2$is_title_1_school[is.na(df2$is_title_1_school)] <- "2-No"

levels(df2$is_title_1_school)

# Remove duplicates in nces_id and id
df2 <- df2[!is.na(df2$id),]

work2 <- work2 %>%
  distinct(nces_id, .keep_all = TRUE) %>%
  distinct(id, .keep_all = TRUE)

# Change state identifiers to match previous NCES update
df2$state <- as.factor(df2$state)
levels(df2$state)
table(df2$state)

levels(df2$state) <- list("2"=c("AL "),
                                         "3"=c("AK "),
                                         "5"=c("AR "),
                                         "6"=c("CA "),
                                         "7"=c("CO "),
                                         "8"=c("CT "),
                                         "9"=c("DE "),
                                         "10"=c("FL "),
                                         "11"=c("GA "),
                                         "12"=c("HI "),
                                         "13"=c("ID "),
                                         "14"=c("IL "),
                                         "15"=c("IN "),
                                         "16"=c("IA "),
                                         "17"=c("KS "),
                                         "18"=c("KY "),
                                         "19"=c("LA "),
                                         "20"=c("ME "),
                                         "21"=c("MD "),
                                         "22"=c("MA "),
                                         "23"=c("MI "),
                                         "24"=c("MN "),
                                         "25"=c("MS "),
                                         "26"=c("MO "),
                                         "27"=c("MT "),
                                         "28"=c("NE "),
                                         "29"=c("NV "),
                                         "30"=c("NH "),
                                         "31"=c("NJ "),
                                         "32"=c("NM "),
                                         "33"=c("NY "),
                                         "34"=c("NC "),
                                         "35"=c("ND "),
                                         "36"=c("OH "),
                                         "37"=c("OK "),
                                         "38"=c("OR "),
                                         "39"=c("PA "),
                                         "40"=c("RI "),
                                         "41"=c("SC "),
                                         "42"=c("SD "),
                                         "43"=c("TN "),
                                         "44"=c("TX "),
                                         "45"=c("UT "),
                                         "46"=c("VT "),
                                         "47"=c("VA "),
                                         "48"=c("WA "),
                                         "49"=c("WV "),
                                         "50"=c("WI "),
                                         "51"=c("WY "),
                                         "52"=c("DC "))

levels(df2$state)
table(df2$state)

# Same with mailing state id
df2$mailing_state_id <- as.factor(df2$mailing_state_id)
levels(df2$mailing_state_id)


levels(df2$mailing_state_id) <- list("2"=c("AL "),
                                                 "3"=c("AK "),
                                                 "5"=c("AR "),
                                                 "6"=c("CA "),
                                                 "7"=c("CO "),
                                                 "8"=c("CT "),
                                                 "9"=c("DE "),
                                                 "10"=c("FL "),
                                                 "11"=c("GA "),
                                                 "12"=c("HI "),
                                                 "13"=c("ID "),
                                                 "14"=c("IL "),
                                                 "15"=c("IN "),
                                                 "16"=c("IA "),
                                                 "17"=c("KS "),
                                                 "18"=c("KY "),
                                                 "19"=c("LA "),
                                                 "20"=c("ME "),
                                                 "21"=c("MD "),
                                                 "22"=c("MA "),
                                                 "23"=c("MI "),
                                                 "24"=c("MN "),
                                                 "25"=c("MS "),
                                                 "26"=c("MO "),
                                                 "27"=c("MT "),
                                                 "28"=c("NE "),
                                                 "29"=c("NV "),
                                                 "30"=c("NH "),
                                                 "31"=c("NJ "),
                                                 "32"=c("NM "),
                                                 "33"=c("NY "),
                                                 "34"=c("NC "),
                                                 "35"=c("ND "),
                                                 "36"=c("OH "),
                                                 "37"=c("OK "),
                                                 "38"=c("OR "),
                                                 "39"=c("PA "),
                                                 "40"=c("RI "),
                                                 "41"=c("SC "),
                                                 "42"=c("SD "),
                                                 "43"=c("TN "),
                                                 "44"=c("TX "),
                                                 "45"=c("UT "),
                                                 "46"=c("VT "),
                                                 "47"=c("VA "),
                                                 "48"=c("WA "),
                                                 "49"=c("WV "),
                                                 "50"=c("WI "),
                                                 "51"=c("WY "),
                                                 "52"=c("DC "))

levels(df2$mailing_state_id) 
table(df2$mailing_state_id)

# Look again at duplicates after removals
length(unique(df2[duplicated(df2$name),])) #33054
length(unique(df2[duplicated(df2$nces_id),])) #0
length(unique(df2[duplicated(df2$id),])) #0

# Count NA's across columns 
sapply(df2, function(x) sum(is.na(x))) 
Hmisc::describe(df2)
# # Verifying that the information looks correct in the merged and cleaned dataset
  # Looking specifically at the distribution of demographic information
  # Ex: students by gender, students by gender/race, students by grade level, students by race

  # Separate into dataframes to individually verify demographic info that is 0,0 or NA for all
look <- df2 %>%
  filter(is.na(students_by_race))
look2 <- df2 %>%
  filter(is.na(students_by_grade_level))
look3 <- df2 %>%
  filter(is.na(students_by_gender))
look4 <- df2 %>%
  filter(is.na(students_by_gender_and_race))
look5 <- df2 %>%
  filter(students_by_gender == "male:0,female:0")

# View the patterns and updated columns
Hmisc::describe(look)
Hmisc::describe(look2)
Hmisc::describe(look3)
Hmisc::describe(look4)
Hmisc::describe(look5)

# Count number of row differences after removing all duplicates
nrow(updated_schools) - nrow(df2) #22073
  
# Remove additional characters in the dataset and replace with 0
  # This step is to help with pre-processing and allowing our dataset to have no special characters or encodings
char <- df2
specChars <- "â|€|¡|‡"
  
# Removing in specific columns 
char2 <- char %>%
  mutate(full_time_equivalent_teachers = str_replace_all(full_time_equivalent_teachers, specChars, "0"),
         students_by_gender = str_replace_all(students_by_gender, specChars, "0"),
         students_by_gender_and_race = str_replace_all(students_by_gender_and_race, specChars, "0"),
         students_by_grade_level = str_replace_all(students_by_grade_level, specChars, "0"),
         students_by_race = str_replace_all(students_by_race, specChars, "0"),
         students_eligible_for_free_lunch = str_replace_all(students_eligible_for_free_lunch, specChars, "0"),
         students_eligible_for_reduced_lunch = str_replace_all(students_eligible_for_reduced_lunch, specChars, "0"),
         total_students = str_replace_all(total_students, specChars, "0"))
char3 <- char2
char3[char3=="000"] <- "0"

# Rename columns for the final export of our cleaned dataset
names(char3)
char3 <- char3 %>%
  dplyr::rename(name = name.x,
                physical_address_line_1 = physical_address_line_1.x,
                physical_city = physical_city.x,
                physical_postal_code = physical_postal_code.x)
names(char3)

# # Check differences between current schools dataset and proposed dataset (char3)
# schools <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\schools.csv",
#                        colClasses = c("character"),
#                        na.strings = c("","NA"),
#                        stringsAsFactors = FALSE)
# names(schools)
# Hmisc::describe(schools)
# look6 <- schools %>%
#   filter(students_by_gender == "male:0,female:0")
# 
# schools <- schools %>%
#   dplyr::rename(county_id = ï..county_id,
#                 state = physical_state_id, 
#                 religious_affiliation = religous_affiliation,
#                 year_of_update = updated_at,
#                 public_private = is_public_school,
#                 students_eligible_for_free_lunch = students_eligibile_for_free_lunch,
#                 students_eligible_for_reduced_lunch = students_eligibile_for_reduced_price_lunch,
#                 virtual_school_status = is_home_school) %>%
#   select(-c(school_district_id,is_eligible_for_swp, is_eligible_for_tas))
# 
# updated_schools <- char3 %>% 
#   select(-c(urban_centric_locale, pupil_teacher_ratio, school_community_type))
# str(updated_schools)
# 
# # Assign states as numbers so that way we can truly compare any updates
# states <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\states.csv",
#                     colClasses = c("character"),
#                     na.strings = c("","NA"),
#                     stringsAsFactors = FALSE)
# names(states)
# states <- states %>%
#   dplyr::rename(state_id = id)
# 
# updated_schools$state_id <- as.factor(updated_schools$state)
# levels(updated_schools$state_id)
# table(updated_schools$state_id)
# 
# levels(updated_schools$state_id) <- list("2"=c("AL "),
#                                          "3"=c("AK "),
#                                          "5"=c("AR "),
#                                          "6"=c("CA "),
#                                          "7"=c("CO "),
#                                          "8"=c("CT "),
#                                          "9"=c("DE "),
#                                          "10"=c("FL "),
#                                          "11"=c("GA "),
#                                          "12"=c("HI "),
#                                          "13"=c("ID "),
#                                          "14"=c("IL "),
#                                          "15"=c("IN "),
#                                          "16"=c("IA "),
#                                          "17"=c("KS "),
#                                          "18"=c("KY "),
#                                          "19"=c("LA "),
#                                          "20"=c("ME "),
#                                          "21"=c("MD "),
#                                          "22"=c("MA "),
#                                          "23"=c("MI "),
#                                          "24"=c("MN "),
#                                          "25"=c("MS "),
#                                          "26"=c("MO "),
#                                          "27"=c("MT "),
#                                          "28"=c("NE "),
#                                          "29"=c("NV "),
#                                          "30"=c("NH "),
#                                          "31"=c("NJ "),
#                                          "32"=c("NM "),
#                                          "33"=c("NY "),
#                                          "34"=c("NC "),
#                                          "35"=c("ND "),
#                                          "36"=c("OH "),
#                                          "37"=c("OK "),
#                                          "38"=c("OR "),
#                                          "39"=c("PA "),
#                                          "40"=c("RI "),
#                                          "41"=c("SC "),
#                                          "42"=c("SD "),
#                                          "43"=c("TN "),
#                                          "44"=c("TX "),
#                                          "45"=c("UT "),
#                                          "46"=c("VT "),
#                                          "47"=c("VA "),
#                                          "48"=c("WA "),
#                                          "49"=c("WV "),
#                                          "50"=c("WI "),
#                                          "51"=c("WY "),
#                                          "52"=c("DC "))
# 
# levels(updated_schools$state_id)
# table(updated_schools$state_id)
# 
# # Same with mailing state id
# updated_schools$mailing_state_id <- as.factor(updated_schools$mailing_state_id)
# levels(updated_schools$mailing_state_id)
# 
# 
# levels(updated_schools$mailing_state_id) <- list("2"=c("AL "),
#                                          "3"=c("AK "),
#                                          "5"=c("AR "),
#                                          "6"=c("CA "),
#                                          "7"=c("CO "),
#                                          "8"=c("CT "),
#                                          "9"=c("DE "),
#                                          "10"=c("FL "),
#                                          "11"=c("GA "),
#                                          "12"=c("HI "),
#                                          "13"=c("ID "),
#                                          "14"=c("IL "),
#                                          "15"=c("IN "),
#                                          "16"=c("IA "),
#                                          "17"=c("KS "),
#                                          "18"=c("KY "),
#                                          "19"=c("LA "),
#                                          "20"=c("ME "),
#                                          "21"=c("MD "),
#                                          "22"=c("MA "),
#                                          "23"=c("MI "),
#                                          "24"=c("MN "),
#                                          "25"=c("MS "),
#                                          "26"=c("MO "),
#                                          "27"=c("MT "),
#                                          "28"=c("NE "),
#                                          "29"=c("NV "),
#                                          "30"=c("NH "),
#                                          "31"=c("NJ "),
#                                          "32"=c("NM "),
#                                          "33"=c("NY "),
#                                          "34"=c("NC "),
#                                          "35"=c("ND "),
#                                          "36"=c("OH "),
#                                          "37"=c("OK "),
#                                          "38"=c("OR "),
#                                          "39"=c("PA "),
#                                          "40"=c("RI "),
#                                          "41"=c("SC "),
#                                          "42"=c("SD "),
#                                          "43"=c("TN "),
#                                          "44"=c("TX "),
#                                          "45"=c("UT "),
#                                          "46"=c("VT "),
#                                          "47"=c("VA "),
#                                          "48"=c("WA "),
#                                          "49"=c("WV "),
#                                          "50"=c("WI "),
#                                          "51"=c("WY "),
#                                          "52"=c("DC "))
# 
# levels(updated_schools$mailing_state_id) 
# table(updated_schools$mailing_state_id)
# 
# # Drop state and rename state_id to state to see if this is more accurate
# updated_schools <- updated_schools %>%
#   select(-state) %>%
#   dplyr::rename(state = state_id)

# Try another method
library(data.table)
library(magrittr)

z = rbindlist(list(schools, updated_schools), idcol=TRUE, use.names = TRUE)

z <- z[, lapply(.SD, function(x) 
  if (uniqueN(x)>1) x %>% unique %>% paste(collapse=";")
  else ""
), keyby=id]


### Export two datasets: work and LP_students ###
write.csv(char3, "C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\work_p.csv", 
          row.names = FALSE)
write.csv(char3, "C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\updated_schools.csv", 
          row.names = FALSE)

write.csv(z, "C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\updated_differences0114.csv", 
          row.names = FALSE)

################################################################################################################
################################################################################################################
## Additional matching before running these tables against QA and staging ##
# Read in QA 
QA_schools <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\schools-qa-Jan272021.csv",
                    colClasses = c("character"),
                    na.strings = c("","NA"),
                    stringsAsFactors = FALSE)


# New method 
QA_df <- merge(work2, QA_schools, by.x = "nces_nozero",by.y = "nces_id", all.x = TRUE)

# Fix the names of the dataframe
colnames(QA_df) <- sub(".x", "", colnames(QA_df))
names(QA_df)

QA_df_new <- QA_df %>%
  select(-contains(".y")) %>%
  select(-c(performance_growth_position,underlying_data_source,performance_position,
            created_at,updated_at,nces_nozero,ids,search_terms,low_grade_level,
            high_grade_level,is_home_school,is_eligible_for_swp,is_eligible_for_tas,provides_swp,
            is_public_school))

names(QA_df_new)


# names(df)
# df2 <- merge(df, new, by.x = "ids",by.y = "LP", all.x = TRUE)

# Check the amount of unique nces_id's and ids that are in df after merging 
length(unique(QA_df_new$nces_id)) #122049
length(unique(QA_df_new$id)) #100008

# Remove extra columns and duplicate nces_id's and ids from the merged dataset
QA_df_new <- QA_df_new %>%
  distinct(nces_id, .keep_all = TRUE) %>%
  distinct(id, .keep_all = TRUE) 

# Check how many id's remain after removing duplicates
length(which(!is.na(QA_df_new$id))) # 99973

# Re-order the columns of the dataset to match LP schools (for reference)
# col_order <- c("county_id", "full_time_equivalent_teachers","has_library",
#                "id", "is_charter_school","is_coed_school","is_magnet_school",
#                "public_private","is_shared_time_school","is_title_1_school",
#                "is_title_1_school_wide","mailing_address_line_1","mailing_city",
#                "mailing_postal_code","mailing_state_id","name","nces_id",
#                "phone_number","physical_address_line_1","physical_city",
#                "physical_latitude","physical_longtiude","physical_postal_code",
#                "state","religious_affiliation","school_type","state_school_id",
#                "students_by_gender","students_by_gender_and_race",
#                "students_by_grade_level","students_by_race",
#                "students_eligible_for_free_lunch","students_eligible_for_reduced_lunch",
#                "total_students","year_of_update","virtual_school_status",
#                "urban_centric_locale","pupil_teacher_ratio","school_community_type")
names(QA_df_new)
QA_df_new <- QA_df_new[, c(41,26,30,38,39,13,31,14,29,15,18,17,
               7,8,9,10,2,1,11,3,4,19,20,6,5,
               32,12,21,34,35,36,37,24,25,23,
               28,22,16,27,33)]
names(QA_df_new)

# Change state identifiers to match previous NCES update
QA_df_new$state <- as.factor(QA_df_new$state)
levels(QA_df_new$state)
table(QA_df_new$state)

levels(QA_df_new$state) <- list("2"=c("AL "),
                          "3"=c("AK "),
                          "5"=c("AR "),
                          "6"=c("CA "),
                          "7"=c("CO "),
                          "8"=c("CT "),
                          "9"=c("DE "),
                          "10"=c("FL "),
                          "11"=c("GA "),
                          "12"=c("HI "),
                          "13"=c("ID "),
                          "14"=c("IL "),
                          "15"=c("IN "),
                          "16"=c("IA "),
                          "17"=c("KS "),
                          "18"=c("KY "),
                          "19"=c("LA "),
                          "20"=c("ME "),
                          "21"=c("MD "),
                          "22"=c("MA "),
                          "23"=c("MI "),
                          "24"=c("MN "),
                          "25"=c("MS "),
                          "26"=c("MO "),
                          "27"=c("MT "),
                          "28"=c("NE "),
                          "29"=c("NV "),
                          "30"=c("NH "),
                          "31"=c("NJ "),
                          "32"=c("NM "),
                          "33"=c("NY "),
                          "34"=c("NC "),
                          "35"=c("ND "),
                          "36"=c("OH "),
                          "37"=c("OK "),
                          "38"=c("OR "),
                          "39"=c("PA "),
                          "40"=c("RI "),
                          "41"=c("SC "),
                          "42"=c("SD "),
                          "43"=c("TN "),
                          "44"=c("TX "),
                          "45"=c("UT "),
                          "46"=c("VT "),
                          "47"=c("VA "),
                          "48"=c("WA "),
                          "49"=c("WV "),
                          "50"=c("WI "),
                          "51"=c("WY "),
                          "52"=c("DC "))

levels(QA_df_new$state)
table(QA_df_new$state)

# Same with mailing state id
QA_df_new$mailing_state_id <- as.factor(QA_df_new$mailing_state_id)
levels(QA_df_new$mailing_state_id)


levels(QA_df_new$mailing_state_id) <- list("2"=c("AL "),
                                     "3"=c("AK "),
                                     "5"=c("AR "),
                                     "6"=c("CA "),
                                     "7"=c("CO "),
                                     "8"=c("CT "),
                                     "9"=c("DE "),
                                     "10"=c("FL "),
                                     "11"=c("GA "),
                                     "12"=c("HI "),
                                     "13"=c("ID "),
                                     "14"=c("IL "),
                                     "15"=c("IN "),
                                     "16"=c("IA "),
                                     "17"=c("KS "),
                                     "18"=c("KY "),
                                     "19"=c("LA "),
                                     "20"=c("ME "),
                                     "21"=c("MD "),
                                     "22"=c("MA "),
                                     "23"=c("MI "),
                                     "24"=c("MN "),
                                     "25"=c("MS "),
                                     "26"=c("MO "),
                                     "27"=c("MT "),
                                     "28"=c("NE "),
                                     "29"=c("NV "),
                                     "30"=c("NH "),
                                     "31"=c("NJ "),
                                     "32"=c("NM "),
                                     "33"=c("NY "),
                                     "34"=c("NC "),
                                     "35"=c("ND "),
                                     "36"=c("OH "),
                                     "37"=c("OK "),
                                     "38"=c("OR "),
                                     "39"=c("PA "),
                                     "40"=c("RI "),
                                     "41"=c("SC "),
                                     "42"=c("SD "),
                                     "43"=c("TN "),
                                     "44"=c("TX "),
                                     "45"=c("UT "),
                                     "46"=c("VT "),
                                     "47"=c("VA "),
                                     "48"=c("WA "),
                                     "49"=c("WV "),
                                     "50"=c("WI "),
                                     "51"=c("WY "),
                                     "52"=c("DC "))

levels(QA_df_new$mailing_state_id) 
table(QA_df_new$mailing_state_id)

# Try another method
# library(data.table)
# library(magrittr)
# 
# z = rbindlist(list(QA_schools, QA_df_new), idcol=TRUE, use.names = TRUE, fill = TRUE)
# 
# z <- z[, lapply(.SD, function(x) 
#   if (uniqueN(x)>1) x %>% unique %>% paste(collapse=";")
#   else ""
# ), keyby=id]

# Remove additional characters in the dataset and replace with 0
# This step is to help with pre-processing and allowing our dataset to have no special characters or encodings
char <- QA_df_new
specChars <- "â|€|¡|‡"

# Removing in specific columns 
char2 <- char %>%
  mutate(full_time_equivalent_teachers = str_replace_all(full_time_equivalent_teachers, specChars, "0"),
         students_by_gender = str_replace_all(students_by_gender, specChars, "0"),
         students_by_gender_and_race = str_replace_all(students_by_gender_and_race, specChars, "0"),
         students_by_grade_level = str_replace_all(students_by_grade_level, specChars, "0"),
         students_by_race = str_replace_all(students_by_race, specChars, "0"),
         students_eligible_for_free_lunch = str_replace_all(students_eligible_for_free_lunch, specChars, "0"),
         students_eligible_for_reduced_lunch = str_replace_all(students_eligible_for_reduced_lunch, specChars, "0"),
         total_students = str_replace_all(total_students, specChars, "0"))
char3 <- char2
char3[char3=="000"] <- "0"


write.csv(char3, "C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\QA_updatedschools_020121.csv", 
          row.names = FALSE)

#################### STAGING ###################################
# Read in Staging 
STAGING_schools <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\schools-staging-Jan272021.csv",
                       colClasses = c("character"),
                       na.strings = c("","NA"),
                       stringsAsFactors = FALSE)


# New method 
STAGING_df <- merge(work2, STAGING_schools, by.x = "nces_nozero",by.y = "nces_id", all.x = TRUE)

# Fix the names of the dataframe
colnames(STAGING_df) <- sub(".x", "", colnames(STAGING_df))
names(STAGING_df)

STAGING_df_new <- STAGING_df %>%
  select(-contains(".y")) %>%
  select(-c(performance_growth_position,underlying_data_source,performance_position,
            created_at,updated_at,nces_nozero,ids,search_terms,low_grade_level,
            high_grade_level,is_home_school,is_eligible_for_swp,is_eligible_for_tas,provides_swp,
            is_public_school))

names(STAGING_df_new)


# names(df)
# df2 <- merge(df, new, by.x = "ids",by.y = "LP", all.x = TRUE)

# Check the amount of unique nces_id's and ids that are in df after merging 
length(unique(STAGING_df_new$nces_id)) #122049
length(unique(STAGING_df_new$id)) #100008

# Remove extra columns and duplicate nces_id's and ids from the merged dataset
STAGING_df_new <- STAGING_df_new %>%
  distinct(nces_id, .keep_all = TRUE) %>%
  distinct(id, .keep_all = TRUE) 

# Check how many id's remain after removing duplicates
length(which(!is.na(STAGING_df_new$id))) # 99916

# Re-order the columns of the dataset to match LP schools (for reference)
# col_order <- c("county_id", "full_time_equivalent_teachers","has_library",
#                "id", "is_charter_school","is_coed_school","is_magnet_school",
#                "public_private","is_shared_time_school","is_title_1_school",
#                "is_title_1_school_wide","mailing_address_line_1","mailing_city",
#                "mailing_postal_code","mailing_state_id","name","nces_id",
#                "phone_number","physical_address_line_1","physical_city",
#                "physical_latitude","physical_longtiude","physical_postal_code",
#                "state","religious_affiliation","school_type","state_school_id",
#                "students_by_gender","students_by_gender_and_race",
#                "students_by_grade_level","students_by_race",
#                "students_eligible_for_free_lunch","students_eligible_for_reduced_lunch",
#                "total_students","year_of_update","virtual_school_status",
#                "urban_centric_locale","pupil_teacher_ratio","school_community_type")
names(STAGING_df_new)
STAGING_df_new <- STAGING_df_new[, c(41,26,30,38,39,13,31,14,29,15,18,17,
                           7,8,9,10,2,1,11,3,4,19,20,6,5,
                           32,12,21,34,35,36,37,24,25,23,
                           28,22,16,27,33)]
names(STAGING_df_new)

# Change state identifiers to match previous NCES update
STAGING_df_new$state <- as.factor(STAGING_df_new$state)
levels(STAGING_df_new$state)
table(STAGING_df_new$state)

levels(STAGING_df_new$state) <- list("2"=c("AL "),
                                "3"=c("AK "),
                                "5"=c("AR "),
                                "6"=c("CA "),
                                "7"=c("CO "),
                                "8"=c("CT "),
                                "9"=c("DE "),
                                "10"=c("FL "),
                                "11"=c("GA "),
                                "12"=c("HI "),
                                "13"=c("ID "),
                                "14"=c("IL "),
                                "15"=c("IN "),
                                "16"=c("IA "),
                                "17"=c("KS "),
                                "18"=c("KY "),
                                "19"=c("LA "),
                                "20"=c("ME "),
                                "21"=c("MD "),
                                "22"=c("MA "),
                                "23"=c("MI "),
                                "24"=c("MN "),
                                "25"=c("MS "),
                                "26"=c("MO "),
                                "27"=c("MT "),
                                "28"=c("NE "),
                                "29"=c("NV "),
                                "30"=c("NH "),
                                "31"=c("NJ "),
                                "32"=c("NM "),
                                "33"=c("NY "),
                                "34"=c("NC "),
                                "35"=c("ND "),
                                "36"=c("OH "),
                                "37"=c("OK "),
                                "38"=c("OR "),
                                "39"=c("PA "),
                                "40"=c("RI "),
                                "41"=c("SC "),
                                "42"=c("SD "),
                                "43"=c("TN "),
                                "44"=c("TX "),
                                "45"=c("UT "),
                                "46"=c("VT "),
                                "47"=c("VA "),
                                "48"=c("WA "),
                                "49"=c("WV "),
                                "50"=c("WI "),
                                "51"=c("WY "),
                                "52"=c("DC "))
levels(STAGING_df_new$state)
table(STAGING_df_new$state)

# Same with mailing state id
STAGING_df_new$mailing_state_id <- as.factor(STAGING_df_new$mailing_state_id)
levels(STAGING_df_new$mailing_state_id)


levels(STAGING_df_new$mailing_state_id) <- list("2"=c("AL "),
                                           "3"=c("AK "),
                                           "5"=c("AR "),
                                           "6"=c("CA "),
                                           "7"=c("CO "),
                                           "8"=c("CT "),
                                           "9"=c("DE "),
                                           "10"=c("FL "),
                                           "11"=c("GA "),
                                           "12"=c("HI "),
                                           "13"=c("ID "),
                                           "14"=c("IL "),
                                           "15"=c("IN "),
                                           "16"=c("IA "),
                                           "17"=c("KS "),
                                           "18"=c("KY "),
                                           "19"=c("LA "),
                                           "20"=c("ME "),
                                           "21"=c("MD "),
                                           "22"=c("MA "),
                                           "23"=c("MI "),
                                           "24"=c("MN "),
                                           "25"=c("MS "),
                                           "26"=c("MO "),
                                           "27"=c("MT "),
                                           "28"=c("NE "),
                                           "29"=c("NV "),
                                           "30"=c("NH "),
                                           "31"=c("NJ "),
                                           "32"=c("NM "),
                                           "33"=c("NY "),
                                           "34"=c("NC "),
                                           "35"=c("ND "),
                                           "36"=c("OH "),
                                           "37"=c("OK "),
                                           "38"=c("OR "),
                                           "39"=c("PA "),
                                           "40"=c("RI "),
                                           "41"=c("SC "),
                                           "42"=c("SD "),
                                           "43"=c("TN "),
                                           "44"=c("TX "),
                                           "45"=c("UT "),
                                           "46"=c("VT "),
                                           "47"=c("VA "),
                                           "48"=c("WA "),
                                           "49"=c("WV "),
                                           "50"=c("WI "),
                                           "51"=c("WY "),
                                           "52"=c("DC "))
levels(STAGING_df_new$mailing_state_id) 
table(STAGING_df_new$mailing_state_id)

# Try another method
# library(data.table)
# library(magrittr)
# 
# z = rbindlist(list(QA_schools, QA_df_new), idcol=TRUE, use.names = TRUE, fill = TRUE)
# 
# z <- z[, lapply(.SD, function(x) 
#   if (uniqueN(x)>1) x %>% unique %>% paste(collapse=";")
#   else ""
# ), keyby=id]

# This step is to help with pre-processing and allowing our dataset to have no special characters or encodings
char <- STAGING_df_new
specChars <- "â|€|¡|‡"

# Removing in specific columns 
char2 <- char %>%
  mutate(full_time_equivalent_teachers = str_replace_all(full_time_equivalent_teachers, specChars, "0"),
         students_by_gender = str_replace_all(students_by_gender, specChars, "0"),
         students_by_gender_and_race = str_replace_all(students_by_gender_and_race, specChars, "0"),
         students_by_grade_level = str_replace_all(students_by_grade_level, specChars, "0"),
         students_by_race = str_replace_all(students_by_race, specChars, "0"),
         students_eligible_for_free_lunch = str_replace_all(students_eligible_for_free_lunch, specChars, "0"),
         students_eligible_for_reduced_lunch = str_replace_all(students_eligible_for_reduced_lunch, specChars, "0"),
         total_students = str_replace_all(total_students, specChars, "0"))
char3 <- char2
char3[char3=="000"] <- "0"

write.csv(char3, "C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\Staging_updatedschools_020121.csv", 
          row.names = FALSE)

#################### SANDBOX ###################################
# Read in Staging 
SANDBOX_schools <- read.csv("C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\sandbox-02052021-schools.csv",
                            colClasses = c("character"),
                            na.strings = c("","NA"),
                            stringsAsFactors = FALSE)


# New method 
SANDBOX_df <- merge(work2, SANDBOX_schools, by.x = "nces_nozero",by.y = "nces_id", all.x = TRUE)

# Fix the names of the dataframe
colnames(SANDBOX_df) <- sub(".x", "", colnames(SANDBOX_df))
names(SANDBOX_df)

SANDBOX_df_new <- SANDBOX_df %>%
  select(-contains(".y")) %>%
  select(-c(performance_growth_position,underlying_data_source,performance_position,
            created_at,updated_at,nces_nozero,ids,search_terms,low_grade_level,
            high_grade_level,is_home_school,is_eligible_for_swp,is_eligible_for_tas,provides_swp,
            is_public_school))

names(SANDBOX_df_new)


# names(df)
# df2 <- merge(df, new, by.x = "ids",by.y = "LP", all.x = TRUE)

# Check the amount of unique nces_id's and ids that are in df after merging 
length(unique(SANDBOX_df_new$nces_id)) #122049
length(unique(SANDBOX_df_new$id)) #100008

# Remove extra columns and duplicate nces_id's and ids from the merged dataset
SANDBOX_df_new <- SANDBOX_df_new %>%
  distinct(nces_id, .keep_all = TRUE) %>%
  distinct(id, .keep_all = TRUE) 

# Check how many id's remain after removing duplicates
length(which(!is.na(SANDBOX_df_new$id))) # 99916

# Re-order the columns of the dataset to match LP schools (for reference)
# col_order <- c("county_id", "full_time_equivalent_teachers","has_library",
#                "id", "is_charter_school","is_coed_school","is_magnet_school",
#                "public_private","is_shared_time_school","is_title_1_school",
#                "is_title_1_school_wide","mailing_address_line_1","mailing_city",
#                "mailing_postal_code","mailing_state_id","name","nces_id",
#                "phone_number","physical_address_line_1","physical_city",
#                "physical_latitude","physical_longtiude","physical_postal_code",
#                "state","religious_affiliation","school_type","state_school_id",
#                "students_by_gender","students_by_gender_and_race",
#                "students_by_grade_level","students_by_race",
#                "students_eligible_for_free_lunch","students_eligible_for_reduced_lunch",
#                "total_students","year_of_update","virtual_school_status",
#                "urban_centric_locale","pupil_teacher_ratio","school_community_type")
names(SANDBOX_df_new)
SANDBOX_df_new <- SANDBOX_df_new[, c(41,26,30,38,39,13,31,14,29,15,18,17,
                                     7,8,9,10,2,1,11,3,4,19,20,6,5,
                                     32,12,21,34,35,36,37,24,25,23,
                                     28,22,16,27,33)]
names(SANDBOX_df_new)

# Change state identifiers to match previous NCES update
SANDBOX_df_new$state <- as.factor(SANDBOX_df_new$state)
levels(SANDBOX_df_new$state)
table(SANDBOX_df_new$state)

levels(SANDBOX_df_new$state) <- list("2"=c("AL "),
                                     "3"=c("AK "),
                                     "5"=c("AR "),
                                     "6"=c("CA "),
                                     "7"=c("CO "),
                                     "8"=c("CT "),
                                     "9"=c("DE "),
                                     "10"=c("FL "),
                                     "11"=c("GA "),
                                     "12"=c("HI "),
                                     "13"=c("ID "),
                                     "14"=c("IL "),
                                     "15"=c("IN "),
                                     "16"=c("IA "),
                                     "17"=c("KS "),
                                     "18"=c("KY "),
                                     "19"=c("LA "),
                                     "20"=c("ME "),
                                     "21"=c("MD "),
                                     "22"=c("MA "),
                                     "23"=c("MI "),
                                     "24"=c("MN "),
                                     "25"=c("MS "),
                                     "26"=c("MO "),
                                     "27"=c("MT "),
                                     "28"=c("NE "),
                                     "29"=c("NV "),
                                     "30"=c("NH "),
                                     "31"=c("NJ "),
                                     "32"=c("NM "),
                                     "33"=c("NY "),
                                     "34"=c("NC "),
                                     "35"=c("ND "),
                                     "36"=c("OH "),
                                     "37"=c("OK "),
                                     "38"=c("OR "),
                                     "39"=c("PA "),
                                     "40"=c("RI "),
                                     "41"=c("SC "),
                                     "42"=c("SD "),
                                     "43"=c("TN "),
                                     "44"=c("TX "),
                                     "45"=c("UT "),
                                     "46"=c("VT "),
                                     "47"=c("VA "),
                                     "48"=c("WA "),
                                     "49"=c("WV "),
                                     "50"=c("WI "),
                                     "51"=c("WY "),
                                     "52"=c("DC "))
levels(SANDBOX_df_new$state)
table(SANDBOX_df_new$state)

# Same with mailing state id
SANDBOX_df_new$mailing_state_id <- as.factor(SANDBOX_df_new$mailing_state_id)
levels(SANDBOX_df_new$mailing_state_id)


levels(SANDBOX_df_new$mailing_state_id) <- list("2"=c("AL "),
                                                "3"=c("AK "),
                                                "5"=c("AR "),
                                                "6"=c("CA "),
                                                "7"=c("CO "),
                                                "8"=c("CT "),
                                                "9"=c("DE "),
                                                "10"=c("FL "),
                                                "11"=c("GA "),
                                                "12"=c("HI "),
                                                "13"=c("ID "),
                                                "14"=c("IL "),
                                                "15"=c("IN "),
                                                "16"=c("IA "),
                                                "17"=c("KS "),
                                                "18"=c("KY "),
                                                "19"=c("LA "),
                                                "20"=c("ME "),
                                                "21"=c("MD "),
                                                "22"=c("MA "),
                                                "23"=c("MI "),
                                                "24"=c("MN "),
                                                "25"=c("MS "),
                                                "26"=c("MO "),
                                                "27"=c("MT "),
                                                "28"=c("NE "),
                                                "29"=c("NV "),
                                                "30"=c("NH "),
                                                "31"=c("NJ "),
                                                "32"=c("NM "),
                                                "33"=c("NY "),
                                                "34"=c("NC "),
                                                "35"=c("ND "),
                                                "36"=c("OH "),
                                                "37"=c("OK "),
                                                "38"=c("OR "),
                                                "39"=c("PA "),
                                                "40"=c("RI "),
                                                "41"=c("SC "),
                                                "42"=c("SD "),
                                                "43"=c("TN "),
                                                "44"=c("TX "),
                                                "45"=c("UT "),
                                                "46"=c("VT "),
                                                "47"=c("VA "),
                                                "48"=c("WA "),
                                                "49"=c("WV "),
                                                "50"=c("WI "),
                                                "51"=c("WY "),
                                                "52"=c("DC "))
levels(SANDBOX_df_new$mailing_state_id) 
table(SANDBOX_df_new$mailing_state_id)

# Try another method
# library(data.table)
# library(magrittr)
# 
# z = rbindlist(list(QA_schools, QA_df_new), idcol=TRUE, use.names = TRUE, fill = TRUE)
# 
# z <- z[, lapply(.SD, function(x) 
#   if (uniqueN(x)>1) x %>% unique %>% paste(collapse=";")
#   else ""
# ), keyby=id]

# This step is to help with pre-processing and allowing our dataset to have no special characters or encodings
char <- SANDBOX_df_new
specChars <- "â|€|¡|‡"

# Removing in specific columns 
char2 <- char %>%
  mutate(full_time_equivalent_teachers = str_replace_all(full_time_equivalent_teachers, specChars, "0"),
         students_by_gender = str_replace_all(students_by_gender, specChars, "0"),
         students_by_gender_and_race = str_replace_all(students_by_gender_and_race, specChars, "0"),
         students_by_grade_level = str_replace_all(students_by_grade_level, specChars, "0"),
         students_by_race = str_replace_all(students_by_race, specChars, "0"),
         students_eligible_for_free_lunch = str_replace_all(students_eligible_for_free_lunch, specChars, "0"),
         students_eligible_for_reduced_lunch = str_replace_all(students_eligible_for_reduced_lunch, specChars, "0"),
         total_students = str_replace_all(total_students, specChars, "0"))
char3 <- char2
char3[char3=="000"] <- "0"

write.csv(char3, "C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\SANDBOX_updatedschools_020521.csv", 
          row.names = FALSE)
