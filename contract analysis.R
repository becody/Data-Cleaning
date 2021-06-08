################ Contract Analysis #######################
# Date: 10/21/2020
# Goal: perform one time data cleaning and analysis for contract data
# Author: Brittany Cody

# Install packages
install.packages("arsenal")
install.packages("tidyr")
install.packages("readr")
library(dplyr)
library(tidyverse)
library(arsenal)
library(stringr)
library(readr)
library(tidyr)

# Read in datasets 
contract <- read.csv("C:\\Users\\Data\\organization_tools_contract.csv")
line_items <- read.csv("C:\\Users\\Data\\line_items.csv")
org_id <- read.csv("C:\\Users\\Data\\organizations.csv")
tool_id <- read.csv("C:\\Users\\\Data\\tools.csv")

# Merge two datasets by unique ID's
names(contract)
names(line_items)

# Try to merge based on organization tool id
contract$org_tool_id <- contract$id
line_items$org_tool_id <- line_items$organization_tool_id
org_id$organization_id <- org_id$id
tool_id$tool_id <- tool_id$ï..id

n_contract <- line_items %>%
  merge(contract, by = c("org_tool_id"), all = TRUE)

y_contract <- n_contract %>%
  merge(org_id, by = c("organization_id"), all.x = TRUE) %>%
  drop_na(org_tool_id)

f_contract <- y_contract %>%
  merge(tool_id, by=c("tool_id"), all.x = TRUE)

unique_contract <- unique(n_contract[duplicated(n_contract$org_tool_id),])

# Export contract 
write.csv(f_contract, "C:\\Users\\17046\\OneDrive\\Documents\\LearnPlatform\\Analysis\\Data\\contract073120.csv", 
          row.names = FALSE)

