extract_temp <- function(patientunitstayids, table_df, lower_hr, upper_hr) {
  source("~/scratch/cicm_extubation/features/hanrepo/useful_functions.R")
  require(tidyverse)
  require(plotly)
  library(caret)
  library(doParallel)
  
  #### TEMPERATURE FEATURE EXTRACTION PERFORMED HERE ###
  table_df <- table_df[which(table_df$patientunitstayid %in% patientunitstayids), ] %>% droplevels(.)
  table_df$value <- as.numeric(as.character(table_df$value))
  table_df <- table_df[which(table_df$offset > (lower_hr * 60) & table_df$offset < (upper_hr * 60)), ] %>% droplevels(.)
  
  temp_features <- as.data.frame(patientunitstayids)
  colnames(temp_features) <- c("patientunitstayid")
  id <- "patientunitstayid"
  tempchart <- table_df %>% droplevels(.)
  tempchart <- tempchart[complete.cases(tempchart), ]
  
  #last
  lastperpatient <- tempchart %>% group_by(patientunitstayid) %>% slice(which.max(offset))
  lastperpatient <- lastperpatient %>% dplyr::select(patientunitstayid, value) %>% droplevels()
  colnames(lastperpatient) <- c(id, "last_temperature")
  
  #first
  firstperpatient <- tempchart %>% group_by(patientunitstayid) %>% slice(which.min(offset))
  firstperpatient <- firstperpatient %>% dplyr::select(patientunitstayid, value) %>% droplevels(.)
  colnames(firstperpatient) <- c(id, "temperature_delta")
  firstperpatient$temperature_delta <- (lastperpatient$last_temperature - firstperpatient$temperature_delta)
  
  #highest
  hightemp <- tempchart %>% group_by(patientunitstayid) %>% slice(which.max(value))
  hightemp <- hightemp %>% dplyr::select(patientunitstayid, value) %>% droplevels(.)
  temp <- hightemp$value
  if (length(hightemp$value > 0)) {
    hightemp$value <- if(temp > 38.2) 1 else 0
  }
  colnames(hightemp) <- c(id, "temperature_over_38.2")
  
  #lowest
  lowtemp <- tempchart %>% group_by(patientunitstayid) %>% slice(which.min(value))
  lowtemp <- lowtemp %>% dplyr::select(patientunitstayid, value) %>% droplevels(.)
  temp <- lowtemp$value
  if (length(lowtemp$value > 0)) {
    lowtemp$value <- if(temp < 36) 1 else 0
  }
  colnames(lowtemp) <- c(id, "temperature_under_36")
  
  firstperpatient <- as.data.frame(firstperpatient)
  lastperpatient <- as.data.frame(lastperpatient)
  hightemp <- as.data.frame(hightemp)
  lowtemp <- as.data.frame(lowtemp)
  
  combined <- cbind(firstperpatient, lastperpatient$last_temperature, hightemp$temperature_over_38.2, lowtemp$temperature_under_36)
   
  temp_features <- merge(temp_features, combined, by = "patientunitstayid", all = T)
  temp_features <- replace_infinite(temp_features, NA)
  temp_features <- replace_nan(temp_features, NA)
  
  return(temp_features)
  
}