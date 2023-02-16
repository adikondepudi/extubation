extract_fluid <- function(patientunitstayids, table_df, lower_hr, upper_hr) {
  source("~/scratch/cicm_extubation/features/hanrepo/useful_functions.R")
  require(tidyverse)
  require(plotly)
  library(caret)
  library(doParallel)
  
  #### TEMPERATURE FEATURE EXTRACTION PERFORMED HERE ###
  table_df <- table_df[which(table_df$patientunitstayid %in% patientunitstayids), ] %>% droplevels(.)
  table_df$nettotal <- as.numeric(as.character(table_df$nettotal))
  table_df <- table_df[which(table_df$intakeoutputentryoffset > (lower_hr * 60) & table_df$intakeoutputentryoffset < (upper_hr * 60)), ] %>% droplevels(.)
  
  temp_features <- as.data.frame(patientunitstayids)
  colnames(temp_features) <- c("patientunitstayid")
  id <- "patientunitstayid"
  tempchart <- table_df %>% droplevels(.)
  tempchart <- tempchart[complete.cases(tempchart), ]
  
  #last
  lastperpatient <- tempchart %>% group_by(patientunitstayid) %>% slice(which.max(intakeoutputentryoffset))
  lastperpatient <- lastperpatient %>% dplyr::select(patientunitstayid, nettotal) %>% droplevels()
  colnames(lastperpatient) <- c(id, "last_fluid_balance")
  
  #first
  firstperpatient <- tempchart %>% group_by(patientunitstayid) %>% slice(which.min(intakeoutputentryoffset))
  firstperpatient <- firstperpatient %>% dplyr::select(patientunitstayid, nettotal) %>% droplevels(.)
  colnames(firstperpatient) <- c(id, "fluid_balance_delta")
  firstperpatient$fluid_balance_delta <- (lastperpatient$last_fluid_balance - firstperpatient$fluid_balance_delta)
  
  firstperpatient <- as.data.frame(firstperpatient)
  lastperpatient <- as.data.frame(lastperpatient)
  
  combined <- cbind(firstperpatient, lastperpatient$last_fluid_balance)
   
  temp_features <- merge(temp_features, combined, by = "patientunitstayid", all = T)
  temp_features <- replace_infinite(temp_features, NA)
  temp_features <- replace_nan(temp_features, NA)
  
  return(temp_features)
  
}