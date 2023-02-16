extract_apache <- function(patientunitstayids, table_df, lower_hr, upper_hr) {
  source("~/scratch/cicm_extubation/features/hanrepo/useful_functions.R")
  require(tidyverse)
  require(plotly)
  library(caret)
  library(doParallel)
    
  #filter for patients and time frame
  table_df <- table_df[which(table_df$patientunitstayid %in% patientunitstayids), ]
    
  variables <- c("verbal", "motor", "eyes", "pao2", "fio2", "pco2", "wbc")
    
  pids <- as.data.frame(patientunitstayids)
  colnames(pids) <- "patientunitstayid"
    
    tempdf <- table_df %>% select(patientunitstayid, all_of(variables))
    colnames(tempdf) <- c("patientunitstayid", "apache_verbal", "apache_motor", "apache_eyes",
                          "apache_pao2", "apache_fio2", "apache_pco2", "apache_wbc")
    tempdf <- tempdf[complete.cases(tempdf),]
      
    pids <- merge(pids, tempdf, by = "patientunitstayid", all = T)
      
  
  tempdf <- table_df %>% select(patientunitstayid, pao2, fio2)
  colnames(tempdf) <- c("patientunitstayid", "pao2", "fio2")
  tempdf <- tempdf[complete.cases(tempdf),]
  tempdf$var <- tempdf$pao2 / tempdf$fi
  
  val <- tempdf %>% select(patientunitstayid, var)
  colnames(val) <- c("patientunitstayid", paste0("apache_pao2/fio2"))
  
  pids <- merge(pids, val, by = "patientunitstayid", all = T)
  
  return(pids)
    
}