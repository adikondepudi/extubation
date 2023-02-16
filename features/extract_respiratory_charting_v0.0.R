# extract all features from RESPIRATORY CHARTING table

#example
# patient <- fread("~/RO1_2020/ICP/propofol/ICP_meta_table.csv")
# patient <- patient %>% dplyr::select(patientunitstayid, drug_offset)
# colnames(patient)[2] <- "offset" #diagosis offset
# 
# #we want to only extract data from before the offset for each patient.
# 
# patient <- patient %>% group_by(patientunitstayid) %>% slice(which.min(offset))
# 
# pids <- patient$patientunitstayid
# 
# resp_chart_df <- fread("/storage/eICU/respiratoryCharting.csv")
# resp_chart_df <- resp_chart_df[which(resp_chart_df$patientunitstayid %in% unique(pids)), ]
# 
# resp_chart_final <- rbind()
# for (i in 1:length(pids)) {
#   print(paste0("--------------", i, "--------------"))
#   
#   tempid <- pids[i]
#   temp_row <- patient[patient$patientunitstayid == tempid, ]
#   upper_hr <- temp_row$offset / 60
#   lower_hr <- upper_hr - 6
#   
#   temp_resp_chart <- extract_respiratory_charting(patientunitstayids = tempid, table_df = resp_chart_df, lower_hr = lower_hr, upper_hr = upper_hr, per_pid_boolean = T, pre_stats = T, extras = T)
#   
#   resp_chart_final <- rbind.fill(resp_chart_final, temp_resp_chart)
# }


extract_respiratory_charting <- function(patientunitstayids, table_df, lower_hr, upper_hr, per_pid_boolean, pre_stats, extras) {
  source("~/scratch/cicm_extubation/features/hanrepo/useful_functions.R")
  require(tidyverse)
  require(plotly)
  library(caret)
  library(doParallel)
  
  if (missing(lower_hr)) {
    lower_hr <- -Inf
  }
  
  if (missing(upper_hr)) {
    upper_hr <- Inf
  }
  
  if (missing(per_pid_boolean)) {
    per_pid_boolean == FALSE
  }
  
  if (missing(extras)) {
    extras == FALSE
  }

  if (per_pid_boolean == TRUE) {
    patientunitstayids <- c(patientunitstayids)
  }
  
  # filter for pids and time frame
  table_df <- table_df[which(table_df$patientunitstayid %in% patientunitstayids), ] %>% droplevels(.)
  
  table_df$respchartvalue <- as.numeric(as.character(table_df$respchartvalue))
  
  if (pre_stats == TRUE) {
    pre_stats_df <- table_df[which(table_df$respchartoffset > -Inf & table_df$respchartoffset <= (lower_hr * 60)), ] %>% droplevels(.)
    pre_stats_df$respchartvalueLabel <- as.character(pre_stats_df$respchartvaluelabel)
  }
  
  table_df <- table_df[which(table_df$respchartoffset > (lower_hr * 60) & table_df$respchartoffset < (upper_hr * 60)), ] %>% droplevels(.)
  
  table_df$respchartvaluelabel <- as.character(table_df$respchartvaluelabel)
  
  varnames <- as.character(unique(table_df$respchartvaluelabel))
  
  resp_chart_features <- as.data.frame(patientunitstayids)
  colnames(resp_chart_features) <- c("patientunitstayid")
  id <- "patientunitstayid"
  
  #fix some of the varnames for uniformity by removing spaces, numbers, dots, and other unsavory variable names.
  table_df$respchartvalue[which(table_df$respchartvaluelabel == "RT Vent On/Off" & table_df$respchartvalue == "Continued")] <- 1
  table_df$respchartvalue[which(table_df$respchartvaluelabel == "RT Vent On/Off" & table_df$respchartvalue != "Continued")] <- 0
  
  table_df$respchartvaluelabel <- as.character(table_df$respchartvaluelabel)
  table_df$respchartvaluelabel <- gsub("-", "", table_df$respchartvaluelabel)
  table_df$respchartvaluelabel <- gsub(" ", "_", table_df$respchartvaluelabel)
  table_df$respchartvaluelabel <- gsub("__", "_", table_df$respchartvaluelabel)
  table_df$respchartvaluelabel <- gsub("\\(|\\)", "", table_df$respchartvaluelabel)
  table_df$respchartvaluelabel <- gsub("\\%|\\)", "percent", table_df$respchartvaluelabel)
  table_df$respchartvaluelabel <- gsub("\\.", "", table_df$respchartvaluelabel)
  
  # table_df$labname[which(table_df$labname == "HCO3")] <- "bicarbonate"
  
  varnames <- as.character(unique(table_df$respchartvaluelabel))
  # View(varnames)
  
  for (i in 1:length(varnames)) {
    chartname <- as.character(varnames[i])
    # print(i)

    #find pre-stats ----------------------------------------------------------------------------------------------
    if (pre_stats == TRUE) {
      idx <- which(pre_stats_df$respchartvaluelabel == chartname)
      temppre <- pre_stats_df[idx, ] %>% droplevels(.)
      premeanperpatient <- temppre %>% group_by(patientunitstayid) %>% dplyr::summarise(temppre_mean = mean(respchartvalue, 
                                                                                                           na.rm = T))
      colnames(premeanperpatient) <- c(id, paste0(chartname, "_pre_mean"))
    }
    
    #find simple statistics --------------------------------------------------------------------------------------
    idx <- which(table_df$respchartvaluelabel == chartname)
    tempchart <- table_df[idx, ] %>% droplevels(.)
    tempchart <- tempchart[complete.cases(tempchart), ]
    #mean
    meanperpatient <- tempchart %>% group_by(patientunitstayid) %>% dplyr::summarise(tempchart_mean = mean(respchartvalue, 
                                                                                                         na.rm = T))
    colnames(meanperpatient) <- c(id, paste0(chartname, "_mean"))
    
    #sd
    sdperpatient <- tempchart %>% group_by(patientunitstayid) %>% dplyr::summarise(tempchart_sd = sd(respchartvalue, 
                                                                                                   na.rm = T))
    colnames(sdperpatient) <- c(id, paste0(chartname, "_sd"))
    
    #min
    minperpatient <- tempchart %>% group_by(patientunitstayid) %>% dplyr::summarise(tempchart_min = min(respchartvalue, 
                                                                                                      na.rm = T))
    colnames(minperpatient) <- c(id, paste0(chartname, "_min"))
    
    #max
    maxperpatient <- tempchart %>% group_by(patientunitstayid) %>% dplyr::summarise(tempchart_max = max(respchartvalue, 
                                                                                                      na.rm = T))
    colnames(maxperpatient) <- c(id, paste0(chartname, "_max"))
    
    ### extras
    if (extras == T) {
      
      #first
      firstperpatient <- tempchart %>% group_by(patientunitstayid) %>% slice(which.min(respchartoffset))
      firstperpatient <- firstperpatient %>% dplyr::select(patientunitstayid, respchartvalue)
      colnames(firstperpatient) <- c(id, paste0(chartname, "_first"))
      
      #last
      lastperpatient <- tempchart %>% group_by(patientunitstayid) %>% slice(which.max(respchartoffset))
      lastperpatient <- lastperpatient %>% dplyr::select(patientunitstayid, respchartvalue)
      colnames(lastperpatient) <- c(id, paste0(chartname, "_last"))
      
    }
    
    #combine data and remove correlated features
    combined <- cbind(meanperpatient, minperpatient[,2], maxperpatient[,2], sdperpatient[,2])
    
    if (extras == T) {
      combined <- cbind(combined, firstperpatient[,2], lastperpatient[,2])
    }
    
    #REMOVED
    # #zeros replace all NAs that may come up due to not enough data points for sd. This is done to ensure correlation is still calculated despite there being NAs. Also, by replacing with ZERO it considers the correlation to every point and there is no need to do pairwise complete correlation, which then reduces samples down to only the complete cases. 
    # 
    # tmpcombined <- combined
    # tmpcombined[is.na(tmpcombined)] <- 0
    # 
    # #remove zero variance variables (same number = not predictive)
    # if (nrow(combined) > 1) {
    #   zero_var <- apply(tmpcombined, 2, function(x) length(unique(x)) == 1)
    #   combined <- combined[, !zero_var]
    #   #reset tmpcombined
    #   tmpcombined <- combined
    #   tmpcombined[is.na(tmpcombined)] <- 0
    # }
    # 
    # tmp_cor <- cor(tmpcombined, method = "kendall")
    # tmp_cor_varnames <- findCorrelation(tmp_cor, cutoff = 0.9, names = T)
    # if (length(tmp_cor_varnames != 0)) {
    #   combined <- combined[ , -which(names(combined) %in% tmp_cor_varnames)]
    # }
    
    if (pre_stats == TRUE) {
      combined <- merge(combined, premeanperpatient, by = "patientunitstayid", all = T)
    }
    
    resp_chart_features <- merge(resp_chart_features, combined, by = "patientunitstayid", all = T)
    resp_chart_features <- replace_infinite(resp_chart_features, NA)
    resp_chart_features <- replace_nan(resp_chart_features, NA)
  }
  
  return(resp_chart_features)
  
}
