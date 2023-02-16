#Alex: 02/03/21 - created to create feature and label spaces for sample ID dataframe demographics
#Alex: 03/04/21 - added respiratory charting feature extraction and imputation code here to streamline model script
#Alex: 03/16/21 - added motor score feature extraction
#Alex: 04/01/21 - adding temperature feature extraction, remove region and hospitalid features, add LOS feature
#Han: 05/01/21  - double check code
#Alex: 05/05/21 - adding apacheApsVar, pastHistory, intakeOutput features
#Alex: 05/20/21 - adding PTS feature extraction

library(tidyverse)
library(plyr)
library(data.table)
library(foreach)
library(doParallel)
require(plotly)
library(caret)

# main_dir = "~/Medical_tutorial/Alexandra/cicm_extubation/"
# eicu_dir = "/storage/eICU/"
# code_source = "/storage/eICU/eICU_feature_extract/eICU_featurization/"

main_dir = "~/scratch/cicm_extubation/"
eicu_dir = "~/scratch/"
code_source = "~/scratch/cicm_extubation/features/hanrepo/"

mvpids <- read.csv(paste0(main_dir, "/csv/300meds_by_num.csv"))
expids <- mvpids[which(mvpids$is_last == TRUE & mvpids$Meets_criteria == TRUE & mvpids$MV_number == 1),]
repids <- mvpids[which(mvpids$is_last == FALSE & mvpids$Meets_criteria == TRUE & (mvpids$next_MV_start_offset - mvpids$MV_end_offset < 4320)),]
patient <- fread(paste0(eicu_dir, "/patient.csv"))

source(paste0(code_source, "/extract_patient.R"))
patient_features <- extract_patient(
                            patientunitstayid_list = mvpids$patientunitstayid[drop = F], 
                            eicu_dir = eicu_dir,
                            patient_table = patient, 
                            hospital_table = fread(paste0(eicu_dir, "/hospital.csv")),
                            as_binary = T,
                            code_dir = code_source)

# Remove this? What is this needed for?
label <- extract_patient(patientunitstayid_list = mvpids$patientunitstayid[drop = F], 
                            eicu_dir = eicu_dir,
                            patient_table = patient, 
                            hospital_table = fread(paste0(eicu_dir, "/hospital.csv")),
                            #apachedx_dictionary_path = "~/scratch/cicm_extubation/features/hanrepo/ApacheDX_dict.xlsx", 
                            as_binary = F,
                            code_dir = code_source,
                            labels_only = T)

# Drop some noisy features and add other relevant features from label dataframe
drops <- c("hospitalid","wardid", "region_Northeast", "region_Midwest", "region_South", "region_West")
patient_features <- patient_features[ , !(names(patient_features) %in% drops)]
patient_features <- cbind(patient_features, label$hospital_LOS, label$unit_LOS)

# Set up patient table------------------------------------------------------------------------------
patient <- mvpids
patient <- patient %>% dplyr::select(patientunitstayid, MV_end_offset, MV_start_offset)
colnames(patient)[2] <- "offset" #diagosis offset#
colnames(patient)[3] <- "start" #diagosis offset#
#we want to only extract data from before the offset for each patient.
patient <- patient %>% group_by(patientunitstayid) %>% slice(which.min(offset))

pids <- patient$patientunitstayid

# Set up all feature extraction tables--------------------------------------------------------------
lab_df <- fread(paste0(eicu_dir, "/lab.csv"))
lab_df <- lab_df[which(lab_df$patientunitstayid %in% unique(pids)), ]

resp_chart_df <- fread(paste0(eicu_dir, "/respiratoryCharting.csv"))
resp_chart_df <- resp_chart_df[which(resp_chart_df$patientunitstayid %in% unique(pids)), ]

mGCS_df <- fread(paste0(eicu_dir, "/all_mGCS_PE_NS.csv"))
mGCS_df <- mGCS_df[which(mGCS_df$patientunitstayid %in% unique(pids)), ]

apache_df <- fread(paste0(eicu_dir, "/apacheApsVar.csv"))
apache_df <- apache_df[which(apache_df$patientunitstayid %in% unique(pids)), ]

fluid_df <- fread(paste0(eicu_dir, "/intakeOutput.csv"))
fluid_df <- fluid_df[which(fluid_df$patientunitstayid %in% unique(pids)), ]

history_df <- fread(paste0(eicu_dir, "/pastHistory.csv"))
history_df <- history_df[which((history_df$patientunitstayid %in% unique(pids)) & (history_df$pasthistoryvaluetext %like% "CHF")), ]
patient_features$CHF <- 1 * (patient_features$patientunitstayid %in% history_df$patientunitstayid)

del_df <- fread(paste0(eicu_dir, "/carePlanGeneral.csv"))
del_df <- del_df[which((del_df$patientunitstayid %in% unique(pids)) & (del_df$cplitemvalue == "Delirious")),] %>% droplevels()
patient_features$delirium <- 1 * (patient_features$patientunitstayid %in% del_df$patientunitstayid)

temp_df <- readRDS(paste0(eicu_dir, "/temperature_nurse_charting.Rds"))
temp_df <- temp_df[which(temp_df$patientunitstayid %in% unique(pids)), ] %>% droplevels(.)
temp_df <- temp_df[which(temp_df$nursingchartcelltypevalname != "Temperature Location"),] %>% droplevels(.)
temp_df$nursingchartvalue <- as.numeric(as.character(temp_df$nursingchartvalue))

#separate out temperature (c) and temperature (f)
table(temp_df$nursingchartcelltypevalname)
F_temp = temp_df[which(temp_df$nursingchartcelltypevalname == "Temperature (F)"), ]
C_temp = temp_df[which(temp_df$nursingchartcelltypevalname == "Temperature (C)"), ]

F_temp$nursingchartvalue = ((F_temp$nursingchartvalue - 32) * (5/9))
temp_df = rbind(F_temp, C_temp) %>% distinct(.)

temp_df <- temp_df %>% dplyr::select(patientunitstayid, nursingchartoffset, nursingchartvalue)
colnames(temp_df)[2] <- "offset" #temp offset#
colnames(temp_df)[3] <- "value" #temp value in C#

## BEGIN TEMPERATURE FEATURE EXTRACTION -----------------------------------------------------------
print("Starting temp")

cores <- detectCores()
cl <- makeCluster(cores-10) #not to overload your computer
registerDoParallel(cl)

temp_final <- rbind()
source(paste0(main_dir, "/features/extract_temp.R"))

temp_final <- foreach (i=1:length(pids), .combine=rbind.fill) %dopar% {
#for (i in 1:length(pids)) {
  print(paste0("--------------", i, "--------------"))
  
  tempid <- pids[i]
  temp_row <- patient[patient$patientunitstayid == tempid, ]
  upper_hr <- temp_row$offset / 60
  mv_start_hr <- temp_row$start / 60
  lower_hr <- if(upper_hr - 6 < mv_start_hr) mv_start_hr else upper_hr - 6
  
  temp_features <- extract_temp(patientunitstayids = tempid, table_df = temp_df, lower_hr = lower_hr, upper_hr = upper_hr)
  
  #temp_final <- rbind.fill(temp_final, temp_features)
  temp_features
}

stopCluster(cl)
## END HERE

## BEGIN PTS FEATURE EXTRACTION -----------------------------------------------------------
print("Starting PTS")

# cores <- detectCores()
# cl <- makeCluster(cores-10) #not to overload your computer
# registerDoParallel(cl)
# 
source(paste0(main_dir, "/features/hanrepo/extract_PTS_time_domain.R"))
# 
# PTS_hr <- rbind()
library(data.table)
# 
# #PTS_hr <- foreach (i=1:length(pids), .combine=rbind.fill) %dopar% {
# for (i in 1:length(pids)) {
#   print(paste0("--------------", i, "--------------"))
#   
#   tempid <- pids[i]
#   temp_row <- patient[patient$patientunitstayid == tempid, ]
#   upper_hr <- temp_row$offset / 60
#   mv_start_hr <- temp_row$start / 60
#   lower_hr <- if(upper_hr - 6 < mv_start_hr) mv_start_hr else upper_hr - 6
#   
#   tempid <- as.data.frame(tempid)
#   tempid$lower <- lower_hr
#   tempid$upper <- upper_hr
#   colnames(tempid) <- c("patientunitstayid", "lower", "upper")
#   
#   just_hr = extract_PTS_time_domain(code_dir = code_source,
#                                     patientunitstayid_time = tempid,
#                                     eicu_dir = "~/scratch",
#                                     PTS_signal_type = "hr",
#                                     binwidth = 1)
#   
#   PTS_hr <- rbind.fill(PTS_hr, just_hr)
#   #just_hr
# }
# 
# print("Starting PTS 2")
# 
# PTS_all <- rbind()
# 
# #PTS_all <- foreach (i=1:length(pids), .combine=rbind.fill) %dopar% {
# for (i in 1:length(pids)) {
#   print(paste0("--------------", i, "--------------"))
#   
#   tempid <- pids[i]
#   temp_row <- patient[patient$patientunitstayid == tempid, ]
#   upper_hr <- temp_row$offset / 60
#   mv_start_hr <- temp_row$start / 60
#   lower_hr <- if(upper_hr - 6 < mv_start_hr) mv_start_hr else upper_hr - 6
#   
#   tempid <- as.data.frame(tempid)
#   tempid$lower <- lower_hr
#   tempid$upper <- upper_hr
#   colnames(tempid) <- c("patientunitstayid", "lower", "upper")
#   
#   all_signals = extract_PTS_time_domain(code_dir = code_source,
#                                         patientunitstayid_time = tempid,
#                                         eicu_dir = "~/scratch",
#                                         binwidth = 1)
#   
#   PTS_all <- rbind.fill(PTS_all, all_signals)
#   #all_signals
# }
# 
# print("Starting PTS 3")

PTS_all_default <- rbind()

#PTS_all_default <- foreach (i=1:length(pids), .combine=rbind.fill) %dopar% {
for (i in 1:length(pids)) {
  print(paste0("--------------", i, "--------------"))
  
  tempid <- pids[i]
  temp_row <- patient[patient$patientunitstayid == tempid, ]
  upper_hr <- temp_row$offset / 60
  mv_start_hr <- temp_row$start / 60
  lower_hr <- if(upper_hr - 6 < mv_start_hr) mv_start_hr else upper_hr - 6
  
  tempid <- as.data.frame(tempid)
  tempid$lower <- lower_hr
  tempid$upper <- upper_hr
  colnames(tempid) <- c("patientunitstayid", "lower", "upper")
  
  all_signals_default = extract_PTS_time_domain(code_dir = code_source,
                                                patientunitstayid_time = tempid,
                                                eicu_dir = "~/scratch",
                                                binwidth = 1)
  
  PTS_all_default <- rbind.fill(PTS_all_default, all_signals_default)
  #all_signals_default
}

stopCluster(cl)
## END HERE

## BEGIN APACHEVAR FEATURE EXTRACTION -----------------------------------------------------------
print("Starting apache")

cores <- detectCores()
cl <- makeCluster(cores-10) #not to overload your computer
registerDoParallel(cl)

apache_final <- rbind()
source(paste0(main_dir, "/features/extract_apache.R"))

apache_final <- foreach (i=1:length(pids), .combine=rbind.fill) %dopar% {
#for (i in 1:length(pids)) {
  print(paste0("--------------", i, "--------------"))
  
  tempid <- pids[i]
  temp_row <- patient[patient$patientunitstayid == tempid, ]
  
  apache_features <- extract_apache(patientunitstayids = tempid, table_df = apache_df)
  
  #apache_final <- rbind.fill(apache_final, apache_features)
  apache_features
}

stopCluster(cl)
## END HERE

## BEGIN FLUID BAL FEATURE EXTRACTION -----------------------------------------------------------
print("Starting fluid balance")

cores <- detectCores()
cl <- makeCluster(cores-10) #not to overload your computer
registerDoParallel(cl)

fluid_final <- rbind()
source(paste0(main_dir, "/features/extract_fluid.R"))

fluid_final <- foreach (i=1:length(pids), .combine=rbind.fill) %dopar% {
  #for (i in 1:length(pids)) {
  print(paste0("--------------", i, "--------------"))
  
  tempid <- pids[i]
  temp_row <- patient[patient$patientunitstayid == tempid, ]
  upper_hr <- temp_row$offset / 60
  mv_start_hr <- temp_row$start / 60
  lower_hr <- if(upper_hr - 6 < mv_start_hr) mv_start_hr else upper_hr - 6
  
  fluid_features <- extract_fluid(patientunitstayids = tempid, table_df = fluid_df, lower_hr = lower_hr, upper_hr = upper_hr)
  
  #fluid_final <- rbind.fill(fluid_final, fluid_features)
  fluid_features
}

stopCluster(cl)
## END HERE

print("Starting labs")

cores <- detectCores()
cl <- makeCluster(cores-1) #not to overload your computer
registerDoParallel(cl)

lab_final <- rbind()
source("~/scratch/cicm_extubation/features/hanrepo/extract_lab.R")
lab_final <- foreach (i=1:length(pids), .combine=rbind.fill) %dopar% {
  print(paste0("--------------", i, "--------------"))

  tempid <- pids[i]
  temp_row <- patient[patient$patientunitstayid == tempid, ]
  upper_hr <- temp_row$offset / 60
  mv_start_hr <- temp_row$start / 60
  lower_hr <- if(upper_hr - 6 < mv_start_hr) mv_start_hr else upper_hr - 6
  df <- as.data.frame(cbind(tempid, lower_hr, upper_hr))

  temp_lab <- extract_lab(code_dir = "~/scratch/cicm_extubation/features/hanrepo/", 
                          patientunitstayid_dataframe = df,
                          eicu_dir = "~/scratch/",
                          lab_table = lab_df, #optional
                          per_pid = TRUE)
  
  temp_lab
}

stopCluster(cl)
cores <- detectCores()
cl <- makeCluster(cores-1) #not to overload your computer
registerDoParallel(cl)

print("Lab extracted")

resp_chart_final <- rbind()
source("~/scratch/cicm_extubation/features/extract_respiratory_charting_v0.0.R")
resp_chart_final <- foreach (i=1:length(pids), .combine=rbind.fill) %dopar% {
  print(paste0("--------------", i, "--------------"))
  
  tempid <- pids[i]
  temp_row <- patient[patient$patientunitstayid == tempid, ]
  upper_hr <- temp_row$offset / 60
  mv_start_hr <- temp_row$start / 60
  lower_hr <- if(upper_hr - 6 < mv_start_hr) mv_start_hr else upper_hr - 6
  
  temp_resp_chart <- extract_respiratory_charting(patientunitstayids = tempid, table_df = resp_chart_df, lower_hr = lower_hr, upper_hr = upper_hr, per_pid_boolean = T, pre_stats = T, extras = T)
  
  temp_resp_chart
}

stopCluster(cl)

## START HERE
cores <- detectCores()
cl <- makeCluster(cores-1) #not to overload your computer
registerDoParallel(cl)

print("Resp extracted")

motor_final <- rbind()
motor_final <- foreach (i=1:length(pids), .combine=rbind.fill) %dopar% {
  #for (i in 1:length(pids)) {
  print(paste0("--------------", i, "--------------"))
  
  tempid <- pids[i]
  temp_row <- patient[patient$patientunitstayid == tempid, ]
  upper_hr <- temp_row$offset / 60
  lower_hr <- temp_row$start / 60
  
  table_df <- mGCS_df
  patientunitstayids <- tempid
  
  require(tidyverse)
  require(plotly)
  library(caret)
  library(doParallel)
  
  #### MOTOR SCORE FEATURE EXTRACTION PERFORMED HERE ###
  table_df <- table_df[which(table_df$patientunitstayid %in% patientunitstayids), ] %>% droplevels(.)
  table_df$value <- as.numeric(as.character(table_df$value))
  table_df <- table_df[which(table_df$offset > (lower_hr * 60) & table_df$offset < (upper_hr * 60)), ] %>% droplevels(.)
  
  motor_features <- as.data.frame(patientunitstayids)
  colnames(motor_features) <- c("patientunitstayid")
  id <- "patientunitstayid"
  tempchart <- table_df %>% droplevels(.)
  tempchart <- tempchart[complete.cases(tempchart), ]
  
  #last
  lastperpatient <- tempchart %>% group_by(patientunitstayid) %>% slice(which.max(offset))
  lastperpatient <- lastperpatient %>% dplyr::select(patientunitstayid, value) %>% droplevels()
  colnames(lastperpatient) <- c(id, "motor_score_last")
  
  #first
  firstperpatient <- tempchart %>% group_by(patientunitstayid) %>% slice(which.min(offset))
  firstperpatient <- firstperpatient %>% dplyr::select(patientunitstayid, value) %>% droplevels(.)
  colnames(firstperpatient) <- c(id, "motor_score_delta")
  firstperpatient$motor_score_delta <- (lastperpatient$motor_score_last - firstperpatient$motor_score_delta)
  
  firstperpatient <- as.data.frame(firstperpatient)
  lastperpatient <- as.data.frame(lastperpatient)
  
  combined <- cbind(firstperpatient, lastperpatient$motor_score_last)
  
  motor_features <- merge(motor_features, combined, by = "patientunitstayid", all = T)
  motor_features <- replace_infinite(motor_features, NA)
  motor_features <- replace_nan(motor_features, NA)
  
  #motor_final <- rbind.fill(motor_final, motor_features)
  motor_features
}

stopCluster(cl)
## END HERE

total_feature <- merge(patient_features,lab_final,by="patientunitstayid",all=TRUE)
total_feature <- merge(total_feature,resp_chart_final,by="patientunitstayid",all=TRUE)
total_feature <- merge(total_feature,motor_final,by="patientunitstayid",all=TRUE)
total_feature <- merge(total_feature,temp_final,by="patientunitstayid",all=TRUE)
total_feature <- merge(total_feature,apache_final,by="patientunitstayid",all=TRUE)
total_feature <- merge(total_feature,fluid_final,by="patientunitstayid",all=TRUE)
# total_feature <- merge(total_feature,PTS_hr,by="patientunitstayid",all=TRUE)
# total_feature <- merge(total_feature,PTS_all,by="patientunitstayid",all=TRUE)
total_feature <- merge(total_feature,PTS_all_default,by="patientunitstayid",all=TRUE)

# Impute mean impute missing values and remove empty columns
ft <- total_feature
ft <- ft[ , (colSums(is.na(ft)) < 0.4*nrow(ft))]
for (i in 1:ncol(ft)) {
  #print(colnames(ft)[i])
  ft[is.na(ft[,i]), i] <- mean(as.numeric(ft[,i]), na.rm = TRUE)
}
total_feature <- ft

# Adjust outcome label in label space
lb <- mvpids
expids$label = "extubated"
repids$label = "reintubated"
label <- rbind(expids, repids)
lb <- label %>% dplyr::select(patientunitstayid, label) %>% group_by(patientunitstayid)
lb <- lb %>% distinct(patientunitstayid, .keep_all=TRUE)
write.csv(resp_chart_final, "/home-1/aszewc1@jhu.edu/scratch/tst.csv", row.names = F)

dir.create("~/scratch/cicm_extubation/features/curr_features")
# write.csv(label, "curr_features/curr_label.csv", row.names = F)
write.csv(total_feature, "/home-1/aszewc1@jhu.edu/scratch/cicm_extubation/features/curr_features/curr_feature.csv", row.names = F)
write.csv(lb, "/home-1/aszewc1@jhu.edu/scratch/cicm_extubation/features/curr_features/curr_label.csv", row.names = F)
