#Alexandra Szewc
#To create sample dataframe for reintubations based on MIT dataframe

#Alex: 1/5/21 - ran MIT df (df_vent_events precursor) to display in same format as previous
#               sample identification dataframes
#Alex: 1/6/21 - debugged to include all pids from MIT dataframe in new format

library(tidyverse) #do not need to be in quotes
library(data.table) #oops
library(tictoc)
library(foreach)
library(doParallel)

##### define directories #####
#data_dir where eICU tables are located
data_dir <- "~/scratch/"
# data_dir <- "/storage/eICU/"

#source_dir where functions to be sourced are located
source_dir <- "~/scratch/cicm_extubation/"
# source_dir <- "~/Medical_tutorial/Alexandra/cicm_extubation/"

#save_dir where code output is saved
save_dir <- "~/scratch/cicm_extubation/csv/"
# save_dir <- "~/Medical_tutorial/Alexandra/"

#loading in the new useful_functions.R directory should be correct
source(paste0(source_dir, "/useful_functions.R"))

#Loading respiratoryCharting
chart <- read.eicu(data_dir, "respiratoryCharting")
mitdf <- read.eicu(data_dir, "MIT_df")

#identifying continuous intubation
## replaced below with one pipe below using data.table 
## runs in half the time but difference becomes more evident with larger operations. 
## applied to this to give an example of how to use it. I've left comments explaining. 

# old code for making preliminary PEEP dataframe
# tic()
# peep <- chart[which(chart$respchartvaluelabel == "PEEP"),] %>% droplevels()
# peep <- peep %>% dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% droplevels()
# peep <- peep %>% group_by(patientunitstayid) %>% arrange(patientunitstayid, respchartoffset)
# # peep_alternative <- peep[order(peep$patientunitstayid, peep$respchartoffset),]
# peep <- peep %>% group_by(patientunitstayid) %>% mutate(difference = respchartoffset -
#                                                           dplyr::lag(respchartoffset, default = respchartoffset[1]))
# toc() #3 seconds

###### THIS PART GAVE ERRORS
tic()
peep <- setDT(mitdf) %>% #make into data.table
  #.[c("patientunitstayid", "vent_start_hrs", "vent_stop_hrs")] %>%
  #^find peep and select columns of interest at the same time.
  setorderv(. , c("patientunitstayid", "vent_start_hrs")) %>% #set order by pid and vent start
  .[, difference := (vent_start_hrs - dplyr::lag(vent_stop_hrs, default = vent_stop_hrs[1])), , by = "patientunitstayid"]
#^ make a column named difference := (assignment operator into a new column) and assign the difference by patientunitstayid group.
toc() #2.887 seconds

mitdf$vent_start_hrs = mitdf$vent_start_hrs * 60
mitdf$vent_stop_hrs = mitdf$vent_stop_hrs * 60

#analysis of how many peep data points per patient
peep_row_count <- setDT(peep) %>% .[, row_count := .N, by = "patientunitstayid"]

#####  Make sample identification dataframe #####
#for (t in 1:10) {                #when generating several
#setting up parametes and pids list
#gap_length <- 300 + (30 * t)     #when generating several
gap_length <- 0 #assume all recordings are MV?
unique_pids <- unique(peep$patientunitstayid)

# subset each element to check for within reintubation gap
#        this is done to reduce df size during loop

# do we need this again? we just did this
peep <- mitdf[which(mitdf$patientunitstayid %in% unique_pids),] %>% 
  group_by(patientunitstayid) %>% 
  arrange(patientunitstayid, vent_start_hrs, vent_stop_hrs) %>% droplevels()

plateau <- chart[which(chart$respchartvaluelabel == "Plateau Pressure" &
                         chart$patientunitstayid %in% unique_pids),] %>% 
  dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% 
  group_by(patientunitstayid) %>% 
  arrange(patientunitstayid, respchartoffset) %>% droplevels()

TVIBW <- chart[which(chart$respchartvaluelabel == "TV/kg IBW" &
                       chart$patientunitstayid %in% unique_pids),] %>% 
  dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% 
  group_by(patientunitstayid) %>% 
  arrange(patientunitstayid, respchartoffset) %>% droplevels()


ETCO2 <- chart[which(chart$respchartvaluelabel == "ETCO2" &
                       chart$patientunitstayid %in% unique_pids),] %>% 
  dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% 
  group_by(patientunitstayid) %>% 
  arrange(patientunitstayid, respchartoffset) %>% droplevels()

#parallelization
core_count <- detectCores()
cl <- makeCluster(core_count - 3)
registerDoParallel(cl)
on.exit(stopCluster(cl))

tic()
# for (i in 1:length(unique_pids)) {
sample_identification_df <- foreach(i = 1:length(unique_pids), .combine=rbind) %dopar% {
  print(paste0(i, "/", length(unique_pids)))
  temp_id <- unique_pids[i]
  temp_peep <- peep[which(peep$patientunitstayid == temp_id), ]
  temp_plateau <- plateau[which(plateau$patientunitstayid == temp_id), ]
  temp_TVIBW <- TVIBW[which(TVIBW$patientunitstayid == temp_id), ]
  temp_Etco2 <- ETCO2[which(ETCO2$patientunitstayid == temp_id), ]
  
  temp_peep <- temp_peep[order(temp_peep$vent_start_hrs), ]
  
  mv_count <- 1 # which mv count for patient
  if (is.na(temp_peep$vent_stop_hrs[1])) {
    mv_stop <- temp_peep$ICU_Discharge_hrs[1] # some initial peep offset
    mv_last_stop <- temp_peep$ICU_Discharge_hrs[1] # Previously recorded peep offset
  }
  else {
    mv_stop <- temp_peep$vent_stop_hrs[1] # some initial peep offset
    mv_last_stop <- temp_peep$vent_stop_hrs[1] # Previously recorded peep offset
  }
  
  temp_sample_identification_df <- rbind()
  appended <- 0 #keep track of how many times appended to temp_sample_identification_df (row number)
  for (j in 1:nrow(temp_peep)) {
    
    mv_start <- temp_peep$vent_start_hrs[j]
    mv_stop <- temp_peep$vent_stop_hrs[j]
    if (is.na(mv_stop)) { mv_stop <- temp_peep$ICU_Discharge_hrs[j] }
    if (j == nrow(temp_peep)) { # handle end of record
      is_case <- FALSE #all last cases cannot be considered a reintubation. 
      temp_sample_identification_df <- rbind(temp_sample_identification_df, c(temp_id, is_case, mv_count,
                                                                              TRUE, mv_start,mv_stop,
                                                                              -1, -1, 0, 0, 0,
                                                                              paste0(temp_id, "_", mv_count),
                                                                              TRUE))
      appended <- appended + 1
      
      if (mv_count > 1 & appended > 1) { #added to change is_case to TRUE since technically we are interested in MV
        # that lead to reintubation 
        temp_sample_identification_df[appended - 1, 2] = TRUE #change previous row's is_case to TRUE
      }
    }
    else { # handle reintubation
      # first check if plateau, Fio2, TV/IBW all have 0 rows in the extubated interval
      exists_PP <- nrow(temp_plateau[which(temp_plateau$respchartoffset > mv_last_stop & temp_plateau$respchartoffset < mv_start),])
      exists_Etco2 <- nrow(temp_Etco2[which(temp_Etco2$respchartoffset > mv_last_stop & temp_Etco2$respchartoffset < mv_start),])
      exists_TVIBW <- nrow(temp_TVIBW[which(temp_TVIBW$respchartoffset > mv_last_stop & temp_TVIBW$respchartoffset < mv_start),])
      meets_criteria <- (exists_PP == 0 && exists_Etco2 == 0 && exists_TVIBW == 0) 
      is_case <- mv_count > 1
      
      #populate df
      temp_sample_identification_df <- rbind(temp_sample_identification_df, c(temp_id, is_case, mv_count,
                                                                              FALSE, mv_start, mv_stop,
                                                                              temp_peep$vent_start_hrs[j+1],
                                                                              temp_peep$vent_start_hrs[j+1] - mv_stop,
                                                                              exists_PP, exists_Etco2,
                                                                              exists_TVIBW,
                                                                              paste0(temp_id, "_", mv_count),
                                                                              meets_criteria))
      appended <- appended + 1
      
      if (is_case & appended > 1) {
        temp_sample_identification_df[appended - 1, 2] = TRUE
      }
      
      if (meets_criteria) {
        mv_count <- mv_count + 1
        # peep_val <- peep_now # (was) not sure if this should be conditional
      } # if reintubated, increment mv count for next recording
    }
    mv_last_stop <- mv_stop
  }
  
  temp_sample_identification_df
  
}

sample_identification_df <- as.data.frame(sample_identification_df, stringsAsFactors = FALSE)
names(sample_identification_df) <- c("patientunitstayid", "class", "MV_number", "is_last",
                                     "MV_start_offset", "MV_end_offset", "next_MV_start_offset",
                                     "reintubation_gap_duration", "exists_PP", "exists_Etco2",
                                     "exists_TVIBW", "MV_ID", "Meets_criteria")
toc()
stopCluster(cl)


# write.csv(sample_identification_df,paste0(save_dir, "/sample_df.csv"), row.names = F)
write.csv(sample_identification_df,paste0(save_dir, "/mit_sample.csv"), row.names = F)
#}                #when generating several
  