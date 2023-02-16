#Alexandra Szewc
#To create sample dataframe including heart rate and medication verifyers

#Alex: 1/21/21 - created file from sample_gen.R

library(tidyverse)
library(data.table)
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
save_dir <- "~/scratch/cicm_extubation/"
# save_dir <- "~/Medical_tutorial/Alexandra/"

#loading in the new useful_functions.R directory should be correct
source(paste0(source_dir, "/useful_functions.R"))

#Loading respiratoryCharting
hr <- read.eicu(data_dir, "hr_csv", "csv")
meds <- read.eicu(data_dir, "medication", "csv")
chart <- read.eicu(data_dir, "respiratoryCharting")
# ourdf <- read.csv("~/scratch/cicm_extubation/csv/merged_samples.csv", stringsAsFactors = F)

tic()
peep <- setDT(chart) %>% #make into data.table
  .[respchartvaluelabel == "PEEP", c("patientunitstayid", "respchartvaluelabel", "respchartvalue", "respchartoffset")] %>%
  #^find peep and select columns of interest at the same time.
  setorderv(. , c("patientunitstayid", "respchartoffset")) %>% #set order by pid and offset
  .[, difference := (respchartoffset - dplyr::lag(respchartoffset, default = respchartoffset[1])), , by = "patientunitstayid"]
#^ make a column named difference := (assignment operator into a new column) and assign the difference by patientunitstayid group.
toc() #1.5 seconds

#we require filtering of peep value for plausible values only and those with adequate number of data points -
#valid peep ranges used on a different project was from 0 to 40.
peep$respchartvalue <- as.numeric(as.character(peep$respchartvalue))
peep <- peep[!(peep$respchartvalue >= 40 | peep$respchartvalue <= 0), ]

#analysis of how many peep data points per patient
peep_row_count <- setDT(peep) %>% .[, row_count := .N, by = "patientunitstayid"]

#Remove those with 1 peep value
peep <- peep[!(peep$row_count == 1), ]

gap_length <- 300

unique_pids <- unique(peep$patientunitstayid)

# subset each element to check for within reintubation gap
#        this is done to reduce df size during loop

peep <- chart[which(chart$respchartvaluelabel == "PEEP" &
                      chart$patientunitstayid %in% unique_pids),] %>%
  dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>%
  group_by(patientunitstayid) %>%
  arrange(patientunitstayid, respchartoffset) %>% droplevels()

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

hr <- hr[which(!is.na(hr$value) &
                hr$patientunitstayid %in% unique_pids),] %>% 
  group_by(patientunitstayid) %>% 
  arrange(patientunitstayid, offset) %>% droplevels()

meds <- meds[which(meds$patientunitstayid %in% unique_pids &
                     (tolower(meds$drugname) %like% "propofol" |
                        tolower(meds$drugname) %like% "etomidate" |
                        tolower(meds$drugname) %like% "midazolam" |
                        tolower(meds$drugname) %like% "ketamine" |
                        tolower(meds$drugname) %like% "succinylcholine" |
                        tolower(meds$drugname) %like% "rocuronium" |
                        tolower(meds$drugname) %like% "vecuronium")),] %>% 
  dplyr::select(patientunitstayid, drugorderoffset, drugname) %>% 
  group_by(patientunitstayid) %>% 
  arrange(patientunitstayid, drugorderoffset) %>% droplevels()

print(nrow(ourdf))

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
  temp_hr <- hr[which(hr$patientunitstayid == temp_id), ]
  temp_med <- meds[which(meds$patientunitstayid == temp_id), ]
  
  temp_peep <- temp_peep[order(temp_peep$respchartoffset), ]
  
  mv_count <- 1 # which mv count for patient
  peep_val <- temp_peep$respchartoffset[1] # some initial peep offset
  peep_last <- temp_peep$respchartoffset[1] # Previously recorded peep offset
  
  temp_sample_identification_df <- rbind()
  appended <- 0 #keep track of how many times appended to temp_sample_identification_df (row number)
  for (j in 1:nrow(temp_peep)) {
    peep_now <- temp_peep$respchartoffset[j] # current iteration peep offset
    if (peep_now - peep_last > gap_length || j == nrow(temp_peep)) { # check if end of record or reintubation
      if (j == nrow(temp_peep) & peep_val != peep_now) { # handle end of record
        is_case <- FALSE #all last cases cannot be considered a reintubation. 
        temp_sample_identification_df <- rbind(temp_sample_identification_df, c(temp_id, is_case, mv_count,
                                                                                TRUE, peep_val, peep_now,
                                                                                -1, -1, 0, 0, 0, 0, 0,
                                                                                paste0(temp_id, "_", mv_count),
                                                                                TRUE))
        appended <- appended + 1
        
        if (mv_count > 1 & appended > 1) { #added to change is_case to TRUE since technically we are interested in MV
          # that lead to reintubation 
          temp_sample_identification_df[appended - 1, 2] = TRUE #change previous row's is_case to TRUE
        }
      }
      else if (peep_now - peep_last > gap_length & peep_val != peep_last) { # handle reintubation
        # first check if plateau, Fio2, TV/IBW all have 0 rows in the extubated interval
        exists_PP <- nrow(temp_plateau[which(temp_plateau$respchartoffset > peep_last & temp_plateau$respchartoffset < peep_now),])
        exists_Etco2 <- nrow(temp_Etco2[which(temp_Etco2$respchartoffset > peep_last & temp_Etco2$respchartoffset < peep_now),])
        exists_TVIBW <- nrow(temp_TVIBW[which(temp_TVIBW$respchartoffset > peep_last & temp_TVIBW$respchartoffset < peep_now),])
        exists_hr <- nrow(temp_hr[which(temp_hr$offset > peep_last & temp_hr$offset < peep_now),])
        exists_med <- nrow(temp_med[which(temp_med$drugorderoffset > peep_last & temp_med$drugorderoffset < peep_now),])
        meets_criteria <- (exists_PP == 0 && exists_Etco2 == 0 && exists_TVIBW == 0 && exists_med && exists_hr) 
        # && peep_val >= 0 ## HK: why the peep_val >= 0? Commented this out. 
        #we can determine how much of the MV is in the negative range later. 
        #This is so we can retain potential number of prior MV at any stage. Just becuase it is negative
        #does not make it a valid MV history. 
        is_case <- mv_count > 1
        
        #populate df
        temp_sample_identification_df <- rbind(temp_sample_identification_df, c(temp_id, is_case, mv_count,
                                                                                FALSE, peep_val, peep_last,
                                                                                peep_now, peep_now - peep_last,
                                                                                exists_PP, exists_Etco2,
                                                                                exists_TVIBW, exists_hr, exists_med,
                                                                                paste0(temp_id, "_", mv_count),
                                                                                meets_criteria))
        appended <- appended + 1
        
        if (is_case & appended > 1) {
          temp_sample_identification_df[appended - 1, 2] = TRUE
        }
        
        if (meets_criteria) {
          mv_count <- mv_count + 1
          peep_val <- peep_now # (was) not sure if this should be conditional
        } # if reintubated, increment mv count for next recording
      }
    }
    peep_last <- temp_peep$respchartoffset[j]
  }
  
  temp_sample_identification_df
  
}

sample_identification_df <- as.data.frame(sample_identification_df, stringsAsFactors = FALSE)
names(sample_identification_df) <- c("patientunitstayid", "class", "MV_number", "is_last",
                                     "MV_start_offset", "MV_end_offset", "next_MV_start_offset",
                                     "reintubation_gap_duration", "exists_PP", "exists_Etco2",
                                     "exists_TVIBW", "exists_hr", "exists_med", "MV_ID",
                                     "Meets_criteria")
toc()
stopCluster(cl)

write.csv(sample_identification_df,paste0(save_dir, "/medhr_df.csv"), row.names = F)