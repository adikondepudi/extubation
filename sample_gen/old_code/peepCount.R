#Alexandra Szewc
#To assess continuity of PEEP values in respiratoryCharting

#Alex: 11/5/20 - Creating a Histogram of differences between PEEP offset vals
#Han: 11/18/20 - Started sample identification dataframe
#Alex: 11/19/20 - Continued building sample identification dataframe
#Alex: 11/24/20 - Made some edits to building of sample identification dataframe
#Han: 12/7/20 - looks like this is a repeat of sample_gen.R - make a separate folder to dump outdated code into. 

library("tidyverse")
require("data.table")

#loading in the new useful_functions.R directory should be correct
source("~/scratch/cicm_extubation/useful_functions.R")

#main_dir where eICU tables are located
main_dir <- "~/scratch/"

chart <- read.eicu(main_dir, "respiratoryCharting")

#identifying continuous intubation
peep <- chart[which(chart$respchartvaluelabel == "PEEP"),] %>% droplevels()
peep <- peep %>% dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% droplevels()
peep <- peep %>% group_by(patientunitstayid) %>% arrange(patientunitstayid, respchartoffset)
# peep_alternative <- peep[order(peep$patientunitstayid, peep$respchartoffset),]
peep <- peep %>% group_by(patientunitstayid) %>% mutate(difference = respchartoffset -
                                                          dplyr::lag(respchartoffset, default = respchartoffset[1]))
# View(peep)
gap_length = 300
reintubated <- peep[which(peep$difference > gap_length),]
never_reintubated <- peep[!(peep$patientunitstayid %in% reintubated$patientunitstayid),]

#####  Make sample identification dataframe #####
unique_pids <- unique(peep$patientunitstayid)

sample_identification_df <- rbind() #things will be appended to this iteratively. 

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


for (i in 1:10) { # length(unique_pids) for 10
  print(paste0(i, "/", length(unique_pids)))
  temp_id <- unique_pids[i]
  temp_peep <- peep[which(peep$patientunitstayid == temp_id), ]
  temp_plateau <- plateau[which(plateau$patientunitstayid == temp_id), ]
  temp_TVIBW <- TVIBW[which(TVIBW$patientunitstayid == temp_id), ]
  temp_Etco2 <- ETCO2[which(ETCO2$patientunitstayid == temp_id), ]
  
  temp_peep <- temp_peep[order(temp_peep$respchartoffset), ]
  
  mv_count <- 1 # which mv count for patient
  peep_val <- temp_peep$respchartoffset[1] # some initial peep val
  peep_last <- temp_peep$respchartoffset[1] # last recorded peep val
  for (j in 1:nrow(temp_peep)) {
    peep_now <-  temp_peep$respchartoffset[j] # current iteration peep val
    if (peep_now - peep_last > gap_length || j == nrow(temp_peep)) { # check if end of record or reintubation
      if (j == nrow(temp_peep)) { # handle end of record
        is_case <- mv_count > 1
        sample_identification_df <- rbind(sample_identification_df, c(temp_id, is_case, mv_count, TRUE, peep_val, peep_now,
                                                                      -1, -1, 0, 0, 0, paste0(temp_id, "_", mv_count),
                                                                      TRUE))
      }
      else if (peep_now - peep_last > gap_length) { # handle reintubation
        # first check if plateau, Fio2, TV/IBW all have 0 rows in the extubated interval
        exists_PP <- nrow(temp_plateau[which(temp_plateau$respchartoffset > peep_last & temp_plateau$respchartoffset < peep_now),])
        exists_Etco2 <- nrow(temp_Etco2[which(temp_Etco2$respchartoffset > peep_last & temp_Etco2$respchartoffset < peep_now),])
        exists_TVIBW <- nrow(temp_TVIBW[which(temp_TVIBW$respchartoffset > peep_last & temp_TVIBW$respchartoffset < peep_now),])
        meets_criteria <- (exists_PP == 0 && exists_Etco2 == 0 && exists_TVIBW == 0) && peep_val >= 0
        is_case <- mv_count > 1
        
        #populate df
        sample_identification_df <- rbind(sample_identification_df, c(temp_id, is_case, mv_count, FALSE, peep_val, peep_last,
                                                                      peep_now, peep_now - peep_last, exists_PP,
                                                                      exists_Etco2, exists_TVIBW, paste0(temp_id, "_", mv_count),
                                                                      meets_criteria))
        if (meets_criteria) {
          mv_count <- mv_count + 1
          peep_val <- peep_now # not sure if this should be conditional
        } # if reintubated, increment mv count for next recording
      }
    }
    peep_last <- temp_peep$respchartoffset[j]
  }
}

sample_identification_df <- as.data.frame(sample_identification_df)
names(sample_identification_df) <- c("patientunitstayid", "class", "MV_number", "is_last",
                                     "MV_start_offset", "MV_end_offset", "next_MV_start_offset",
                                     "reintubation_gap_duration", "exists_PP", "exists_Etco2",
                                     "exists_TVIBW", "MV_ID", "Meets_criteria")

# Checking other MV variables
plateau <- chart[which(chart$respchartvaluelabel == "Plateau Pressure"),] %>% droplevels()
plateau <- plateau[(plateau$patientunitstayid %in% peep$patientunitstayid),] %>% droplevels()
plateau <- plateau %>% dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% droplevels()
plateau <- plateau %>% group_by(patientunitstayid) %>% arrange(patientunitstayid, respchartoffset)

TVIBW <- chart[which(chart$respchartvaluelabel == "TV/kg IBW"),] %>% droplevels()
TVIBW <- TVIBW[(TVIBW$patientunitstayid %in% peep$patientunitstayid),] %>% droplevels()
TVIBW <- TVIBW %>% dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% droplevels()
TVIBW <- TVIBW %>% group_by(patientunitstayid) %>% arrange(patientunitstayid, respchartoffset)

TVset <- chart[which(chart$respchartvaluelabel == "Tidal Volume (set)"),] %>% droplevels()
TVset <- TVset[(TVset$patientunitstayid %in% peep$patientunitstayid),] %>% droplevels()
TVset <- TVset %>% dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% droplevels()
TVset <- TVset %>% group_by(patientunitstayid) %>% arrange(patientunitstayid, respchartoffset)

FIO2 <- chart[which(chart$respchartvaluelabel == "FiO2"),] %>% droplevels()
FIO2 <- FIO2[(FIO2$patientunitstayid %in% peep$patientunitstayid),] %>% droplevels()
FIO2 <- FIO2 %>% dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% droplevels()
FIO2 <- FIO2 %>% group_by(patientunitstayid) %>% arrange(patientunitstayid, respchartoffset)

ETCO2 <- chart[which(chart$respchartvaluelabel == "ETCO2" | chart$respchartvaluelabel == "EtCO2"),] %>% droplevels()
ETCO2 <- ETCO2[(ETCO2$patientunitstayid %in% peep$patientunitstayid),] %>% droplevels()
ETCO2 <- ETCO2 %>% dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% droplevels()
ETCO2 <- ETCO2 %>% group_by(patientunitstayid) %>% arrange(patientunitstayid, respchartoffset)

x <- c(1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5)
y <- c(0, 10, 20, 0, 20, 40, 0, 10, 50, 0, 10, 80, 0, 10, 100)
df <- as.data.frame(cbind(x, y))
df <- df %>% group_by(x) %>% mutate(diff = y -
                                        dplyr::lag(y, default = y[1]))

write.csv(sample_identification_df,"/home-1/aszewc1@jhu.edu/scratch/cicm_extubation/sample_df.csv")
