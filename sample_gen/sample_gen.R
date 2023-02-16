#Alexandra Szewc
#To create sample dataframe for reintubations

#Han: 11/18/20 - Started sample identification dataframe
#Alex: 11/19/20 - Continued building sample identification dataframe
#Alex: 11/24/20 - Made some edits to building of sample identification dataframe
#Han: 12/7/20 - QA: made data.table example suggestion for you to follow in future scripts
#     Parallelization of the main for loop using foreach and doparallel. It runs 25x faster 
#     Running on 24 core - if implemented on MARCC can request 24 cores. 
#       10k pids - non-parallel takes 305 seconds to run
#       10k pids - parallel - takes 11 seconds. 
#       all pids - parallel - takes 107 seconds. 
#     Implemented so you can learn how to do it AND now it's not the most time consuming task
#     to run and test for differnt reintubation gaps. (i think gap needs be between 400 and 500
#     based on what i saw in the results)
#Alex: 12/10/20 - look through pull request and complete TODOs; run on 300, 400, 500 gap length
#Alex: 12/11/20 - commented out plot generation and moved to plot.R; jpeg not running
#Alex: 12/12/20 - ran loop from 5 hour to 10 hour cutoff at 30 minute intervals
#Alex: 1/5/21 - ran MIT df (df_vent_events precursor) to display in same format as previous
#               sample identification dataframes; see mit_gen.R

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
save_dir <- "~/scratch/cicm_extubation/"
# save_dir <- "~/Medical_tutorial/Alexandra/"

#loading in the new useful_functions.R directory should be correct
source(paste0(source_dir, "/useful_functions.R"))

#Loading respiratoryCharting
chart <- read.eicu(data_dir, "respiratoryCharting")

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

########################### HK
#DONE: in sample identification_df creation loop, make it so that if start and end offsets are the exact same, you do not count
#that particular MV and move on even if gap in data to the next data point is > 300. This will solve the problem of a single data 
#point counting as a mechanical ventilation row because the difference between first and second data  point is > 300. 
###########################

#####  Make sample identification dataframe #####
#for (t in 1:11) {                #when generating several
#setting up parametes and pids list
#gap_length <- 300 + (30 * t-1)     #when generating several
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

# #empty initialized df
# sample_identification_df <- rbind() #things will be appended to this iteratively. 
# 
# tic()
# for (i in 1:length(unique_pids)) {
#   print(paste0(i, "/", length(unique_pids)))
#   temp_id <- unique_pids[i]
#   temp_peep <- peep[which(peep$patientunitstayid == temp_id), ]
#   temp_plateau <- plateau[which(plateau$patientunitstayid == temp_id), ]
#   temp_TVIBW <- TVIBW[which(TVIBW$patientunitstayid == temp_id), ]
#   temp_Etco2 <- ETCO2[which(ETCO2$patientunitstayid == temp_id), ]
#   
#   temp_peep <- temp_peep[order(temp_peep$respchartoffset), ]
#   
#   mv_count <- 1 # which mv count for patient
#   peep_val <- temp_peep$respchartoffset[1] # some initial peep offset
#   peep_last <- temp_peep$respchartoffset[1] # Previously recorded peep offset
#   for (j in 1:nrow(temp_peep)) {
#     peep_now <- temp_peep$respchartoffset[j] # current iteration peep offset
#     if (peep_now - peep_last > gap_length || j == nrow(temp_peep)) { # check if end of record or reintubation
#       if (j == nrow(temp_peep)) { # handle end of record
#         is_case <- mv_count > 1
#         sample_identification_df <- rbind(sample_identification_df, c(temp_id, is_case, mv_count, TRUE, peep_val, peep_now,
#                                                                       -1, -1, 0, 0, 0, paste0(temp_id, "_", mv_count),
#                                                                       TRUE))
#       }
#       else if (peep_now - peep_last > gap_length) { # handle reintubation
#         # first check if plateau, Fio2, TV/IBW all have 0 rows in the extubated interval
#         exists_PP <- nrow(temp_plateau[which(temp_plateau$respchartoffset > peep_last & temp_plateau$respchartoffset < peep_now),])
#         exists_Etco2 <- nrow(temp_Etco2[which(temp_Etco2$respchartoffset > peep_last & temp_Etco2$respchartoffset < peep_now),])
#         exists_TVIBW <- nrow(temp_TVIBW[which(temp_TVIBW$respchartoffset > peep_last & temp_TVIBW$respchartoffset < peep_now),])
#         meets_criteria <- (exists_PP == 0 && exists_Etco2 == 0 && exists_TVIBW == 0) 
#                                         # && peep_val >= 0 ## HK: why the peep_val >= 0? Commented this out. 
#                                         #we can determine how much of the MV is in the negative range later. 
#                                         #This is so we can retain potential number of prior MV at any stage. Just becuase it is negative
#                                         #does not make it a valid MV history. 
#         is_case <- mv_count > 1
#         
#         #populate df
#         sample_identification_df <- rbind(sample_identification_df, c(temp_id, is_case, mv_count, FALSE, peep_val, peep_last,
#                                                                       peep_now, peep_now - peep_last, exists_PP,
#                                                                       exists_Etco2, exists_TVIBW, paste0(temp_id, "_", mv_count),
#                                                                       meets_criteria))
#         if (meets_criteria) {
#           mv_count <- mv_count + 1
#           peep_val <- peep_now # not sure if this should be conditional
#         } # if reintubated, increment mv count for next recording
#       }
#     }
#     peep_last <- temp_peep$respchartoffset[j]
#   }
# }
# 
# sample_identification_df <- as.data.frame(sample_identification_df)
# names(sample_identification_df) <- c("patientunitstayid", "class", "MV_number", "is_last",
#                                      "MV_start_offset", "MV_end_offset", "next_MV_start_offset",
#                                      "reintubation_gap_duration", "exists_PP", "exists_Etco2",
#                                      "exists_TVIBW", "MV_ID", "Meets_criteria")
# toc() #305.978 sec elapsed for 10k pids

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
                                                                                -1, -1, 0, 0, 0,
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
        meets_criteria <- (exists_PP == 0 && exists_Etco2 == 0 && exists_TVIBW == 0) 
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
                                                                                exists_TVIBW,
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
                                     "exists_TVIBW", "MV_ID", "Meets_criteria")
toc()
stopCluster(cl)


# write.csv(sample_identification_df,paste0(save_dir, "/sample_df.csv"), row.names = F)
write.csv(sample_identification_df,paste0(save_dir, "/", gap_length, "_cut.csv"), row.names = F)
#}                #when generating several
# #HK: Plots
# df <- read.csv(paste0(save_dir, "/sample_df.csv"), stringsAsFactors = F)
# df$reintubation_gap_duration_hr <- df$reintubation_gap_duration/60
# 
# #remove False meets criteria
# #remove MV_number > 5 #up for discussion
# df <- df[which(df$MV_number <= 5), ]
# df <- df[which(df$Meets_criteria), ]
# 
# ggplot(data = df, aes(reintubation_gap_duration_hr, group = as.factor(as.character(MV_number)), fill = as.factor(as.character(MV_number)))) + geom_histogram(binwidth = 1, position=position_nudge(x = 0.5), alpha = 0.5) +
#   geom_vline(xintercept = 300/60, color = "red") +
#   xlim(0,24) + labs(fill = "MV number") +
#   ggtitle(paste0("Reintubated MVs up to 4 reintubations (5 MV)\nReintubation if MV gap greater than ", gap_length/60, " hours"))
# 
# ggsave(paste0(save_dir, "/reintubation_stratified.jpeg"), dpi = 300, width = 6, height = 4)
# 
# ggplot(data = df, aes(reintubation_gap_duration_hr, group = as.factor(as.character(class)), fill = as.factor(as.character(class)))) + 
#   geom_histogram(binwidth = 1, position=position_nudge(x = 0.5), alpha = 0.5) +
#   geom_vline(xintercept = 300/60, color = "red") +
#   xlim(-10,24) + labs(fill = "MV number") +
#   ggtitle(paste0("Reintubation gaps vs non-reintubated MV counts\nReintubation if MV gap greater than ", gap_length/60, " hours\n
#                  Potential Cases vs Controls"))
# 
# ggsave(paste0(save_dir, "/reintubation_class_stratified.jpeg"), dpi = 300, width = 6, height = 4)
# 
# df$mv_duration <- d(f$MV_end_offset - df$MV_start_offset)/60
# 
# ggplot(data = df, aes(mv_duration, group = as.factor(as.character(class)), fill = as.factor(as.character(class)))) + 
#   geom_histogram(binwidth = 1, position=position_nudge(x = 0.5), alpha = 0.5) +
#   xlim(-10,100) + labs(fill = "MV number") +
#   ggtitle(paste0("MV durations of MVs leading to reintubations vs \nMVs NOT leading to reintubations\nReintubation if MV gap greater than ", gap_length/60, " hours\n
#                  MV duration of Potential Cases vs Controls\n
#                  FIXES ERROR"))
# 
# ggsave(paste0(save_dir, "/reintubation_duration_stratified.jpeg"), dpi = 300, width = 6, height = 4)
# #HK ^above plots shows issues where many patients have only 1 peep value? - why i added the task of removing those that 
# #generate MV durations of 0. not reliable. 

