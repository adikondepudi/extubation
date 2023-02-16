#Alexandra Szewc
#To create sample dataframe for reintubations based on MIT dataframe

#Alex: 1/7/21 - wrote code to create merged dataframe with MIT and respiratoryCharting data overlap

library(tidyverse) #do not need to be in quotes
library(data.table) #oops
library(tictoc)
library(foreach)
library(doParallel)

##### define directories #####
#data_dir where eICU tables are located
data_dir <- "~/scratch/cicm_extubation/csv/"
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
ourdf <- read.csv(paste0(data_dir, "/backup.csv"), stringsAsFactors = F)
mitdf <- read.csv(paste0(save_dir, "mit_sample.csv"), stringsAsFactors = F)

# mit minus our table when relative to our df

tic()
ourdf <- setDT(ourdf) %>% #make into data.table
  setorderv(. , c("patientunitstayid", "MV_start_offset")) %>% #set order by pid and vent start
  .[, resp_ref_overlap := (-1), , by = "patientunitstayid"] %>%
  .[, mit_ref_overlap := (-1), , by = "patientunitstayid"] %>%
  .[, MV_start_diff := (-1), , by = "patientunitstayid"] %>%
  .[, MV_end_diff := (-1), , by = "patientunitstayid"]
toc() #0.242 seconds

unique_pids <- unique(ourdf$patientunitstayid)

# subset each element to check for within reintubation gap
#        this is done to reduce df size during loop

mitdf <- mitdf[which(mitdf$patientunitstayid %in% unique_pids),] %>% 
  group_by(patientunitstayid) %>% 
  arrange(patientunitstayid, MV_start_offset, MV_end_offset) %>% droplevels()

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
  temp_df <- ourdf[which(ourdf$patientunitstayid == temp_id), ]
  temp_mit <- mitdf[which(mitdf$patientunitstayid == temp_id), ]
  
  temp_df <- temp_df[order(temp_df$MV_start_offset), ]
  
  temp_sample_identification_df <- rbind()
  
  count <- 1
  for (j in 1:nrow(temp_df)) {
    if (count < nrow(temp_mit) & temp_df$MV_start_offset[j] > temp_mit$MV_end_offset[count]) {
      count <- count + 1
    }
    
    if (count > nrow(temp_mit)) {
      # start_diff <- temp_mit$MV_start_offset[count] - temp_df$MV_start_offset[j]
      # end_diff <- temp_mit$MV_end_offset[count] - temp_df$MV_end_offset[j]
      # temp_df$MV_start_diff[j] <- start_diff
      # temp_df$MV_end_diff[j] <- end_diff
      temp_sample_identification_df <- rbind(temp_sample_identification_df, temp_df[j])
      next
    }
    
    start_diff <- temp_mit$MV_start_offset[count] - temp_df$MV_start_offset[j]
    end_diff <- temp_mit$MV_end_offset[count] - temp_df$MV_end_offset[j]

    if (start_diff < 0) { # if our start happens after MIT start
      if (end_diff < 0) { # if our end happens after MIT end
        temp_df$resp_ref_overlap[j] <- (temp_mit$MV_end_offset[count] - temp_df$MV_start_offset[j]) / 
                                    (temp_df$MV_end_offset[j] - temp_df$MV_start_offset[j])
        temp_df$mit_ref_overlap[j] <- (temp_mit$MV_end_offset[count] - temp_df$MV_start_offset[j]) / 
                                   (temp_mit$MV_end_offset[count] - temp_mit$MV_start_offset[count])
      }
      else { # if our end happens before MIT end
        temp_df$resp_ref_overlap[j] <- (temp_df$MV_end_offset[j] - temp_df$MV_start_offset[j]) / 
                                    (temp_df$MV_end_offset[j] - temp_df$MV_start_offset[j])
        temp_df$mit_ref_overlap[j] <- (temp_df$MV_end_offset[j] - temp_df$MV_start_offset[j]) / 
                                   (temp_mit$MV_end_offset[count] - temp_mit$MV_start_offset[count])
      }
    }
    else { # if our start happens before MIT start
      if (end_diff < 0) { # if our end happens after MIT end
        temp_df$resp_ref_overlap[j] <- (temp_mit$MV_end_offset[count] - temp_mit$MV_start_offset[count]) / 
                                    (temp_df$MV_end_offset[j] - temp_df$MV_start_offset[j])
        temp_df$mit_ref_overlap[j] <- (temp_mit$MV_end_offset[count] - temp_mit$MV_start_offset[count]) / 
                                   (temp_mit$MV_end_offset[count] - temp_mit$MV_start_offset[count])
      }
      else { # if our end happens before MIT end
        temp_df$resp_ref_overlap[j] <- (temp_df$MV_end_offset[j] - temp_mit$MV_start_offset[count]) / 
                                    (temp_df$MV_end_offset[j] - temp_df$MV_start_offset[j])
        temp_df$mit_ref_overlap[j] <- (temp_df$MV_end_offset[j] - temp_mit$MV_start_offset[count]) / 
                                   (temp_mit$MV_end_offset[count] - temp_mit$MV_start_offset[count])
      }
    }
    
    temp_df$MV_start_diff[j] <- start_diff
    temp_df$MV_end_diff[j] <- end_diff
    
    # Possible error here or at other rbind (based on StackOverflow)
    # Error Message:
    # Error in { : task 2 failed - "names do not match previous names"
    # Calls: %dopar% -> <Anonymous>
    # Execution halted
    temp_sample_identification_df <- rbind(temp_sample_identification_df, temp_df[j])
  }
  
  temp_sample_identification_df
  
}

sample_identification_df <- as.data.frame(sample_identification_df, stringsAsFactors = FALSE)


toc()
stopCluster(cl)


# write.csv(sample_identification_df,paste0(save_dir, "/sample_df.csv"), row.names = F)
write.csv(sample_identification_df,paste0(save_dir, "/merged_samples.csv"), row.names = F)
#}                #when generating several
  
