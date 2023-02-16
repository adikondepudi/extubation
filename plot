#Alexandra Szewc
#To create sample dataframe for reintubations based on MIT dataframe

#Alex: 1/6/21 - created code to show Patient MV intervals

library(tidyverse) #do not need to be in quotes
library(data.table) #oops
library(tictoc)
library(foreach)
library(doParallel)

##### define directories #####
#data_dir where eICU tables are located
# data_dir <- "~/scratch/"
data_dir <- "/storage/eICU/"
df_dir <- "~/Downloads/"

# source_dir where functions to be sourced are located
# source_dir <- "~/scratch/cicm_extubation/"
source_dir <- "~/Medical_tutorial/Alexandra/cicm_extubation/"
# source_dir <- "~/Downloads/"

#save_dir where code output is saved
# save_dir <- "~/scratch/cicm_extubation/plots/"
save_dir <- "~/Medical_tutorial/Alexandra/plots/"
dir.create(save_dir)

#loading in the new useful_functions.R directory should be correct
source(paste0(source_dir, "/useful_functions.R"))

chart <- read.eicu(data_dir, "respiratoryCharting", "csv")
resp <- read.eicu(data_dir, "resp_csv", "csv")
df <- read.csv(paste0(df_dir, "300_cut.csv"), stringsAsFactors = F)
mit <- read.csv(paste0(df_dir, "mit_sample.csv"), stringsAsFactors = F)
unique_pids <- unique(df[which(df$patientunitstayid %in% mit$patientunitstayid),])

# chart$respchartoffset <- chart$respchartoffset/60
chart$respchartvalue <- as.numeric(chart$respchartvalue)
chart <- chart[which(complete.cases(chart$respchartvalue)), ]
table(is.na(chart$respchartvalue))
# 
# plateau <- chart[which(chart$respchartvaluelabel == "Plateau Pressure" &
#                          chart$patientunitstayid %in% unique_pids$patientunitstayid),] %>% 
#   dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% 
#   group_by(patientunitstayid) %>% 
#   arrange(patientunitstayid, respchartoffset) %>% droplevels()
# 
# plateau$respchartvalue <- (plateau$respchartvalue - min(plateau$respchartvalue)) / (max(plateau$respchartvalue) - min(plateau$respchartvalue)) + 0.4
# 
# TVIBW <- chart[which(chart$respchartvaluelabel == "TV/kg IBW" &
#                        chart$patientunitstayid %in% unique_pids$patientunitstayid),] %>% 
#   dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% 
#   group_by(patientunitstayid) %>% 
#   arrange(patientunitstayid, respchartoffset) %>% droplevels()
# 
# TVIBW$respchartvalue <- (TVIBW$respchartvalue - min(TVIBW$respchartvalue)) / (max(TVIBW$respchartvalue) - min(TVIBW$respchartvalue)) + 0.6
# 
# 
# ETCO2 <- chart[which(chart$respchartvaluelabel == "ETCO2" &
#                        chart$patientunitstayid %in% unique_pids$patientunitstayid),] %>% 
#   dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% 
#   group_by(patientunitstayid) %>% 
#   arrange(patientunitstayid, respchartoffset) %>% droplevels()
# 
# ETCO2$respchartvalue <- (ETCO2$respchartvalue - min(ETCO2$respchartvalue)) / (max(ETCO2$respchartvalue) - min(ETCO2$respchartvalue)) + 0.2

rate <- resp[which(resp$patientunitstayid %in% unique_pids$patientunitstayid),] %>% 
  group_by(patientunitstayid) %>% 
  arrange(patientunitstayid, offset) %>% droplevels()

PEEP <- chart[which(chart$respchartvaluelabel == "PEEP" &
                       chart$patientunitstayid %in% unique_pids$patientunitstayid),] %>%
  dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>%
  group_by(patientunitstayid) %>%
  arrange(patientunitstayid, respchartoffset) %>% droplevels()

PEEP$respchartvalue <- (PEEP$respchartvalue - min(PEEP$respchartvalue)) / (max(PEEP$respchartvalue) - min(PEEP$respchartvalue))
colnames(PEEP)[4] <- "offset"
colnames(PEEP)[3] <- "value"

PEEP <- PEEP %>% dplyr::select(patientunitstayid, value, offset)

# fio2 <- chart[which(chart$respchartvaluelabel == "FiO2" &
#                       chart$patientunitstayid %in% unique_pids$patientunitstayid),] %>% 
#   dplyr::select(patientunitstayid, respchartvaluelabel, respchartvalue, respchartoffset) %>% 
#   group_by(patientunitstayid) %>% 
#   arrange(patientunitstayid, respchartoffset) %>% droplevels()
# 
# fio2$respchartvalue <- (fio2$respchartvalue - min(fio2$respchartvalue)) / (max(fio2$respchartvalue) - min(fio2$respchartvalue)) + 0.2

pids <- unique(unique_pids$patientunitstayid)

# for (i in 1:length(pids)) {
for (i in 1:100) {
  
  print(paste0(i, "/", length(pids)))
  
  pid <- pids[i]
  patient <- df[which(df$patientunitstayid == pid),]
  patientmit <- mit[which(mit$patientunitstayid == pid),]
  
  temp_peep <- PEEP[which(PEEP$patientunitstayid == pid), ]
  temp_peep$type <- "peep"
  
  # temp_fio2 <- fio2[which(fio2$patientunitstayid == pid), ]
  # temp_fio2$type <- "fio2"

  # temp_plateau <- plateau[which(plateau$patientunitstayid == pid), ]
  # temp_plateau$type <- "plateau"
  # temp_TVIBW <- TVIBW[which(TVIBW$patientunitstayid == pid), ]
  # temp_TVIBW$type <- "TV/IBW"
  # temp_ETCO2 <- ETCO2[which(ETCO2$patientunitstayid == pid), ]
  # temp_ETCO2$type <- "ETCO2"
  
  temp_resp <- rate[which(rate$patientunitstayid == pid), ]
  temp_resp$type <- "respiratory rate"
  
  settings <- rbind(temp_resp,
                    temp_peep)
                    # temp_fio2, 
                    # temp_plateau, temp_TVIBW, temp_ETCO2)

  P <- ggplot() +
    geom_point(data = settings, aes(x = offset/60, y = value, group = as.factor(type), color = as.factor(type)), size = 0.5) +
    geom_segment(data = patientmit, aes(x = MV_start_offset/60, y = 1, xend = MV_end_offset, yend = 1, colour = "MIT work", group = as.factor("MIT"))) +
    geom_segment(data = patient, aes(x = MV_start_offset/60, y = 1.2, xend = MV_end_offset/60, yend = 1.2, colour = "our work", group = as.factor("respiratoryCharting"))) +
    geom_vline(data = patient, aes(xintercept = MV_start_offset/60), alpha = 0.3, size = 0.2, color = "green") +
    geom_vline(data = patient, aes(xintercept = MV_end_offset/60), alpha = 0.3, size = 0.2, color = "red") +
    xlab("time (hour)") +
    ylab("values") +
    ggtitle(paste0("pid: ", pid, " -- MV storyboard")) +
    theme(legend.title = element_blank())
  
  ggsave(plot = P, paste0(save_dir, "/", pid,"_resp.png"), dpi = 300, width = 15, height = 8)
  
}
  