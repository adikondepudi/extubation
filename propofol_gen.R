#Alexandra Szewc
#To create sample dataframe for reintubations including heart rate, medications, and resp rate

#Alex: 01/22/21 - Created this file to add respiratory value reporting
#Alex: 02/03/21 - Modifying how medication values are selected for: propofol only
#Alex: 02/03/21 - Adding selection criteria for other medications

library(tidyverse)
library(data.table)

##### define directories #####
#data_dir where eICU tables are located
data_dir <- "~/scratch"

#source_dir where functions to be sourced are located
source_dir <- "~/scratch/cicm_extubation/"
# source_dir <- "~/Medical_tutorial/Alexandra/cicm_extubation/"

#save_dir where code output is saved
save_dir <- "~/scratch/cicm_extubation/csv/"
# save_dir <- "~/Medical_tutorial/Alexandra/"

#loading in the new useful_functions.R directory should be correct
source(paste0(source_dir, "/useful_functions.R"))

#plot the timeline of TBI propofol. 
patient <- fread(paste0(data_dir, "/patient.csv"))
treatment <- fread(paste0(data_dir, "/treatment.csv"))
medication <- fread(paste0(data_dir, "/medication.csv"))
infusion_drug <- fread(paste0(data_dir, "/infusionDrug.csv"))
intakeoutput <- fread(paste0(data_dir, "/intakeOutput.csv"))

##########################Identifying Propofol#################################

treatment <- treatment[which(treatment$patientunitstayid %in% patient$patientunitstayid), ]
med <- medication[which(medication$patientunitstayid %in% patient$patientunitstayid), ] %>% droplevels(.)
inf_drug <- infusion_drug[which(infusion_drug$patientunitstayid %in% patient$patientunitstayid), ] %>% droplevels(.)
intake <- intakeoutput[which(intakeoutput$patientunitstayid %in% patient$patientunitstayid), ] %>% droplevels(.)

treatment_table <- as.data.frame(table(treatment$treatmentstring))
drugnames_med <- as.data.frame(table(med$drugname))
drugnames_inf <- as.data.frame(table(inf_drug$drugname))
intake_name <- as.data.frame(table(intake$celllabel))
intake_path <- as.data.frame(table(intake$cellpath))

intervention_p <- treatment %>% filter(str_detect(treatmentstring, regex("propofol" , ignore_case = T))) %>% droplevels(.)
intervention_e <- treatment %>% filter(str_detect(treatmentstring, regex("etomidate" , ignore_case = T))) %>% droplevels(.)
intervention_m <- treatment %>% filter(str_detect(treatmentstring, regex("midazolam" , ignore_case = T))) %>% droplevels(.)
intervention_k <- treatment %>% filter(str_detect(treatmentstring, regex("ketamine" , ignore_case = T))) %>% droplevels(.)
intervention_s <- treatment %>% filter(str_detect(treatmentstring, regex("succinylcholine" , ignore_case = T))) %>% droplevels(.)
intervention_r <- treatment %>% filter(str_detect(treatmentstring, regex("rocuronium" , ignore_case = T))) %>% droplevels(.)
intervention_v <- treatment %>% filter(str_detect(treatmentstring, regex("vecuronium" , ignore_case = T))) %>% droplevels(.)
intervention <- rbind(intervention_p, intervention_p, intervention_p,
                      intervention_p, intervention_p, intervention_p,
                      intervention_p)

int_med_p <- med %>% filter(str_detect(drugname, regex("propofol", ignore_case = T))) %>% droplevels(.)
int_med_e <- med %>% filter(str_detect(drugname, regex("etomidate", ignore_case = T))) %>% droplevels(.)
int_med_m <- med %>% filter(str_detect(drugname, regex("midazolam", ignore_case = T))) %>% droplevels(.)
int_med_k <- med %>% filter(str_detect(drugname, regex("ketamine", ignore_case = T))) %>% droplevels(.)
int_med_s <- med %>% filter(str_detect(drugname, regex("succinylcholine", ignore_case = T))) %>% droplevels(.)
int_med_r <- med %>% filter(str_detect(drugname, regex("rocuronium", ignore_case = T))) %>% droplevels(.)
int_med_v <- med %>% filter(str_detect(drugname, regex("vecuronium", ignore_case = T))) %>% droplevels(.)
int_med <- rbind(int_med_p, int_med_e, int_med_m, int_med_k, int_med_s, int_med_r, int_med_v)

int_inf_p <- inf_drug%>% filter(str_detect(drugname, regex("propofol", ignore_case = T))) %>% droplevels(.)
int_inf_e <- inf_drug%>% filter(str_detect(drugname, regex("etomidate", ignore_case = T))) %>% droplevels(.)
int_inf_m <- inf_drug%>% filter(str_detect(drugname, regex("midazolam", ignore_case = T))) %>% droplevels(.)
int_inf_k <- inf_drug%>% filter(str_detect(drugname, regex("ketamine", ignore_case = T))) %>% droplevels(.)
int_inf_s <- inf_drug%>% filter(str_detect(drugname, regex("succinylcholine", ignore_case = T))) %>% droplevels(.)
int_inf_r <- inf_drug%>% filter(str_detect(drugname, regex("rocuronium", ignore_case = T))) %>% droplevels(.)
int_inf_v <- inf_drug%>% filter(str_detect(drugname, regex("vecuronium", ignore_case = T))) %>% droplevels(.)
int_inf <- rbind(int_inf_p, int_inf_e, int_inf_m, int_inf_k, int_inf_s, int_inf_r, int_inf_v)

int_intake_label_p <- intake %>% filter(str_detect(celllabel, regex("propofol", ignore_case = T))) %>% droplevels(.)
int_intake_label_e <- intake %>% filter(str_detect(celllabel, regex("etomidate", ignore_case = T))) %>% droplevels(.)
int_intake_label_m <- intake %>% filter(str_detect(celllabel, regex("midazolam", ignore_case = T))) %>% droplevels(.)
int_intake_label_k <- intake %>% filter(str_detect(celllabel, regex("ketamine", ignore_case = T))) %>% droplevels(.)
int_intake_label_s <- intake %>% filter(str_detect(celllabel, regex("succinylcholine", ignore_case = T))) %>% droplevels(.)
int_intake_label_r <- intake %>% filter(str_detect(celllabel, regex("rocuronium", ignore_case = T))) %>% droplevels(.)
int_intake_label_v <- intake %>% filter(str_detect(celllabel, regex("vecuronium", ignore_case = T))) %>% droplevels(.)
int_intake_label <- rbind(int_intake_label_p, int_intake_label_e, int_intake_label_m,
                         int_intake_label_k, int_intake_label_s, int_intake_label_r,
                         int_intake_label_v)

int_intake_path_p <- intake %>% filter(str_detect(cellpath, regex("propofol", ignore_case = T))) %>% droplevels(.)
int_intake_path_e <- intake %>% filter(str_detect(cellpath, regex("etomidate", ignore_case = T))) %>% droplevels(.)
int_intake_path_m <- intake %>% filter(str_detect(cellpath, regex("midazolam", ignore_case = T))) %>% droplevels(.)
int_intake_path_k <- intake %>% filter(str_detect(cellpath, regex("ketamine", ignore_case = T))) %>% droplevels(.)
int_intake_path_s <- intake %>% filter(str_detect(cellpath, regex("succinylcholine", ignore_case = T))) %>% droplevels(.)
int_intake_path_r <- intake %>% filter(str_detect(cellpath, regex("rocuronium", ignore_case = T))) %>% droplevels(.)
int_intake_path_v <- intake %>% filter(str_detect(cellpath, regex("vecuronium", ignore_case = T))) %>% droplevels(.)
int_intake_path <- rbind(int_intake_path_p, int_intake_path_e, int_intake_path_m,
                         int_intake_path_k, int_intake_path_s, int_intake_path_r,
                         int_intake_path_v)

pids_intervention <- as.data.frame(unique(intervention$patientunitstayid))
colnames(pids_intervention) <- c("patientunitstayid")

unique(int_med$drughiclseqno)
med$drughiclseqno <- as.character(med$drughiclseqno)
int_med_hicl_propofol <- med %>% filter(str_detect(drughiclseqno, regex("4842", ignore_case = T))) %>% droplevels(.)
int_med_hicl_etomidate <- med %>% filter(str_detect(drughiclseqno, regex("1554", ignore_case = T))) %>% droplevels(.)
int_med_hicl_midazolam <- med %>% filter(str_detect(drughiclseqno, regex("1619", ignore_case = T))) %>% droplevels(.)
int_med_hicl_succinylcholine <- med %>% filter(str_detect(drughiclseqno, regex("1948", ignore_case = T))) %>% droplevels(.)
int_med_hicl_rocuronium <- med %>% filter(str_detect(drughiclseqno, regex("8963", ignore_case = T))) %>% droplevels(.)
int_med <- rbind(int_med, int_med_hicl_propofol, int_med_hicl_etomidate,
                 int_med_hicl_midazolam, int_med_hicl_succinylcholine,
                 int_med_hicl_rocuronium) %>% distinct()

int_intake <- rbind(int_intake_label, int_intake_path) %>% distinct(.)

drug_pids <- unique(c(unique(int_inf$patientunitstayid), unique(int_med$patientunitstayid), unique(int_intake$patientunitstayid)))
drug_pids <- as.data.frame(drug_pids)
colnames(drug_pids) <- "patientunitstayid"

pids_intervention <- rbind(pids_intervention, drug_pids)
pids_intervention <- pids_intervention %>% distinct(patientunitstayid)

length(unique(pids_intervention$patientunitstayid))

int_med_select <- int_med %>% select(patientunitstayid, drugstartoffset)
colnames(int_med_select)[2] <- "offset"
int_inf_select <- int_inf %>% select(patientunitstayid, infusionoffset)
colnames(int_inf_select)[2] <- "offset"
intervention_select <- intervention %>% select(patientunitstayid, treatmentoffset)
colnames(intervention_select)[2] <- "offset"
intake_select <- int_intake %>% select(patientunitstayid, intakeoutputoffset)
colnames(intake_select)[2] <- "offset"

combined <- rbind(int_med_select, int_inf_select, intervention_select, intake_select) %>% distinct()

############# SEARCHING FOR PROPOFOL #############

write.csv(combined, paste0(save_dir, "/allmeds_records.csv"), row.names = F)