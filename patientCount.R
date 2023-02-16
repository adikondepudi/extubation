#Alexandra Szewc
#Counts patients who are mechanically ventilated according each of the eICU databases

#Update date: (this reflects GITHUB push date ideally so it's easy to keep track)
#Han: 10/8/20 - Checked code and looked through getting all 8000 pids similar to df_vent_events
#Alex: 10/15/20 - Tried to find similar results for df_vent_event.csv
#Alex: 10/22/20 - Finding indtubated and extubated patiens in carePlan and treatment
#Alex: 10/23/20 - Found way to count number of times patients who were extubated were intubated
#Alex: 11/3/20 - Finding continuously intubated and extubated and then reintubated patients in respiratoryCharting
#Alex: 11/20/20 - Began comparison between sample dataframe and df_vent_event + eICU tables
#Han: 12/07/20 - QA check - Nothing in particular jumps out at me as incorrect.
#Alex: 1/5/21 - Adding comparison between MIT (df_vent_events precursor) dataframe and our sample dfs (different cutoffs)

library("tidyverse")
require("data.table")

#loading in the new useful_functions.R directory should be correct
source("~/scratch/cicm_extubation/useful_functions.R")

#main_dir where eICU tables are located
main_dir <- "~/scratch/"
# main_dir <- "/storage/eICU/"
save_dir <- "~/scratch/cicm_extubation/cuts/"

#load in tables of interest
carePlan <-  read.eicu(main_dir, "carePlanGeneral")
treatment <- read.eicu(main_dir, "treatment")
chart <- read.eicu(main_dir, "respiratoryCharting")
restCare <- read.eicu(main_dir, "respiratoryCare")
dfVent <- read.eicu(main_dir, "df_vent_event")
mydf <- data.table::fread("~/scratch/cicm_extubation/cuts/300_cut.csv")
mitdf <- data.table::fread("~/scratch/MIT_df.csv")
mitdf2 <- data.table::fread("~/scratch/cicm_extubation/csv/mit_sample.csv")

#manipulating carePlan to see how many ventilated patients we have
tbl <- carePlan[which(carePlan$cplgroup == "Ventilation"),] %>% droplevels()
tbl <- tbl[which(tbl$cplitemvalue %like% "Ventilated"),] %>% droplevels()
# table(tbl$cplitemvalue)
length(unique(tbl$patientunitstayid)) #50490 unique patientunitstayids
tblnr <- tbl[!duplicated(tbl$patientunitstayid),] #50490 rows
tblf <- tblnr %>% select(patientunitstayid, cplitemoffset, cplgroup, cplitemvalue) %>% droplevels
View(table(tblf$cplitemvalue))

#finding patients who were both intubated and extubated in carePlanGeneral
# tblex <- carePlan[which(carePlan$cplgroup == "Planned Procedures"),] %>% droplevels()
# tblex <- tblex[which(tblex$cplitemvalue == "Extubation"),] %>% droplevels()
tblex <- carePlan[which(carePlan$cplgroup == "Ordered Protocols"),] %>% droplevels()
tblex <- tblex[which(tblex$cplitemvalue == "Ventilator wean"),] %>% droplevels()
commonCP <- tbl[which(tbl$patientunitstayid %in% tblex$patientunitstayid),] %>% droplevels() # 10997
#data.frame(intersect(tbl$patientunitstayid, tblex$patientunitstayid)) # 6209 obs

delirious <- carePlan[which(carePlan$cplgroup == "Psychosocial Status"),] %>% droplevels()
psych <- commonCP[which(commonCP$patientunitstayid %in% delirious$patientunitstayid),] %>% droplevels()

#manipulating treatment to see how many mechanically ventilated patients we have
treatment$treatmentstring <- as.character(treatment$treatmentstring)
tbt <- treatment[which(grepl("ventila", treatment$treatmentstring, fixed=TRUE) &
                       grepl("mech", treatment$treatmentstring, fixed=TRUE)),] %>% droplevels()
tbtnr <- tbt[!duplicated(tbt$patientunitstayid),] #42111 rows
length(unique(tbtnr$patientunitstayid)) #42111 unique patientunitstayids
tbtf <- tbtnr %>% select(patientunitstayid, treatmentoffset, treatmentstring) %>% droplevels()
View(table(tbtf$treatmentstring))

#finding patients who were both intubated and extubated in treatment
tbtex <- treatment[which(grepl("endotracheal tube removal", treatment$treatmentstring, fixed=TRUE)),] %>% droplevels()
commonTT <- tbt[which(tbt$patientunitstayid %in% tbtex$patientunitstayid),] %>% droplevels()
  #data.frame(intersect(tbt$patientunitstayid, tbtex$patientunitstayid)) # 3713 obs
tbtex <- treatment[which(grepl("reintubation", treatment$treatmentstring, fixed=TRUE)),] %>% droplevels()
commonTTR <- tbt[which(tbt$patientunitstayid %in% tbtex$patientunitstayid),] %>% droplevels()
  #data.frame(intersect(tbt$patientunitstayid, tbtex$patientunitstayid)) # 434 obs
table(tbtex$treatmentstring)
psych <- delirious[which(commonTTR$patientunitstayid %in% delirious$patientunitstayid),] %>% droplevels()
table(psych$cplitemvalue)

#manipulating respiratory charting to find unique cases of PEEP
tbc <- chart[which(chart$respchartvaluelabel == "PEEP"),] %>% droplevels()
tbcnr <- tbc[!duplicated(tbc$patientunitstayid),] #45541 rows
length(unique(tbcnr$patientunitstayid)) #45541 unique patientunitstayids
View(table(tbcf$respchartvaluelabel))

#identifying continuous intubation
# peep <- tbc %>% dplyr::select(patientunitstayid, respchartoffset, respchartvaluelabel) %>% droplevels()
# peep <- peep %>% group_by(patientunitstayid) %>% arrange(patientunitstayid, respchartoffset)
# peep_alternative <- peep[order(peep$patientunitstayid, peep$respchartoffset),]
# peep <- peep %>% group_by(patientunitstayid) %>% mutate(difference = respchartoffset -
                                                          # dplyr::lag(respchartoffset, default = respchartoffset[1]))
View(peep)

#manipulating respiratory care to see if it is bad
#one idea of how to use respiratory care is to first find patients of interest using
#Respiratory Charting, CarePlanGeneral, and Treatment, then see if we can 
#match intubation and extubation times to respiratory care table. 

tbr <- restCare[which((restCare$peeplimit > 0)),] %>% droplevels()
tbrnr <- tbr[!duplicated(tbr$patientunitstayid),]
length(unique(tbrnr$patientunitstayid))
tbrf <- tbrnr %>% select(patientunitstayid, ventstartoffset, peeplimit) %>% droplevels()
View(table(tbrf$peeplimit))

#manipulating df_vent_event to see how many ventilated patients we have
tdf <- dfVent[which(dfVent$event %like% "mechvent"),] %>% droplevels()
length(unique(tdf$patientunitstayid)) #8886 unique patientunitstayids
tdfnr <- tdf[!duplicated(tdf$patientunitstayid),] #8886 rows
tdff <- tdfnr %>% select(patientunitstayid, event, hrs) %>% droplevels
View(table(tdff$event))

#looking at my own sample df to see how it compares to eICU tables and df_vent_event
mdf <- mydf[which(mydf$Meets_criteria == TRUE),]
mdfnr <- mdf[!duplicated(mdf$patientunitstayid),]
mdf <- mdf %>% group_by(patientunitstayid) %>% arrange(patientunitstayid, desc(MV_number))
mdfnr <- mdf[!duplicated(mdf$patientunitstayid),]
summary(mdfnr$MV_number)
View(table(mdfnr$MV_number))
# write.csv(mdfnr,"/home-1/aszewc1@jhu.edu/scratch/cicm_extubation/mvCount.csv")

#looping through my own sample identification dfs and comparing each to MIT df
unique_pids <- mitdf[!duplicated(mitdf$patientunitstayid),]
print(paste0(nrow(unique_pids), " unique pids in mit df"))
for (t in 1:11) {                #when generating several
  gap_length <- 300 + (30 * (t-1))     #when generating several
  current <- data.table::fread(paste0(save_dir, gap_length, "_cut.csv"))
  current <- current[!duplicated(current$patientunitstayid),]
  num <- nrow(current[which(current$patientunitstayid %in% unique_pids$patientunitstayid &
                            current$Meets_criteria == TRUE),])
  print(paste0(gap_length, " cutoff dataframe has in common with MIT: ", num))
}

#print venn diagram - NEED TO FIX
# print.venn(tblnr$patientunitstayid, tbtnr$patientunitstayid, tbcnr$patientunitstayid, tbrnr$patientunitstayid)
library(gplots)
require(gplots)
jpeg("venn_plot.jpg")
bitmap("venn_plot.png")
careP <- as.character(tblnr$patientunitstayid)
treat <- as.character(tbtnr$patientunitstayid)
respCh <- as.character(tbcnr$patientunitstayid)
respC <- as.character(tbrnr$patientunitstayid)
dfevt <- as.character(tdfnr$patientunitstayid)
myevt <- as.character(mdfnr$patientunitstayid)
input <- list(dfevt=dfevt, myevt=myevt)
diag <- (venn(input, show.plot = FALSE, intersections=FALSE))
dev.off()

careP <- as.character(unique(commonCP$patientunitstayid))
treat <- as.character(unique(commonTT$patientunitstayid))
reint <- as.character(unique(commonTTR$patientunitstayid))
input <- list(careP=careP, treat=treat, reint=reint)
diag <- (venn(input, show.plot = FALSE, intersections=FALSE))
