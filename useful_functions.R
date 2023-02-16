#Functions

#separate file created to house useful time saving functions


#function created to simplify the loading of eICU data based on table directory. 
#This will help to use this code on other machines. 
read.eicu <- function(directory, tablename, extension) {
  #checks for two extensions: variations of csv and rds
  #extension should represent the case of the extension of the file
  #to be loaded 
  
  if (grepl(".", extension, fixed=TRUE)) {
    extension <- gsub("[[:punct:]]", "", extension)
  }
  
  if(extension == "csv" | extension == "CSV" | extension == "Csv") {
    
    return(data.table::fread(paste0(directory, "/",tablename, ".", extension)))
    
  } else if (extension == "rds" | extension == "RDS" | extension == "Rds") {
  
    return(readRDS(paste0(directory, "/",tablename, ".", extension)))
  } else {
    stop("check extension")
  }
  
}

#function created to compute and print a venn-diagram
# print.venn <- function(tblnr, tbtnr, tbcnr, tbrnr) {
#   library(gplots)
#   require(gplots)
#   careP <- as.character(tblnr)
#   treat <- as.character(tbtnr)
#   respCh <- as.character(tbcnr)
#   respC <- as.character(tbrnr)
#   input <- list(careP=careP, treat=treat, respCh=respCh, respC=respC)
#   diag <- (venn(input, show.plot = TRUE, intersections=FALSE))
#   diag
#   # Old code
#   # planTreat <- data.frame(intersect(tblnr$patientunitstayid, tbtnr$patientunitstayid))
#   # planChart <- data.frame(intersect(tblnr$patientunitstayid, tbcnr$patientunitstayid))
#   # treatChart <- data.frame(intersect(tbtnr$patientunitstayid, tbcnr$patientunitstayid))
#   
#   #tbl <- tbl[order(tbl$patientunitstayid, tbl$nursingchartoffset), ] #orders id then offset
#   #tbl$nursingchartvalue <- as.numeric(as.character(tbl$nursingchartvalue)) #turns factors into char then num
#   
#   #for (i in 1:length(unique(tbl$patientunitstayid))) {
#   #  i = 7
#   #  temp <- tbl[which(tbl$patientunitstayid == 141233), ] #250724 #256864
#   #  plot(temp$nursingchartoffset, temp$nursingchartvalue, type = "l", main = "Body Temperature after Cardiac Arrest", xlab = "Time after ICU Admit (min)", ylab = "Temperature (°C)")
#   #}
# }