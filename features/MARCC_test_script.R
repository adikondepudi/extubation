### MARCC TEST CODE

#MY DIR - for local machine
code_folder_dir <- "~/scratch/cicm_extubation/features/"
code_dir <- paste0(code_folder_dir, "/")


#MARCC DIR
# code_folder_dir <- "/home-2/hkim171@jhu.edu/scratch/"
# code_dir <- paste0(code_folder_dir, "/eICU_featurization")

source(paste0(code_dir, "/model_guide.R"))

build_prototype(code_dir = code_dir,
                save_dir = code_folder_dir,
                num_outer_loop = 5,
                cross_validation_k = 5,
                merge_identifier = "patientunitstayid",
                feature_dir = paste0(code_dir, "/test_data/test_feature_space.csv"),
                label_dir = paste0(code_dir, "/test_data/test_label.csv"),
                outcomes = c("Reintubated", "Extubated"),
                experiment_name = "test_4_7_21",
                Already_trained = FALSE
)

# build_prototype(code_dir = code_dir, 
#                 save_dir = code_folder_dir,
#                 num_outer_loop = 5,
#                 cross_validation_k = 5,
#                 merge_identifier = "patientunitstayid",
#                 feature_dir = paste0(code_dir, "/test_data/test_feature_space.csv"),
#                 label_dir = paste0(code_dir, "/test_data/test_label.csv"),
#                 outcomes = c("Expired", "Alive"),
#                 experiment_name = "test",
#                 Already_trained = TRUE
# )

source(paste0(code_dir, "/Performance_metric_plotting.R"))
performance_metric_plotting(experiment_name = "test_4_7_21", saved_file_location = paste0(code_folder_dir, "/", "test_4_7_21"))


#Random forest feature ranking example
experiment_folder_dir <- code_folder_dir
code_dir <- paste0(code_folder_dir, "/eICU_featurization")

source(paste0(code_dir, "/feature_ranking.R"))

random_forest_rank(experiment_folder_dir <- experiment_folder_dir,
                   code_dir <- code_dir,
                   experiment_name = "test_4_7_21",
                   num_outer_loop = 5,
                   how_many_top_features = 50,
                   use_trained_rf = TRUE)

random_forest_rank(experiment_folder_dir <- experiment_folder_dir,
                   code_dir <- code_dir,
                   experiment_name = "test_4_7_21",
                   num_outer_loop = 5,
                   how_many_top_features = 50,
                   use_trained_rf = FALSE)

XG_rank(experiment_folder_dir <- experiment_folder_dir,
        code_dir <- code_dir,
        experiment_name = "test_4_7_21",
        num_outer_loop = 5,
        how_many_top_features = 50,
        shap = TRUE)

XG_rank(experiment_folder_dir <- experiment_folder_dir,
        code_dir <- code_dir,
        experiment_name = "test_4_7_21",
        num_outer_loop = 5,
        how_many_top_features = 50,
        shap = FALSE)

GLM_rank(experiment_folder_dir <- experiment_folder_dir,
         code_dir <- code_dir,
         experiment_name = "test_4_7_21",
         num_outer_loop = 5,
         how_many_top_features = 50)