import os 
save_path = '/home/team25/M2/xgb_files/'
file_stats = os.stat(save_path+"xgb_model.json")
print("XGBoost Model size on disk:", file_stats.st_size / (1024 * 1024), "MB")