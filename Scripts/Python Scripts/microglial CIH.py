import pandas as pd
import os
import glob
import re

#### Set paths ###
data_path = "/home/christophery/Collaborations/Microglia Morphology/Data"
skeleton_dir = os.path.join(data_path, "CIH Microglia data/SkeletonResults/")

#### Read in the fraclac data ###
FracLac1_CIH = pd.read_csv(os.path.join(data_path, "Hull and Circle Results CIH.txt"), sep='\t', header=None)

# Split the first column to extract Name and ID
fraclac_cih = FracLac1_CIH[0].str.split('tif_thresholdedtif_', n=1, expand=True)
fraclac_cih.columns = ['Name', 'temp']
fraclac_cih['ID'] = fraclac_cih['temp'].str.split('tif', n=1, expand=True)[0]
fraclac_cih = fraclac_cih[['Name', 'ID']]

#### Read in the fraclac data ###
FracLac1_Controls= pd.read_csv(os.path.join(data_path, "Hull and Circle Results Controls.txt"), sep='\t', header=None)

# Split the first column to extract Name and ID
fraclac_ctl = FracLac1_Controls[0].str.split('tif_thresholdedtif_', n=1, expand=True)
fraclac_ctl.columns = ['Name', 'temp']
fraclac_ctl['ID'] = fraclac_ctl['temp'].str.split('tif', n=1, expand=True)[0]
fraclac_ctl = fraclac_ctl[['Name', 'ID']]

# Merge the two fraclac dataframes, and deduplicate
fraclac = pd.concat([fraclac_cih, fraclac_ctl]).drop_duplicates().reset_index(drop=True)

# Get subdirectories
subdirs = [d for d in os.listdir(skeleton_dir) 
           if os.path.isdir(os.path.join(skeleton_dir, d))]

complete_df_list = []

for subdir in subdirs:
    subdir_path = os.path.join(skeleton_dir, subdir)
    # Get CSV files in this subdirectory
    file_vec = glob.glob(os.path.join(subdir_path, "*.csv"))
    
    
    # Create an empty dataframe to add the CSV file's data to
    skeleton = pd.DataFrame(columns=[
        'Branches', 'Junctions', 'End_point_voxels', 'Junction_voxels', 
        'Slab_voxels', 'Average_Branch_Length', 'Triple_points', 
        'Quadruple_points', 'Maximum_Branch_Length', 'Name', 'ID'
    ])
    
    rows = []
    for file_path in file_vec:
        # Read in the CSV
        open_file = pd.read_csv(file_path, header=0)

        # Remove the first column and add Name/ID columns
        open_file = open_file.iloc[:, 1:].copy()
        open_file['Name'] = None
        open_file['ID'] = None

        open_file.columns = [col.replace("# ", "", 1).lstrip() if col.startswith("# ") else col.lstrip() for col in open_file.columns]

        open_file2 = open_file.copy()

        # Extract file name with extension, remove the extension, periods, spaces, and 'tif_results'
        base_name = os.path.basename(file_path)
        file_name_no_ext = os.path.splitext(base_name)[0]
        cleaned_name = re.sub(r' |\.|tif_results$', '', file_name_no_ext)

        # Extract Name and ID using regex
        name_match = re.search(r'(.*?)(?=tif_thresholdedtif_)', cleaned_name)
        id_match = re.search(r'(?<=tif_thresholdedtif_)(.*)', cleaned_name)

        open_file2.loc[0, 'Name'] = name_match.group(1) if name_match else None
        open_file2.loc[0, 'ID'] = id_match.group(1) if id_match else None

        # Collect just the first row for later concatenation
        rows.append(open_file2.iloc[[0]])
        
    rows_df = pd.concat(rows, ignore_index=True, sort=False)
    
    # Remove rows with NA values in Name or ID
    rows_df = rows_df.dropna(subset=['Name', 'ID']).reset_index(drop=True)
    merged_df = pd.merge(rows_df, fraclac, on=['Name', 'ID'], how='inner')

    new_cols = list(skeleton.columns) + [merged_df.columns[-1]]
    merged_df.columns = new_cols

    merged_df = merged_df.dropna(axis=1, how='all')
    
    merged_df['Groups'] = subdir

    complete_df_list.append(merged_df)
# Combine all subdirectory dataframes into one
complete_df = pd.concat(complete_df_list, ignore_index=True, sort=False)
complete_df.to_csv(os.path.join(data_path, "skeleton morphology output.csv"), index=False)