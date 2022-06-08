
import pandas as pd



def get_dataframe_from_excel(file_path, sheet_name):
    try:
        return pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception as ex:
        print("Unable to get labels sheet from excel file.\n {}".format(ex))


def get_sas_labels_dataframes(file_path: str, sheet_names: list):
    """
        Returns a list of the dataframes with the sas labeles definitions depending on their sheet name.
        
        Arguments:
            - file_path (str) excel file location of labels.
            - sheet_names (list) list with the names of the sheet names.
        
        Returns:
            - (pd.DataFrame)
    """
    labels_dataframes = []
    for name in sheet_names:
        labels_dataframes.append(get_dataframe_from_excel(file_path=file_path, sheet_name=name))
    
    if len(sheet_names) != len(labels_dataframes):
        raise Exception("Unable to retrieve al the labels dataframes.")
    
    return labels_dataframes



sas_file = './data/capstone_sas_labels.xlsx' 
labels_sheet_names = ['I94CIT & I94RES', 'I94ADDR', 'I94PORT', 'I94MODE', 'I94VISA']

# labels_df = get_sas_labels_dataframes(file_path=sas_file, sheet_names=labels_sheet_names)

# for df in labels_df:
#     print(df.head(5))
#     print('-'*20)

insert_q = "Something {}"
for x in labels_sheet_names:
    insert_q += insert_q.format(x)

print(insert_q)
