import json
import pandas as pd
import pathlib as p


def read_voivodeship_data(file_name: str):
    """

    :param file_name: name of the file with the list of voivodeships and the regions contained in each voivodeship
    :return: voivodeships dictionary
    """

    voivodeship_file: p.Path = p.Path.cwd().joinpath(file_name)

    voivodeships = {}

    with voivodeship_file.open(mode="r", encoding="utf-8-sig") as read_file:
        for line in read_file:
            raw_line: str = line.rstrip(" \n").rsplit(" - ")
            voivodeships[raw_line[0].strip()] = [x.rstrip(";") for x in raw_line[1].rsplit("; ")]

    return voivodeships


def read_json_file(region_name:str):
    """
    Loads nodal data for chosen region
    :param region_name: name of the region for which to extract data
    :param folder_name: directory where all the json files are contained
    :return: DataFrame of nodal data for chosen region
    """

    json_folder_path: p.Path = p.Path('/Users/Olga/PycharmProjects/MGR_Project/MGR/region_data_files')
    #print(json_folder_path)
    region_file_path: p.Path = json_folder_path.joinpath(f"{region_name}.json")

    with region_file_path.open(mode="r", encoding="utf-8-sig") as read_file:
        file = json.load(read_file)

    df = pd.DataFrame(file["nodes"])

    return df


def get_data_voivodeship(voivodeship_name: str, voivodeships_dict: dict ):

    data_voivodeship = pd.DataFrame()
    for region in voivodeships_dict[voivodeship_name]:
        #print(f"Getting data for {region}")
        df_region = read_json_file(region)
        if type(df_region) != int:
            #print(f"Len of data for {region} is {len(df_region)}")
            data_voivodeship = pd.concat([data_voivodeship,df_region], ignore_index= True)
        #else:
        #    print(f"Dataframe for {region} is empty")

        #if data_voivodeship.empty == False:
        #    print(f"Length of voivodeship data after appending data from {region} is {len(data_voivodeship)}")
    return  data_voivodeship



