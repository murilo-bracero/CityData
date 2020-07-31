import csv
import os

import requests
import pandas as pd

BASE_URL = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados/'


def ufs_to_csv(file_path="ufs.csv"):
    res = requests.get(BASE_URL)

    res.raise_for_status()

    dataframe = pd.DataFrame(res.json())

    dataframe.to_csv(file_path, sep=";", index=False)

    return res.json()


def cities_by_uf_to_csv(uf, file_path="cities.csv"):
    res = requests.get(BASE_URL + uf + "/municipios")

    res.raise_for_status()

    df = pd.DataFrame(res.json())

    df.to_csv(file_path, sep=";", index=False)


def delete_csvs(filenames, path=''):
    for filename in filenames:
        os.remove(f'{path}/{filename}')


def merge_cities(path):
    csv_filenames = os.listdir(path)

    cities_csvs = csv_filenames[:-1]

    cities_frames = []

    for cities_csv in cities_csvs:
        df = pd.read_csv(f'{path}/{cities_csv}', sep=';')
        cities_frames.append(df)

    big_frame = pd.concat(cities_frames)

    big_frame.to_csv(f'{path}/cities.csv', sep=';', index=False)

    delete_csvs(cities_csvs, path)


def extract_from_ibge(path):
    try:
        os.mkdir(path)
    except:
        print(f'{path} folter already exists')

    ufs = ufs_to_csv(f'{path}/ufs.csv')

    for uf in ufs:
        sigla = uf['sigla']
        cities_by_uf_to_csv(sigla, f'{path}/cities_{sigla}.csv')

    merge_cities(path)


if __name__ == "__main__":
    extract_from_ibge('digest')
