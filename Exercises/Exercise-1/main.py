import requests, zipfile, io, os
from pathlib import Path
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]
def main():

    directory_path = Path('downloads')
    try:
        directory_path.mkdir(parents=True)
        print(f'Успешно создана папка {directory_path}')
    except FileExistsError:
        print(f'папка {directory_path} уже существует')
    except PermissionError:
        print(f'Отсутствует разрешение на создание папки {directory_path}')
    except Exception as e:
        print(f'Случилась ошибка: {e}')

    for uri in download_uris:
        try:
            response = requests.get(uri, stream=True)
            if response.status_code!=200:
                print(f'Пропуск {uri}: статус код {response.status_code}')    
                continue
            print(f'Статус ответа по {uri}: {response.status_code}')
            zip_file_name = uri.split('/')[-1]
            zip_path = directory_path / zip_file_name
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            with zipfile.ZipFile(zip_path) as myzip:
                print(f'Список файлов: {myzip.namelist()}')
                extracted = 0
                total = 0
                for item in myzip.infolist():
                    total+=1
                    if '.csv' in item.filename.lower() and '__MACOSX' not in item.filename:
                        item.filename = os.path.basename(item.filename)
                        myzip.extract(item, path=directory_path)
                        print(f'Файл {item} извлечён')
                        extracted+=1
                print(f'Извлечено {extracted} файлов из {total}')
            if zip_path.exists():
                zip_path.unlink()
        except Exception as e:
            print(f'Случилась ошибка: {e}')
if __name__ == "__main__":
    main()
