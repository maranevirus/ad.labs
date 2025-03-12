import os
import urllib.request
from datetime import datetime
import pandas as pd
import glob
import time
import hashlib

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

regionS = {
    1: "Вінницька", 2: "Волинська", 3: "Дніпропетровська", 4: "Донецька",
    5: "Житомирська", 6: "Закарпатська", 7: "Запорізька", 8: "Івано-Франківська",
    9: "Київська", 10: "Кіровоградська", 11: "Луганська", 12: "Львівська",
    13: "Миколаївська", 14: "Одеська", 15: "Полтавська", 16: "Рівненська",
    17: "Сумська", 18: "Тернопільська", 19: "Харківська", 20: "Херсонська",
    21: "Хмельницька", 22: "Черкаська", 23: "Чернівецька", 24: "Чернігівська",
    25: "Республіка Крим"
}

NOAA_TO_UA = {
    1: 13, 2: 14, 3: 15, 4: 16, 5: 17, 6: 18, 7: 19, 8: 20,
    9: 21, 10: 22, 11: 23, 12: 24, 13: 1, 14: 2, 15: 3, 16: 4,
    17: 5, 18: 6, 19: 7, 20: 8, 21: 9, 22: 10, 23: 11, 24: 12,
    25: 25
}
def download_vhi(region_index):
    if region_index not in regionS:
        print(f" Невірний індекс області: {region_index}")
        return

    region_name = regionS[region_index]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{DATA_DIR}/VHI_{region_index}_{region_name}_{timestamp}.csv"

    prefix = f"VHI_{region_index}"
    for existing_file in os.listdir(DATA_DIR):
        if existing_file.startswith(prefix):
            print(f" Дані для {region_name} вже існують: {existing_file}")
            return

    url = f"https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/get_TS_admin.php?country=UKR&provinceID={region_index}&year1=1981&year2=2024&type=Mean"

    print(f" Завантаження даних для {region_name}...")

    try:
        urllib.request.urlretrieve(url, filename)
        print(f" Дані для {region_name} збережено: {filename}")
    except Exception as e:
        print(f" Помилка завантаження для {region_name}: {e}")

def download_all_regions(delay=2):
    print("Початок завантаження даних для всіх областей...")
    downloaded_files = []
    
    for region_index in regionS.keys():
        try:
            time.sleep(delay)
            file_path = download_vhi(region_index)
            if file_path:
                downloaded_files.append(file_path)
        except Exception as e:
            print(f" Помилка завантаження для області {region_index}: {e}")
    
    print(f"Завантаження завершено. Завантажено {len(downloaded_files)} файлів.")
    return downloaded_files

def read_data_to_dataframe(directory):
    csv_files = glob.glob(os.path.join(directory, "VHI_*.csv"))
    print(" Доступні файли у директорії:")
    for i, filename in enumerate(csv_files, start=1):
        print(f"{i}. {filename}")

    try:
        choice = int(input("Введіть номер файлу для обробки (або 0 для обробки всіх файлів): "))
        if choice == 0:
            selected_files = csv_files
        else:
            selected_files = [csv_files[choice - 1]]
    except (ValueError, IndexError):
        return pd.DataFrame()

    frames = []

    for filename in selected_files:
        try:
            with open(filename, 'r') as file:
                lines = file.readlines()
            
            region_id = int(lines[0].split('Province=')[1].split(':')[0].strip())
            # region_id = pd.Series(region_id)
            # region_id = region_id.map(NOAA_TO_UA).map(regionS)
            # region_id = region_id.to_list()
            data_rows = []
            for line in lines[2:]:
                clean_line = (line.replace('<tt><pre>', '')
                              .replace('</tt></pre>', '')
                              .strip())
                
                if clean_line and not clean_line.startswith('<'): 
                    values = [v.strip() for v in clean_line.split(',') if v.strip()]
                    if len(values) >= 7: 
                        try:
                            row = {
                                'Year': int(values[0]),
                                'Week': int(values[1]),
                                'SMN': float(values[2]),
                                'SMT': float(values[3]),
                                'VCI': float(values[4]),
                                'TCI': float(values[5]),
                                'VHI': float(values[6]),
                                'Region': region_id
                            }
                            data_rows.append(row)
                        except (ValueError, IndexError) as e:
                            print(e)
                            continue
            
            if data_rows:
                df = pd.DataFrame(data_rows)
                df.columns = (
                    df.columns
                    .str.replace("[^a-zA-Z0-9]", "_", regex=True)  
                    .str.lower() 
                )
                
                frames.append(df)
            
        except Exception as e:
            print(e)

    if frames:
        combined_df = pd.concat(frames, ignore_index=True)

        last_df = combined_df.drop(combined_df.loc[combined_df['vhi'] == -1].index)
        last_df['region_name'] = last_df['region'].map(regionS)
        print(combined_df.columns.tolist())

        return last_df

def get_vhi_for_region_year(df, region_id, year):
    if 'region' not in df.columns or 'year' not in df.columns:
        print(" Необхідні стовпці відсутні у DataFrame.")
        return pd.DataFrame()
    
    result = df[(df['region'] == region_id) & (df['year'] == year)][['week', 'vhi']]
    if result.empty:
        print(f" Немає даних для області {regionS.get(region_id, 'Невідомо')} у {year} році.")
    else:
        print(f"Дані VHI для області {regionS.get(region_id, 'Невідомо')} у {year} році:")
        print(result.to_string(index=False))
    return result

def get_vhi_extremes(df, region_ids, years):
    if 'region' not in df.columns or 'year' not in df.columns:
        print(" Необхідні стовпці відсутні у DataFrame.")
        return None
    
    filtered_df = df[
        (df['region'].isin(region_ids)) & 
        (df['year'].isin(years))
    ]
    
    if filtered_df.empty:
        print(" Немає даних для вказаних областей та років.")
        return None
    
    stats = {
        'min_vhi': filtered_df['vhi'].min(),
        'max_vhi': filtered_df['vhi'].max(),
        'mean_vhi': filtered_df['vhi'].mean(),
        'median_vhi': filtered_df['vhi'].median()
    }
    
    print("\nСтатистика VHI:")
    print(f"Області: {', '.join([regionS.get(r, 'Невідомо') for r in region_ids])}")
    print(f"Роки: {', '.join(map(str, years))}")
    print(f"Мінімальне значення VHI: {stats['min_vhi']:.2f}")
    print(f"Максимальне значення VHI: {stats['max_vhi']:.2f}")
    print(f"Середнє значення VHI: {stats['mean_vhi']:.2f}")
    print(f"Медіанне значення VHI: {stats['median_vhi']:.2f}")
    
    return stats
def update_region_indices(df):
    
    if 'region' not in df.columns:
        print(" 'region' column not found in df")
        return df
    
    print("updating region indices...")
    df['region'] = df['region'].map(NOAA_TO_UA)
    df['region_Name'] = df['region'].map(regionS)
    print("region indices updated successfully")
    return df

def find_extreme_droughts(df, threshold_percent=20):
    results = []
    total_regions = 25
    threshold_regions = int(total_regions * threshold_percent / 100)
    
    print(f" Пошук екстремальних посух, які торкнулися більше {threshold_percent}% областей...")
    
    for year in df['year'].unique():
        year_data = df[df['year'] == year]
        drought_regions = year_data[year_data['vhi'] < 15]['region'].unique()
        
        if len(drought_regions) >= threshold_regions:
            results.append({
                'year': year,
                'affected_regions': len(drought_regions),
                'regions': [regionS[r] for r in drought_regions if r in regionS],
                'vhi_values': year_data[year_data['region'].isin(drought_regions)]['vhi'].tolist()
            })
    
    if results:
        print(f" Знайдено {len(results)} років з екстремальними посухами.")
        for result in results:
            print(f"Рік {result['year']} - області: {', '.join(result['regions'])} - VHI: {result['vhi_values']}")
    else:
        print("Не знайдено екстремальних посух за цей період.")
    
    return results

def main_menu():
    print("Ласкаво просимо до аналізу VHI даних!")
    df = None 
    while True:
        print("Головне меню:")
        print("1 - Завантажити дані для одного регіону")
        print("2 - Завантажити дані для всіх регіонів")
        print("3 - Зчитати та аналізувати дані VHI")
        print("0 - Вийти")

        try:
            choice = int(input("Введіть номер операції: "))

            if choice == 0:
                print("👋 Вихід з програми.")
                break

            elif choice == 1:
                print("Оберіть область для завантаження даних (введіть номер області):")
                for region_id, region_name in regionS.items():
                    print(f"{region_id}: {region_name}")

                try:
                    selected_region = int(input("Ваш вибір: "))
                    if selected_region in regionS:
                        download_vhi(selected_region)
                    else:
                        print(" Невірний номер області. Введіть число від 1 до 25.")
                except ValueError:
                    print(" Введено неправильне значення. Будь ласка, введіть число.")

            elif choice == 2:
                download_all_regions()

            elif choice == 3:
                df = read_data_to_dataframe(DATA_DIR)
                if df is not None and not df.empty:
                    print(" Дані успішно зчитано у фрейм:")
                    print(df.head())

                    while True:
                        print("Меню аналізу даних VHI:")
                        print("1 - Отримати VHI для області та року")
                        print("2 - Отримати статистику VHI для областей та років")
                        print("3 - Знайти екстремальні посухи")
                        print("4 - Оновити індекси областей")
                        print("0 - Повернутися до головного меню")

                        sub_choice = int(input("Введіть номер операції: "))

                        if sub_choice == 0:
                            break

                        elif sub_choice == 1:
                            region_id = int(input("Введіть ID області: "))
                            year = int(input("Введіть рік: "))
                            get_vhi_for_region_year(df, region_id, year)

                        elif sub_choice == 2:
                            region_ids = list(map(int, input("Введіть ID областей через пробіл: ").split()))
                            years = list(map(int, input("Введіть роки через пробіл: ").split()))
                            get_vhi_extremes(df, region_ids, years)

                        elif sub_choice == 3:
                            threshold = int(input("Введіть порогове значення у відсотках (за замовчуванням 20): ") or 20)
                            find_extreme_droughts(df, threshold)
                        elif sub_choice == 4:
                            df = update_region_indices(df)
                            print(df.head())

                        else:
                            print(" Невірний вибір. Будь ласка, введіть число від 0 до 3.")

                else:
                    print(" Немає даних для аналізу. Спочатку завантажте або зчитайте дані.")

            else:
                print(" Невірний вибір. Будь ласка, введіть число від 0 до 3.")

        except ValueError as e:
            print(e)
if __name__ == "__main__":
    main_menu()
