# ETL-processes HW 2
Домашнее задание 2 по учебной дисциплине «ETL-процессы».  
Тема: «Основы трансформации данных».  
Выполнил студент: Хорасанджян Левон, МИНДА251.

## Task
Выполните преобразование данных. Для этого используйте [датасет](https://www.kaggle.com/datasets/atulanandjha/temperature-readings-iot-devices).  
Что необходимо сделать:
- вычислите 5 самых жарких и самых холодных дней за год;
- отфильтруйте out/in = in;
- поле `noted_date` переведите в формат `yyyy-MM-dd` с типом данных `date`;
- очистите температуру по 5-му и 95-му процентилю.

## Solution
Для решения задачи использован инструмент **Apache Airflow**.  
ETL-пайплайн реализован в виде DAG с последовательными этапами:
**extract → transform → load → aggregate**.

На этапе трансформации выполняются:
- фильтрация данных по признаку `out/in = In`;
- преобразование поля `noted_date` к типу `date` без временной компоненты;
- очистка значений температуры методом clipping по 5-му и 95-му процентилю.

На этапе агрегации рассчитывается средняя температура по дням и формируются выборки
из 5 самых жарких и 5 самых холодных дней.

## Repository Description
В репозитории настроено окружение [**Apache Airflow** с использованием **Docker Compose**](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml).

Основные компоненты:
- `dags/etl_dag.py` — DAG Airflow, реализующий ETL-пайплайн;
- `src/etl_pipeline.py` — логика извлечения, трансформации, загрузки и агрегации данных;
- `data/raw/IOT-temp.csv` — исходный датасет;
- `data/processed/` — результаты выполнения пайплайна.

## Credentials (Airflow authorization)
URL: http://localhost:8080  
username: airflow  
password: airflow  

## Results
В результате выполнения DAG формируются следующие файлы:
- `data/processed/cleaned.csv` — очищенный датасет после всех трансформаций;
- `data/processed/top5_hot.csv` — 5 самых жарких дней;
- `data/processed/top5_cold.csv` — 5 самых холодных дней.

Скриншоты успешного выполнения DAG в Airflow, а также примеры полученных CSV-файлов, размещены в папке `screenshots`.
