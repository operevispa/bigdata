# Домашнее задание №1. Luigi pipeline. Студент Перевиспа О.В.

# Запуска пайплайна из командной строки
#   python -m luigi_pipeline CreateDataSet --dataset-name GSE68849 --local-scheduler
# Входящие параметры:
#  - dataset_name - имя набора данных (строка). По умолчанию используется имя GSE68849
#  - data_folder - папка, в которую будут сохраняться данные (строка). Необязательный
#  - file_preprocess - файлы, которые требутеся предобработать (список). Необязательный
#  - columns_del - столбцы, которые необходимо удалить (список). Необязательный


import luigi
from luigi.format import Nop
import io
import os
import requests
import pandas as pd
import tarfile
import gzip
from pathlib import Path


class DownloadDataset(luigi.Task):
    """
    Скачивает файл с данными по ссылке с сайта.
    Название датасета передается пользователем в консоли в параметре dataset-name
    Ссылка https://www.ncbi.nlm.nih.gov/geo/download/?acc={dataset-name}&format=file
    """

    dataset_name = luigi.Parameter()
    data_folder = luigi.Parameter()

    def output(self):
        # поскольку tar-файл, это двоичный файл, нужно указывать в LocalTarget format=Nop, иначе ожидается строка и возникает ошибка
        return luigi.LocalTarget(
            os.path.join(self.data_folder, f"{self.dataset_name}.tar"),
            format=Nop,
        )

    def run(self):
        response = requests.get(
            f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file",
            stream=True,
        )

        # проверяем http статус код
        if response.status_code == 200:
            # создаем папку, в которую выгрузим скачанный файл
            os.makedirs(self.data_folder, exist_ok=True)

            # сохраняем файл в папку
            with self.output().open("w") as f:
                f.write(response.content)
        else:
            raise ValueError(f"Ошибка загрузки датасета: {response.status_code}")


class ExtractFiles(luigi.Task):
    """
    Распаковывает скачанный файл и архивы внутри него.
    Вычленяет таблицы из загруженных файлов и сохраняет их в отдельные файлы
    """

    dataset_name = luigi.Parameter()
    data_folder = luigi.Parameter()

    # определяем зависимость данной задачи от задачи DownloadDataset
    def requires(self):
        return DownloadDataset(
            dataset_name=self.dataset_name, data_folder=self.data_folder
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.data_folder, self.dataset_name, "extracts.txt")
        )

    def run(self):
        # создаем каталог для разархивированного базового файла
        ds_dir = os.path.join(self.data_folder, self.dataset_name)
        os.makedirs(ds_dir, exist_ok=True)

        # Открываем tar-архив
        with tarfile.open(self.input().path, "r") as tar:
            # Перебираем все члены архива
            for member in tar.getmembers():
                # Проверяем, является ли член файлом и имеет ли он расширение .gz
                if member.isfile() and member.name.endswith(".gz"):
                    # Получаем имя файла без пути
                    gz_file_name = os.path.basename(member.name)

                    # Создаем папку с именем файла (без .gz и .txt), если она не существует
                    folder_name = os.path.join(ds_dir, gz_file_name[:-7])
                    os.makedirs(folder_name, exist_ok=True)

                    # извлекаем содержимое tar файла
                    with tar.extractfile(member) as gz_file:
                        if gz_file is not None:
                            # Сохраняем извлеченный файл в нужной папке
                            output_path = os.path.join(folder_name, gz_file_name[:-3])
                            # не сохраняя gz файл сразу извелкаем его содержимое и сохраняем в папку
                            # в конечном итоге в папке окажется разархивированный txt файл
                            with gzip.open(gz_file, "rb") as f_in:
                                with open(output_path, "wb") as f_out:
                                    f_out.write(f_in.read())  # Записываем

                    # из полученного текстового файла вычленяем 4 таблицы
                    self.separate_tables(output_path)

        with self.output().open("w") as f:
            f.write(f"Базовый архив распакован в папку: {ds_dir}\n")
            f.write(f"Из файлов выделены таблицы и записаны в отдельные tsv файлы")

    # функция разделения 4 таблиц из одного файла в 4 отдельных TSV файла
    def separate_tables(self, fname):
        dfs = {}
        with open(fname, "r") as f:
            write_key = None
            fio = io.StringIO()
            for l in f.readlines():
                if l.startswith("["):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == "Heading" else "infer"
                        dfs[write_key] = pd.read_csv(fio, sep="\t", header=header)
                    fio = io.StringIO()
                    write_key = l.strip("[]\n")
                    continue
                if write_key:
                    fio.write(l)
            fio.seek(0)
            dfs[write_key] = pd.read_csv(fio, sep="\t")

        # cохраняем таблицы в отдельные файлы
        for key, df in dfs.items():
            output_file = Path(fname).parent / f"{key}.tsv"
            df.to_csv(output_file, sep="\t", index=False)


class DataPreprocessing(luigi.Task):
    """
    Выполняет процедуры предобработки данных:
    - из файлов file_preprocess (список файлов) удалет колонки columns_del
    - сохраняет урезанные файлы с постфиксом _preprocess
    """

    dataset_name = luigi.Parameter()
    data_folder = luigi.Parameter()
    file_preprocess = luigi.ListParameter()
    columns_del = luigi.ListParameter()

    # определяем зависимость данной задачи от задачи ExtractFiles,
    # поскольку нам нужна таблица Probes, которая готовится в ExtractFiles
    def requires(self):
        return ExtractFiles(
            dataset_name=self.dataset_name, data_folder=self.data_folder
        )

    def output(self):
        ds_dir = Path(os.path.join(self.data_folder, self.dataset_name))
        files = []
        for pattern in self.file_preprocess:
            f_preprocess = list(ds_dir.rglob(pattern))
            if f_preprocess:
                for fl in f_preprocess:
                    name, ext = os.path.splitext(fl)
                    files.append(f"{name}_preprocessed{ext}")

        return [luigi.LocalTarget(file_name) for file_name in files]

    def run(self):
        ds_dir = Path(os.path.join(self.data_folder, self.dataset_name))

        # ищем все файлы из переменной table_preprocess внутри ds_dir
        for pattern in self.file_preprocess:
            # получаем список путей, в случае, если файлы найдены
            f_preprocess = list(ds_dir.rglob(pattern))
            if f_preprocess:
                for fl in f_preprocess:
                    # подтягиваем файлы в pandas dataframe
                    df = pd.read_csv(fl, sep="\t")
                    # удаляем лишние столбцы
                    df_prep = df.drop(columns=list(self.columns_del), errors="ignore")
                    # формируем новое название файла
                    name, ext = os.path.splitext(fl)
                    # сохраняем урезанные файлы
                    df_prep.to_csv(f"{name}_preprocessed{ext}", sep="\t", index=False)


class DelTempFiles(luigi.Task):
    """
    Очищает папки от лишних файлов:
    - удаляет скачанный по ссылке файл
    - удаляет разархивированные txt файлы
    """

    dataset_name = luigi.Parameter(default="GSE68849")
    data_folder = luigi.Parameter(default="data")
    file_preprocess = luigi.ListParameter(default=["Probes.tsv"])
    columns_del = luigi.ListParameter(
        default=[
            "Definition",
            "Ontology_Component",
            "Ontology_Process",
            "Ontology_Function",
            "Synonyms",
            "Obsolete_Probe_Id",
            "Probe_Sequence",
        ]
    )

    def requires(self):
        return DataPreprocessing(
            dataset_name=self.dataset_name,
            data_folder=self.data_folder,
            file_preprocess=self.file_preprocess,
            columns_del=self.columns_del,
        )

    def output(self):
        # поскольку данный класс не возвращает обработанные данные,
        # а занимается очисткой папок, рекомендуется возвращать файл-метку
        # который будет означать успешное завершение процесса.
        return luigi.LocalTarget(
            Path(self.data_folder) / self.dataset_name / "done.txt"
        )

    def run(self):
        ds_dir = Path(os.path.join(self.data_folder, self.dataset_name))
        del_files = []

        # не очень понял из задания нужно ли удалять скачанный tar файл
        # но на всякий случай удалю, потому что он занимает много места и большой
        # ценности в нем уже нет.
        path_ds = os.path.join(self.data_folder, f"{self.dataset_name}.tar")
        if os.path.exists(path_ds):
            os.remove(path_ds)
            del_files.append(path_ds)

        # находим и удаляем все txt файлы
        for txt_file in ds_dir.rglob("*.txt"):
            os.remove(txt_file)
            del_files.append(txt_file)

        # сохраняем информацию о завершении процесса и удалнных файлах в файл
        with self.output().open("w") as f:
            f.write("Пайплайн завершен.\n Временные файлы удалены:\n")
            for fl in del_files:
                f.write(f" - {fl}\n")


class CreateDataSet(luigi.WrapperTask):
    """
    Обертка для задачи DelTempFiles.
    """

    dataset_name = luigi.Parameter(default="GSE68849")
    data_folder = luigi.Parameter(default="data")
    file_preprocess = luigi.ListParameter(default=["Probes.tsv"])
    columns_del = luigi.ListParameter(
        default=[
            "Definition",
            "Ontology_Component",
            "Ontology_Process",
            "Ontology_Function",
            "Synonyms",
            "Obsolete_Probe_Id",
            "Probe_Sequence",
        ]
    )

    def requires(self):
        return DelTempFiles(
            dataset_name=self.dataset_name,
            data_folder=self.data_folder,
            file_preprocess=self.file_preprocess,
            columns_del=self.columns_del,
        )


if __name__ == "__main__":
    luigi.run()
