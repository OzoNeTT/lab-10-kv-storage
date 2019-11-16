[![Build Status](https://travis-ci.com/OzoNeTT/lab-10-kv-storage.svg?branch=master)](https://travis-ci.com/OzoNeTT/lab-10-kv-storage)
![GitHub Repo size](https://img.shields.io/github/repo-size/OzoNeTT/lab-10-kv-storage.svg)
![Some badge](https://img.shields.io/badge/OzoNe-T__T-blue.svg)
![forthebadge](https://forthebadge.com/images/badges/not-an-issue.svg)

# Лабораторная работа № 10

## Задание

Реализовать утилиту вычисления контрольной суммы записей **key-value** хранилища.<br />

> **key-value** хранилище является простейшим хранилищем данных, использующим ключ для доступа к значению. 
> Такие хранилища используются для хранения изображений, создания специализированных файловых систем, 
> в качестве кэшей для объектов, а также в системах, спроектированных с прицелом на масштабируемость.

## Иллюстрация

Утилита проходит в несколько потоков содежимое исходного хранилаща 
```md
# db-cat source.db

| column family | column family | column family |
|---------------|---------------|---------------|
| key1 : value1 | key5 : value5 | key7 : value7 |
|---------------|---------------|---------------|
| key2 : value2 | key6 : value6 | key8 : value8 |
|---------------|---------------|---------------|
| key3 : value3 |               | key9 : value9 |
|---------------|---------------|---------------|
| key4 : value4 |               |               |
|---------------|---------------|---------------|
```
и заполняет новое хранилище контрольной суммой для каждой записи

```md
# db-cat dbcs-source.db

| column family | column family | column family |
|---------------|---------------|---------------|
| key1 : hash1  | key5 : hash5  | key7 : hash7  |
|---------------|---------------|---------------|
| key2 : hash2  | key6 : hash6  | key8 : hash8  |
|---------------|---------------|---------------|
| key3 : hash3  |               | key9 : hash9  |
|---------------|---------------|---------------|
| key4 : hash4  |               |               |
|---------------|---------------|---------------|
```

Контрольная сумма вычисляется от значения и от ключа записи:
```
hash = sha256(key, value)
```

## Требования

- Утилита должна поддерживать [**rocksdb**](https://github.com/facebook/rocksdb) с множетсвенным семейством стоблоцов.
- Утилита, должна обработаывать следующие опции: `log-level`, `thread-count` и `output`.
- В качестве алгоритма хеширования для подсчета контрольной суммы выбрыть **SHA-256**.
- При реализации использовать шаблон проектирования **producer-consumer**.

## Пример

```Shell
# получение справочной информации
$ dbcs --help

Usage:

  dbcs [options] <path/to/input/storage.db>

Options:

  --log-level <string>          = "info"|"warning"|"error"         
                                = default: "error"
                                
  --thread-count <number>       = 
                                = default: count of logical core

  --output <path>               = <path/to/output/storage.db>
                                = default: <path/to/input/dbcs-storage.db>

# вычисление контрольной суммы
$ dbcs /dir/file.db
...
```

## Рекомендации

- Для обработки параметров командной строки использовать компонент `boost::program_options`.
- Для логгирования работы утилиты использовать компонент `boost::log`.
- Для вычисления контрольной суммы использовать библиотеку [**PicoSHA2**](https://github.com/okdshin/PicoSHA2).
- Для подключения [**Boost**](https://docs.hunter.sh/en/latest/packages/pkg/Boost.html) и
[**rocksdb**](https://docs.hunter.sh/en/latest/packages/pkg/rocksdb.html) использовать пакетный менеджер **Hunter**.
