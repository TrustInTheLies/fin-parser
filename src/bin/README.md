# Comparer | Converter

Два приложения, использующих `fin-parser`, для использования требуется соблюдать следующий синтаксис:

## Comparer

`comparer --file1 <filename> --format1 <bin/csv/txt> --file2 <filename> --format2 <bin/csv/txt>`, 

где `filename` - имя файла, `bin/csv/txt` один из трех форматов



Пример:
`comparer --file1 converted.bin --format1 bin --file2 records_example.bin --format2 bin`

## Converter

`converter -i <filename> -if <bin/csv/txt> -of <bin/csv/txt>`, 

где `filename` - имя файла, `bin/csv/txt` один из трех форматов



Пример:
`converter -i converted.txt -if txt -of bin`
