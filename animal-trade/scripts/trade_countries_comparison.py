
import csv

all_country_codes = set()
missing = set()
with open("../data/country_region_mapping.csv", 'r') as country_region_f:
    reader = csv.reader(country_region_f)
    header = next(reader)
    for line in reader:
        iso_code = line[2]
        all_country_codes.add(iso_code.strip())

print(all_country_codes)
print(len(all_country_codes))
with open("../data/CITIES_data.csv", 'r') as cities_data_file:
    reader = csv.reader(cities_data_file)

    header = next(reader)

    for line in reader:
        # extract the used country codes

        importer = line[7].strip()
        exporter = line[8].strip()
        origin = line[9].strip()

        if importer not in all_country_codes:
            missing.add(importer)
        if exporter not in all_country_codes and exporter not in missing:
            missing.add(exporter)
        if origin not in all_country_codes and origin not in missing:
            missing.add(origin)

print(missing)




