import json
from pprint import pprint

d=None
with open('temp.json') as json_data:
    d = json.load(json_data)
    json_data.close()
    #pprint(d)

dim_counter=0
units_counter = 0
for dimension in d["dimension"]:
    dim_counter = dim_counter+1
    #print(dimension)
    name = dimension["name"]
    print('insert into tDimension(id, name, description) values ({}, "{}", "{}");'.format(
            dim_counter,
            dimension["name"].encode('utf-8').strip().replace('"','\\"'),
            dimension["name"].encode('utf-8').strip().replace('"','\\"')
            )
        )
    for unit in dimension["unit"]:
        units_counter = units_counter+1
        #print unit
        print('insert into tUnit(id, dimension_id, name, abbreviation, lf, cf, description) values ({}, {}, "{}", "{}", "{}", "{}", "{}");'.format(
                units_counter,
                dim_counter,
                unit["name"].encode('utf-8').strip().replace('"','\\"'),
                unit["abbr"].encode('utf-8').strip().replace('"','\\"'),
                unit["lf"].encode('utf-8').strip().replace('"','\\"'),
                unit["cf"].encode('utf-8').strip().replace('"','\\"'),
                unit["name"].encode('utf-8').strip().replace('"','\\"')
                )
                )
