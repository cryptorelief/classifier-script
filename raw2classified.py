import time
import json
import requests
from db import Supply, Demand, Contact, Raw, get_session


def obj_to_dict(obj):
    return dict([(k,v) for (k,v) in vars(obj).items() if not k.startswith("_")])


def get_config():
    config_file_obj = open("config.json","r")
    config_data = json.load(config_file_obj)["data"]
    config_file_obj.close()
    return config_data


def save_config(last_id, config_data):
    config_data['last_id'] = last_id
    config_file_obj = open("config.json","w")
    json_data = {"data":config_data}
    json.dump(json_data,config_file_obj,indent=4,default=str)
    config_file_obj.close()


def get_raw(last_id):
    with get_session() as session:
        s = session.query(Raw).filter_by(id>=last_id).all()
        raw_data = [obj_to_dict(data) for data in s]
        last_id = raw_data[-1]['id'] + 1
    return raw_data, last_id


def transform(raw_data):
    transformed_data = []
    for data in raw_data:
        new_dict = {'metadata':{},'text':data['content']}
        transformed_data.append(new_dict)
        del new_dict
        pass
    return transformed_data


def classify(transformed_data):
    r = requests.post("https://nlp.covidbot.in/process",json={'messages':transformed_data})
    classified_data = r.json()
    return classified_data


def get_supplies_and_demands(classified_data, raw_data):
    supply_data = []
    demand_data = []
    for data in classified_data:
        new_dict = {'source':'nlp',
                    'phone':" ".join(data['phone']),
                    'resource_raw':" ".join(data['resource']),
                    'location_raw':" ".join(data['location']),
                    }
        if(data['type']=="supply"):
            supply_data.append(new_dict)
        elif(data['type']=="demand"):
            demand_data.append(new_dict)
        del new_dict
    return supply_data, demand_data


def data2db(table, new_data):
    with get_session() as session:
        table_objs_inserts = []
        contact_objs_inserts = []
        for d in new_data:
            contact_fields = ["source","tg_user_id","tg_user_handle"]
            if(any(item in contact_fields for item in list(d.keys()))):
                contact_dict = {}
                for entry in contact_fields:
                    if(entry in list(d.keys())):
                        if(entry=="tg_user_handle"):
                            contact_dict["user_handle"] = d.get(entry,"")
                        else:
                            contact_dict[entry] = d.get(entry)
                        if(entry!="source"):
                            d.pop(entry)
            contact_objs_inserts.append(contact_dict)
            table_objs_inserts.append(d)

    with get_session() as session:
        try:
            session.bulk_insert_mappings(Contact,contact_objs_inserts,return_defaults=True)
            for (i,d) in enumerate(table_objs_inserts):
                d['contact_id'] = contact_objs_inserts[i]['id']
            session.bulk_insert_mappings(table,table_objs_inserts)
            session.commit()
        except Exception as e:
            print("Error while saving: " + str(e))


if __name__=="__main__":
    if(sys.argv[1]):
        try:
            refresh_rate = int(sys.argv[1])*60
        except Exception as e:
            print("ERROR: Refresh Rate should be an integer!")
            raise SystemExit(e)
    else:
        refresh_rate = 300
        print("Executing with Default Refresh Rate of 5 minutes")

    while True:
        config_data = get_config()
        raw_data, last_id = get_raw(config_data['last_id'])
        transformed_data = transform(raw_data)
        classified_data = classify(transformed_data)
        supply_data, demand_data = get_supplies_and_demands(classified_data, raw_data)
        data2db(Supply, supply_data)
        data2db(Demand, demand_data)
        save_config(last_id, config_data)
        time.sleep(refresh_rate)
