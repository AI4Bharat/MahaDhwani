import json
import ast


langs=[ "Bodo", "Dogri", "Kashmiri", "Konkani", "Maithili", "Manipuri", "Santali", "Sindhi" ] #"Nepali"
path='languages'
for lang in langs:
    channel_dict={}
    with open(f"{path}/{lang}/domain_info_raw.txt",'r') as f:
        for line in f:
            temp=line.strip().split('<separator>')
            channel_id=temp[0].strip()
            domains=temp[1].strip().split(',')
            for i in range(len(domains)):
                domains[i]=domains[i].strip().lower()
            channel_dict[channel_id]=domains
    with open(f"{path}/{lang}/channel_domain_mapping.json", 'w') as f:
        json.dump(channel_dict,f)

langs=["Assamese", "Bengali", "Gujarati", "Hindi", "Kannada", "Malayalam", "Marathi", "Odia", "Punjabi", "Sanskrit", "Tamil", "Telugu", "Urdu"]
path='languages'
for lang in langs:
    channel_dict={}
    print(lang,'...........')
    with open(f"{path}/{lang}/domain_info_raw.txt",'r') as f:
        for line in f:
            temp=line.strip().split('<separator>')
            channel_id=temp[0].strip()
            domains=list(ast.literal_eval(temp[1].strip()))  #.replace("'",'"')
            for i in range(len(domains)):
                domains[i]=domains[i].strip().replace(f"{lang} ","").lower()
            channel_dict[channel_id]=domains
    with open(f"{path}/{lang}/channel_domain_mapping.json", 'w') as f:
        json.dump(channel_dict,f)

# langs=[ "Nepali" ]
# path='languages'
# for lang in langs:
#     try:
#         with open(f"/home/asr/deovrat/mahadhwani/mahadhwani_backup/mahadhwani_{lang}_50k_hrs/channel_domain_mapping.json", 'r') as f:
#             channel_dict=json.load(f)
#             for key in channel_dict.keys():
#                 for i in range(len(channel_dict[key])):
#                     channel_dict[key][i]=channel_dict[key][i].replace(f"{lang}","").strip().lower()
#         with open(f"{path}/{lang}/channel_domain_mapping.json", 'w') as f:
#             json.dump(channel_dict,f)
#         print(f"{lang}: channel domain mappings retrieved...........")
#     except:
#         print(f"{lang} doesn't have channel domain mappings")

langs=["Assamese", "Bengali", "Bodo", "Dogri", "Gujarati", "Hindi", "Kannada", "Kashmiri", "Konkani", "Maithili", "Malayalam", "Manipuri", "Marathi", "Nepali", "Odia", "Punjabi", "Sanskrit", "Santali", "Sindhi", "Tamil", "Telugu", "Urdu"]
path='/home/asr/deovrat/mahadhwani/mahadhwani_dataflow_pipeline/languages'
channel_dict={}
for lang in langs:
    try:
        with open(f"{path}/{lang}/channel_domain_mapping.json", 'r') as f:
            dict1=json.load(f)
        print(lang,':',len(dict1.keys()))
        channel_dict.update(dict1)
    except Exception as e:
        print(lang,':',e)

with open(f"{path}/channel_domain_mapping_all_lang.json", 'w') as f:
    json.dump(channel_dict,f)
print('All:',len(channel_dict.keys()))
