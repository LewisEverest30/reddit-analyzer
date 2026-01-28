import json

url_file_json = None
with open('./outputs/dogs/dogs_urls.json', 'r', encoding='utf-8') as f:
    url_file_json = json.load(f)

url_set = set()
for url_obj in url_file_json['collected_urls']:
    if url_obj['url'] in url_set:
        print(f"Duplicate URL found: {url_obj['url']}")
    else:
        url_set.add(url_obj['url'])
print("URL uniqueness check completed. Total unique URLs:", len(url_set))