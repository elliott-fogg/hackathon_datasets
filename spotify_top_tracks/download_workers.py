from os.path import join as pjoin
from os.path import isdir
from os import mkdir
import requests

def mp_download_files(chart_type, country_list, date_list, queue, name):
    total_count = len(country_list) * len(date_list)
    current_count = 0
    error_count = 0
    queue.put( (name, total_count, current_count, error_count) )
    
    output_base = pjoin("data", chart_type)
    
    for country_abbrev, country_name in country_list:
        
        output_folder = pjoin(output_base, country_abbrev)
        if not isdir(output_folder):
            mkdir(output_folder)
            
        for date in date_list:
            
            # Add updated information to the queue every ten files
            if current_count % 10 == 0:
                queue.put( (name, total_count, current_count, error_count) )
            
            # Get the data
            try:
                # Attempt to get the data
                r_temp = requests.get("https://spotifycharts.com/{}/{}/daily/{}/download".format(
                    chart_type, country_abbrev, date))
                assert r_temp
                
                output_text = "# {} chart for {} on {}\n#".format(chart_type, country_name, date) + r_temp.text
                output_folder = pjoin(output_base, country_abbrev)
                output_filename = "{}_{}_{}.csv".format(chart_type, country_abbrev, date)
                output_filepath = pjoin(output_folder, output_filename)
                
                with open(output_filepath, "wb") as f:
                    f.write(output_text.encode('utf-8'))
            
            except Exception as e:
                log_text = "EXCEPTION: {}".format(e)
                log_name = "data/logs/{}_{}_{}.txt".format(chart_type, country_abbrev, date)
                with open(log_name, "w") as log:
                    log.write(log_text)
                error_count += 1
            
            current_count += 1
    
    queue.put( (name, total_count, current_count, error_count) )