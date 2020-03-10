from os.path import join as pjoin
from os.path import isdir
from os import mkdir
import requests
from bs4 import BeautifulSoup as bs
import pandas

REGIONAL_HEADER = 'Position,"Track Name",Artist,Streams,URL'
VIRAL_HEADER = 'Position,"Track Name",Artist,URL'
HTML_HEADER = '<!doctype html>'
PREFACE_LINE = ',,,"Note that these figures are generated using a formula that protects against any artificial inflation of chart positions.",'

def write_file(filepath, text):
    file = open(filepath, "wb")
    file.write(text.encode('utf-8'))
    file.close()

def download_chart_files(chart_type, country_list, date_list, queue, name):
    total_count = len(country_list) * len(date_list)
    current_count = 0
    error_count = 0
    queue.put( (name, total_count, current_count, error_count) )
    
    base_dir = pjoin("data", chart_type)
    
    for country in country_list:
        
        output_folder = pjoin(base_dir, country)
        if not isdir(output_folder):
            mkdir(output_folder)
            
        for date in date_list:
            
            # Add updated information to the queue every ten files
            current_count += 1
            if current_count % 10 == 0:
                queue.put( (name, total_count, current_count, error_count) )
            
            # Get the data
            try:
                # Attempt to get the data
                r_temp = requests.get("https://spotifycharts.com/{}/{}/daily/{}/download".format(
                    chart_type, country, date))
                
                if r_temp.reason == "Not Found":
                    logfile = pjoin(base_dir, "logs", "missing_{}_{}.txt".format(country, date))
                    write_file(logfile, "0")
                    continue
                
                raw_text = r_temp.text
                first_line, _, remaining_text = raw_text.partition("\n")
                
                if first_line in (REGIONAL_HEADER, VIRAL_HEADER):
                    # raw_text is just the .csv file, save it
                    output_text = raw_text
                
                elif first_line == PREFACE_LINE:
                    # raw_text includes one preface line about stream count, save the rest
                    output_text = remaining_text
                
                elif first_line == HTML_HEADER:
                    # request has returned an HTML page. Need to check whether the chart is present,
                    # whether the chart has malfunctioned temporarily, or whether the chart does not exist.
                    
                    # Check if it has an error box, saying that the
                    soup = bs(raw_text, 'html.parser')
                    error_div = soup.find("div", attrs={"class":"chart-error"})
                    
                    if error_div == None:
                        # Unexpected HTML. Could be a returned HTML of the spotifycharts page, for some reason.
                        # Log the returned text.
                        logfile = pjoin(base_dir, "logs", "unexpected-html_{}_{}.txt".format(country, date))
                        write_file(logfile, raw_text)
                        error_count += 1
                        continue
                    
                    else:
                        # A chart-error div was found
                        error_text = error_div.text.strip()
                        if error_text == 'Sorry, there was an error accessing the chart. Please try again later.':
                            # The chart exists, but there was an error. Log to try again later.
                            logfile = pjoin(base_dir, "logs", "access-error_{}_{}.txt".format(country, date))
                            write_file(logfile, "0")
                            continue
                        
                        elif error_text == 'This chart does not exist. Please make another selection from the dropdown menus.':
                            # The chart doesn't exist. Log that this combo should not be retried.
                            logfile = pjoin(base_dir, "logs", "not-exist_{}_{}.txt".format(country, date))
                            write_file(logfile, "0")
                            continue
                        
                        else:
                            # chart-error div exists, but does not contain expected text. Log the returned text. 
                            logfile = pjoin(base_dir, "logs", "unexpected-chart-error_{}_{}.txt".format(country, date))
                            write_file(logfile, raw_text)
                            error_count += 1
                            continue

                else:
                    # First line is unexpected, create a log file and skip entry
                    logfile = pjoin(base_dir, "logs", "unexpected-start_{}_{}.txt".format(country, date))
                    error_count += 1
                    write_file(logfile, raw_text)
                    continue
                
                # Beyond this point, the data should be correct, and output_text should be assigned
                output_folder = pjoin(base_dir, country)
                output_filename = "{}_{}_{}.csv".format(chart_type, country, date)
                output_filepath = pjoin(output_folder, output_filename)
                
                write_file(output_filepath, output_text)  

            
            except Exception as e:
                logfile = pjoin(base_dir, "logs", "unknown-error_{}_{}.txt".format(country, date))
                log_text = "EXCEPTION: {}".format(str(e))
                write_file(logfile, log_text)
                error_count += 1
    
    queue.put( (name, total_count, current_count, error_count) )
    
def get_track_info(input_data):
    
    chunk_number, chart_type, song_ids, sp = input_data
    
    df = pandas.DataFrame()
    
    track_list = sp.tracks(song_ids)['tracks']
    feature_list = sp.audio_features(song_ids)
    
    output_folder = pjoin("data", chart_type, "_track_info")
    
    for i in range(len(song_ids)):
        track_info = track_list[i]
        track_features = feature_list[i]
        
#         if track_info['id'] != track_features['id']:
#             logfile = pjoin("data", chart_type, "_track_info", "logfile")
#             write_file(logfile, "Misaligned ID numbers\n" + track_info['id'] + " " + feature_list['id'])
#             continue

        track_dict = {
            "id": track_info['id'],
            "name": track_info['name'],
            "explicit": track_info['explicit'],
            "duration": track_info['duration_ms'],
            "current_popularity": track_info['popularity'],
            "available_markets": tuple(track_info['available_markets']),
            "artist_id": track_info['artists'][0]['id'],
            "artist_name": track_info['artists'][0]['name'],
            "album_id": track_info['album']['id'],
            "album_name": track_info['album']['name'],
            "album_tracks": track_info['album']['total_tracks'],
            "track_number": track_info['track_number'],
            "album_release_date": track_info['album']['release_date'],
            "release_date_precision": track_info['album']['release_date_precision']
        }
        
        if len(track_info['artists']) > 1:
            track_dict['additional_artists'] = tuple([(artist['name'], artist['id']) for artist in track_info['artists'][1:]])
        else:
            track_dict['additional_artists'] = None

            
        if track_features != None:
            for data_label in ('danceability','energy','key','loudness','mode','speechiness','acousticness','instrumentalness',
                                'liveness','valence','tempo','time_signature'):
                if data_label in track_features:
                    track_dict[data_label] = track_features[data_label]

        temp_df = pandas.DataFrame([track_dict])
        df = pandas.concat([df, temp_df])

    output_filepath = pjoin(output_folder, str(chunk_number))
    write_file(output_filepath, df.to_csv(index=False))

    
def get_artist_info(input_data):
    
    chunk_number, chart_type, artist_ids, sp = input_data
    
    df = pandas.DataFrame()
    
    artist_list = sp.artists(artist_ids)['artists']
    
    output_folder = pjoin("data", chart_type, "_artist_info")
    
    for artist in artist_list:
        artist_dict = {
            "id": artist['id'],
            "name": artist['name'],
            "genres": tuple(artist['genres']),
            "current_popularity": artist['popularity'],
            "current_followers": artist['followers']
        }
        
        temp_df = pandas.DataFrame([artist_dict])
        df = pandas.concat([df, temp_df])
    
    output_filepath = pjoin(output_folder, str(chunk_number))
    write_file(output_filepath, df.to_csv(index=False))
    