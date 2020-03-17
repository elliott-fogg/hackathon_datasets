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

def log_error(folder, country, date, reason, error_text="0"):
    error_file_name = "{}_{}_{}.txt".format(reason, country, date)
    write_file(pjoin(folder, error_file_name), error_text)

def download_chart_files(chart_type, country_list, date_list, queue, name):
    total_count = len(country_list) * len(date_list)
    current_count = 0
    error_count = 0
    queue.put( (name, total_count, current_count, error_count) )
    
    base_dir = pjoin("data", chart_type)
    log_dir = pjoin(base_dir, "logs")
    
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
            error_logs = []
            success = False
            output_text = None
            attempt = 0
            
            request_url = "https://spotifycharts.com/{}/{}/daily/{}/download".format(chart_type, country, date)
            
            while not request_complete and attempt < 10:
                
                # Wait a second between attempts in case the server is overloaded.
                if attempt > 0:
                    time.sleep(1)
                    
                attempt += 1
                
                try:
                    # Request the data
                    r = requests.get(request_url)
                    
                    if r.reason == "Not Found":
                        # Invalid request URL. Log and skip.
                        log_error(log_dir, country, date, 'not-found', request_url)
                        break
                    
                    if not r:
                        # Unexpected request error. Log and try again.
                        error_logs.append("Unexpected request error. Code: '{}'. Reason: '{}'.".format(r.status_code, r.reason))
                        continue

                                          
                    # Request successful. Determine if returned data is valid
                    raw_text = r.text
                    first_line, _, remaining_text = raw_text.partition("\n")
                    
                    if first_line in (REGIONAL_HEADER, VIRAL_HEADER):
                        # raw_text is just the .csv file
                        output_text = raw_text
                        success = True
                        break
                    
                    
                    elif first_line == PREFACE_LINE:
                        # raw_text includes one unwanted line about stream count. Discard and save the rest.
                        output_text = remaining_text
                        success = True
                        break
                        
                    
                    elif first_line == HTML_HEADER:
                        # Request has returned a page of HTML. Need to check whether this was an error at the website,
                        # or if the desired chart does not exist.
                        
                        soup = bs(raw_text, 'html.parser')
                        error_div = soup.find("div", attrs={"class": "chart-error"})
                        
                        if error_div == None:
                            # Unexpected HTML. Could be a mistake by the website where it returns the page for the URL
                            # without the '/download' suffix. Log and try again.
                            error_logs.append(raw_text)
                            continue
                            
                        else:
                            # A chart-error div was found. Determine whether the website failed, or the chart does not exist.
                            error_text = error_div.text.strip()
                            
                            if error_text == 'Sorry, there was an error accessing the chart. Please try again later.':
                                # Chart exists, but the website has failed. Log and try again.
                                error_logs.append(error_text)
                                continue
                            
                            elif error_text == 'This chart does not exist. Please make another selection from the dropdown menus.':
                                # Chart does not exist. Log and skip.
                                log_error(log_dir, country, date, 'chart-not-exist')
                                request_complete = True
                                continue
                            
                            else:
                                # Chart-error div exists, but with an unknown message. Log and try again.
                                error_logs.append(error_text)
                                continue
                    
                    else:
                        # First line is unexpected. Log and try again.
                        error_logs.append(raw_text)
                        continue
                    
                except Exception as e:
                    # An unexpected error has occurred during the execution. Log and try again.
                    error_logs.append(str(e))
                
                
            # Exited the loop. Check if the request was successful, or if a log needs to be made.
            if request_complete:
                # Request was successful. Save the output.
                output_filename = "{}_{}_{}.csv".format(chart_type, country, date)
                output_filepath = pjoin(output_folder, output_filename)
                write_file(output_filepath, output_text)
                
            else:
                # Request failed. Save the logs if necessary.
                if len(error_logs) > 0:
                    log_error(log_dir, country, date, "general-error", "\n\n===\n\n".join(error_logs))
                    
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
    