import pandas as pd
import json

json_files = ["Streaming_History_Audio_2019-2020_0.json", "Streaming_History_Audio_2020-2021_1.json", "Streaming_History_Audio_2021-2022_2.json", "Streaming_History_Audio_2022-2025_3.json"]

dataframes = []

for json_file in json_files:
    df = pd.read_json(json_file)
    csv_filename = json_file.replace(".json", ".csv")
    df.to_csv(csv_filename, index=False, encoding="utf-8")
    dataframes.append(df)

merged_df = pd.concat(dataframes, ignore_index=True)

merged_df = merged_df.rename(columns={'master_metadata_track_name':'track_name',
                                      'master_metadata_album_artist_name':'artist_name',
                                      'master_metadata_album_album_name':'album_name'})

merged_df = merged_df[['spotify_track_uri',
                       'ts','platform',
                       'ms_played',
                       'track_name',
                       'artist_name',
                       'album_name',
                       'reason_start',
                       'reason_end',
                       'shuffle',
                       'skipped'
                      ]]

merged_df['spotify_track_uri'] = merged_df['spotify_track_uri'].apply(lambda x: x.split(':')[-1] if isinstance(x, str) else None)

merged_df.to_csv("merged_data3.csv", index=False, encoding="utf-8")
