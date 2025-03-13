#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""An apache beam pipeline for downloading, processing and uploading youtube videos to a cloud bucket"""


import argparse
import logging
import re
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.filesystem import BeamIOError

import subprocess
# from pydub import AudioSegment
import json

from minio import Minio
import psycopg2
import glob

# import librosa
# import soundfile as sf



class YTPipelineDoFn(beam.DoFn):
    """Contains all the major functions for downloading, processing and uploading youtube videos."""

    def __init__(self, lang=None,channel_domain_map=None):
        # super().__init__()
        self.lang = lang  
        self.channel_domain_map = channel_domain_map

    def process(self, element):
        """Returns an iterator over the words of this element.
        The element is a line of text.
        Args:
            element: the element being processed
            82
        Returns:
            processed [video_id, duration, file_size], [str(0)] if there is an error.
        """
        temp_dir = "/tmp/mahadhwani_downloads"        ## actual VM directory
        # temp_dir = "/home/asr/deovrat/mahadhwani/mahadhwani_dataflow_pipeline/temp_downloads"
        os.makedirs(temp_dir, exist_ok=True)
        video_id=element.strip()
        try:
            eos_client = self.create_minio_client()
            self.update_table(video_id,temp_dir,0,0,self.lang,eos_client,'YT_PROCESSING')
            skip_flag = self.download_audio(video_id,temp_dir,eos_client)
            if skip_flag=='skip':
                duration, file_size=self.get_size_duration_from_bucket(self.lang,video_id,temp_dir,eos_client)
                self.update_table(video_id,temp_dir,file_size,duration,self.lang,eos_client)
                return [video_id+' '+str(duration)+' '+str(file_size)]
            # self.process_audio(video_id,temp_dir)
            with open(f"{temp_dir}/{video_id}.info.json",'r') as f:
                temp_duration=json.load(f)['duration']
            if temp_duration<=3600:
                self.upload_audio(video_id,temp_dir,self.lang,eos_client,skip_flag)
                if skip_flag!='skip_audio':
                    duration, file_size=self.get_mp3_details(video_id,temp_dir)
                    os.remove(f"{temp_dir}/{video_id}.mp3")
                else:
                    duration, file_size=self.get_size_duration_from_bucket(self.lang,video_id,temp_dir,eos_client)
                # logging.error(f"{duration},{file_size}")
                self.update_table(video_id,temp_dir,file_size,duration,self.lang,eos_client)
                os.remove(f"{temp_dir}/{video_id}.info.json")
                return [video_id+' '+str(duration)+' '+str(file_size)]
            else:
                if skip_flag!='skip_audio':
                    os.remove(f"{temp_dir}/{video_id}.mp3")
                os.remove(f"{temp_dir}/{video_id}.info.json")
                return ['0']
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            try:
                if ('403' in str(e.stderr.decode()).lower()) and ('forbidden' in str(e.stderr.decode()).lower()):
                    logging.error(f"Reporting: VM unhealthy......")
                    # raise ConnectionError(f"IP blocked (403). VM unhealthy")
                    # raise beam.io.UnwritableFileError(f"VM unhealthy: {e}")
                    # raise BeamIOError(f"Failed to download: HTTP Error or IP blocked, forbidden. VM unhealthy")
                if ('private video' in str(e.stderr.decode()).lower()) or ('video unavailable' in str(e.stderr.decode()).lower()):
                    self.update_table(video_id,temp_dir,0,0,self.lang,eos_client,"YT_VIDEO_UNAVAILABLE")
            except:
                pass
            return ['0']
        finally:
            # if os.path.exists(f"{temp_dir}/{video_id}.mp3"):
            #     os.remove(f"{temp_dir}/{video_id}.mp3")
            # if os.path.exists(f"{temp_dir}/{video_id}.info.json"):
            #     os.remove(f"{temp_dir}/{video_id}.info.json")

            file_pattern = f"{temp_dir}/{video_id}*"            
            files_to_delete = glob.glob(file_pattern)
            for file_path1 in files_to_delete:
                if os.path.exists(file_path1):
                    os.remove(file_path1)


    
    def create_minio_client(self):
        eos_client = Minio('objectstore.e2enetworks.net',
                   access_key='',
                   secret_key='',
                   secure=True)
        return eos_client
    
    def check_file_availability(self,video_id,lang,eos_client):
        audio_flag = "not_processed"
        json_flag = "not_processed"
        objects = eos_client.list_objects('indic-asr',
                                  prefix=f"mahadhwani/{lang}/mp3/{video_id}.mp3",
                                  recursive=False)
        for obj in objects:
            audio_flag='processed'
        objects = eos_client.list_objects('indic-asr',
                                  prefix=f"mahadhwani/{lang}/json/{video_id}.info.json",
                                  recursive=False)
        for obj in objects:
            json_flag='processed'
        return audio_flag,json_flag


    def download_audio(self,video_id,temp_dir,eos_client):
        skip_flag='no_skip'
        audio_flag, json_flag = self.check_file_availability(video_id,self.lang,eos_client)
        output_file = f"{temp_dir}/{video_id}"           # video_id.mp3 and video_id.info.json - these two files get created 
        if audio_flag=="processed" and json_flag=="processed":
            return 'skip'
        else:
            if audio_flag=="processed" and json_flag=="not_processed":
                command = [
                    "yt-dlp",
                    "-cw",
                    "-o", output_file,
                    "--skip-download",
                    "--no-playlist",
                    "--write-info-json",
                    f"https://youtu.be/{video_id}",
                ]
                skip_flag='skip_audio'
            else:
                command = [
                    "yt-dlp",
                    "-f", "bestaudio/best",
                    "-cw",
                    "-o", output_file,
                    "--extract-audio",
                    # "--quiet",
                    "--audio-format", "mp3",
                    "--audio-quality", "0",
                    "--no-playlist",
                    "--write-info-json",
                    f"https://youtu.be/{video_id}",
                    "--ppa", "ffmpeg:-ac 1 -ar 16000"
                ]
        try:
            result = subprocess.run(command, stderr=subprocess.PIPE, timeout=300, check=True)      # will error out after 5 min
        except Exception as e:
            logging.error("download_audio() failed with error:")
            self.update_table(video_id,temp_dir,0,0,self.lang,eos_client,"YT_DOWNLOAD_FAILED")
            try:
                logging.error(e.stderr.decode())
            except:
                pass
            raise
        return skip_flag



    def upload_audio(self,video_id,temp_dir,lang,eos_client,skip_flag):
        audio_path=f"{temp_dir}/{video_id}.mp3"
        json_path=f"{temp_dir}/{video_id}.info.json"
        try:
            # selecting desired metadata fields
            with open(json_path,'r') as f:
                metadata=json.load(f)
            keys_to_keep = ['id', 'title', 'description', 'channel_id', 'channel_url', 'duration', 'view_count', 'age_limit', 'categories', 'tags', 'playable_in_embed', 'live_status', 'comment_count', 'location', 'like_count', 'channel', 'channel_follower_count', 'channel_is_verified', 'uploader', 'uploader_id', 'uploader_url', 'upload_date', 'timestamp', 'availability', 'extractor', 'extractor_key', 'display_id', 'fulltitle', 'duration_string', 'is_live', 'was_live', 'epoch', 'asr', 'filesize', 'format_id', 'format_note', 'source_preference', 'audio_channels', 'quality', 'has_drm', 'tbr', 'filesize_approx', 'language', 'language_preference', 'ext', 'vcodec', 'acodec', 'container', 'protocol', 'resolution', 'audio_ext', 'video_ext', 'vbr', 'abr', 'format', '_type']
            filtered_dict = {k: metadata[k] for k in keys_to_keep if k in metadata}

            # add domain info
            try:
                filtered_dict['domain'] = ','.join(self.channel_domain_map[filtered_dict["channel_id"]])
            except Exception as e:
                filtered_dict['domain'] = 'unavailable'
                
            with open(json_path.replace('.info','.info_temp'),'w') as f:
                json.dump(filtered_dict,f)
            os.replace(json_path.replace('.info','.info_temp'),json_path)

            # uploading audio and metadata to the bucket
            if skip_flag!='skip_audio':
                with open(audio_path, 'rb') as file_data:
                    file_stat = os.stat(audio_path)
                    eos_client.put_object('indic-asr', f"mahadhwani/{lang}/mp3/{video_id}.mp3",
                                        file_data, file_stat.st_size,
                                        content_type='audio/mpeg')
            with open(json_path, 'rb') as file_data:
                file_stat = os.stat(json_path)
                eos_client.put_object('indic-asr', f"mahadhwani/{lang}/json/{video_id}.info.json",
                                    file_data, file_stat.st_size,
                                    content_type='application/json')
        except Exception as e:
            logging.error("upload_audio() failed with error:")
            logging.error(e)
            raise

    def get_mp3_details(self,video_id,temp_dir):
        file_path=f"{temp_dir}/{video_id}.mp3"
        cmd = [
            "ffprobe", "-v", "error", "-show_entries", 
            "format=duration", "-of", 
            "default=noprint_wrappers=1:nokey=1", file_path
        ]
        file_size = os.path.getsize(file_path) / (1024 * 1024)
        file_size = round(file_size, 3)
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            duration = float(result.stdout.decode().strip())
            duration = round(duration, 3)
        except subprocess.CalledProcessError as e:
            logging.error("get_mp3_details() failed with error:")
            logging.error(e)
            raise
        # print(duration,file_size)
        return duration, file_size

    def get_size_duration_from_bucket(self,lang,video_id,temp_dir,eos_client):
        file_path=f"{temp_dir}/{video_id}.info.json"
        if not os.path.exists(file_path):
            data = eos_client.get_object('indic-asr', f"mahadhwani/{lang}/json/{video_id}.info.json")
            with open(file_path, 'wb') as file_data:
                for d in data.stream(32*1024):
                    file_data.write(d)

        with open(file_path,'r') as f:
            data=json.load(f)
        duration = data['duration']
        objects = eos_client.list_objects('indic-asr',
                                  prefix=f"mahadhwani/{lang}/mp3/{video_id}.mp3",
                                  recursive=False)
        for obj in objects:
            size_in_mb = obj.size / (1024 * 1024)
        return duration,round(size_in_mb, 3)

        
    def update_table(self,video_id,temp_dir,file_size,duration,lang,eos_client,download_status='success'):
        try:
            conn = psycopg2.connect(
            host="",
            database="postgres",
            user="postgres",
            password="",
            )
            cursor = conn.cursor()

            if download_status=='success':
                file_path = f"{temp_dir}/{video_id}.info.json"
                if not os.path.exists(file_path):
                    data = eos_client.get_object('indic-asr', f"mahadhwani/{lang}/json/{video_id}.info.json")
                    with open(file_path, 'wb') as file_data:
                        for d in data.stream(32*1024):
                            file_data.write(d)
                with open(file_path,'r') as f:
                    metadata = json.load(f)
                if 'domain' in metadata.keys():
                    domain = metadata['domain']
                else:
                    try:
                        domain = ','.join(self.channel_domain_map[metadata["channel_id"]])
                    except Exception as e:
                        domain = 'unavailable'

                
                update_query = """
                UPDATE public.audios
                SET size = %s,
                    duration = %s,
                    metadata = %s,
                    bucket_path = %s,
                    status = %s,
                    domain = %s
                WHERE id = %s;
                """

                size = file_size
                duration = duration
                bucket_path = f"e2e/indic-asr/mahadhwani/{lang}/mp3/{video_id}.mp3"
                status = "DOWNLOADED"
                metadata = json.dumps(metadata)
                # metadata=None
                id = video_id

                cursor.execute(update_query, (size, duration, metadata, bucket_path, status, domain, id))
                conn.commit()
            else:
                # if download_status=='failed':
                update_query = """
                UPDATE public.audios
                SET status = %s
                WHERE id = %s;
                """

                status = download_status #"YT_DOWNLOAD_FAILED"
                id = video_id

                cursor.execute(update_query, (status, id))
                conn.commit()

        except Exception as error: #psycopg2.DatabaseError
            logging.error(f"update_table() failed with error: {error}")
            if 'conn' in locals() or 'conn' in globals():
                if conn:
                    conn.rollback()
            raise
        finally:
            if 'cursor' in locals() or 'cursor' in globals():
                if cursor:
                    cursor.close()
            if 'conn' in locals() or 'conn' in globals():
                if conn:
                    conn.close()



def fetch_remaining_video_ids(lang):
    try:
        conn = psycopg2.connect(
        host="",
        database="postgres",
        user="postgres",
        password="",
        )

        cursor = conn.cursor()
        select_query = "SELECT id FROM public.audios WHERE lang = %s and status = %s;" 
        cursor.execute(select_query, (lang.lower(),'PENDING_DOWNLOAD'))
        records = cursor.fetchall()

        with open(f"remaining_{lang}.txt", "w") as file:
            for record in records:
                file.write(f"{record[0]}\n")
        # print('num records fetched:..............',len(records))
        logging.info(f"num records fetched:..............{len(records)}")
        

    except Exception as error: #psycopg2.DatabaseError
        conn.rollback()
        logging.error(f"fetch_remaining_video_ids() failed with error: {error}")
        raise
    finally:
        if 'cursor' in locals() or 'cursor' in globals():
                if cursor:
                    cursor.close()
        if 'conn' in locals() or 'conn' in globals():
            if conn:
                conn.close()
        
def upload_to_bucket_gsutil(lang):
    try:
        file_path = f"remaining_{lang}.txt"
        destination_path = f"gs://mahadhwani_pipeline_bucket/video_ids/remaining_{lang}.txt"
        gsutil_command = ['gsutil','cp',file_path,destination_path]
        subprocess.run(gsutil_command, check=True)
    except Exception as e:
        print('error uploading to google storage bucket:',e)
        raise
    finally:
        if os.path.exists(f"remaining_{lang}.txt"):
            os.remove(f"remaining_{lang}.txt")



def filter_successful_downloads(video_id):
    return video_id != '0'

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    parser.add_argument(
        '--lang',
        dest='lang',
        required=True,
        help='language bucket to upload the data to. e.g. Assamese')
    parser.add_argument(
        '--channel_domain_map_file',
        dest='channel_domain_map_file',
        required=True,
        help='a dictionary containing channel_ids and their respective domains')
    known_args, pipeline_args = parser.parse_known_args(argv)

    with open(known_args.channel_domain_map_file,'r') as f:
        channel_domain_map=json.load(f)

    fetch_remaining_video_ids(known_args.lang)
    upload_to_bucket_gsutil(known_args.lang)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
        
    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'Read' >> ReadFromText(known_args.input)
        output = (
            lines
            | 'Download' >> (beam.ParDo(YTPipelineDoFn(lang=known_args.lang,channel_domain_map=channel_domain_map)).with_output_types(str))
            | 'FilterSuccessfulVideoIds' >> beam.Filter(filter_successful_downloads))

        output | 'Write' >> WriteToText(known_args.output,num_shards=1) # , num_shards=1


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

