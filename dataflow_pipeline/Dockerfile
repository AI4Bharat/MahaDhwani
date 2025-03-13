FROM apache/beam_python3.10_sdk:2.58.1

RUN apt-get update
RUN apt-get install -y --no-install-recommends ffmpeg

# RUN pip install yt-dlp==2024.3.10
RUN pip install yt-dlp
RUN pip install minio
RUN pip install psycopg2-binary



# sudo docker build . --tag us-east1-docker.pkg.dev/mahadhwani/youtube-pipeline-docker/dataflow/pratinidhi_docker:v1
# sudo docker push us-east1-docker.pkg.dev/mahadhwani/youtube-pipeline-docker/dataflow/pratinidhi_docker:v1
