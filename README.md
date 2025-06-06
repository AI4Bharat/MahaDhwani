# MahaDhwani
MahaDhwani is a corpus comprising 279K hours of raw audio across 22 Indian languages and English. We propose a framework to create large raw audio datasets for under-represented languages by collating publicly accessible audio content.

<img width="400" alt="MahaDhwani stats" src="https://github.com/AI4Bharat/MahaDhwani/blob/master/stats/mahadhwani%20stats.png" />

## Download 

#### Our Dataflow Pipeline works as follows-
1. Video_ids are fetched from a postgreSQL table which contains all the video_ids, metadata and related info(which is blank intially).
2. Video_ids are assigned to each VM through dataflow.
3. Audios are downloaded on each VM using yt-dlp and then uploaded to the cloud bucket.
4. After successful upload, the PostgreSQL table is updated with the metadata, bucket_path, duration, file size, etc.

#### Steps to run the pipeline -
1. Setup a PostgreSqL table, a cloud bucket and update the code in ```pipeline.py``` accordingly.
2. Setup a GCP account for dataflow access.
3. Create and push the ```Dockerfile``` provided for setting up VM environments.
      - Make sure that the apache beam version in the dockerfile matches with the local environment(Python 3.10 and Apache beam sdk 2.58.1 were used here).
4. Run the bash script - 
```
bash run.sh
```
5. For filtering video_ids based on metadata, ```dataflow_pipeline/languages/*/video_ids_*.csv``` files can be used.

## Model
| Model | Link |
|----------|----------|
| MahaDhwani Pretrained Conformer Encoder  | [HF](https://huggingface.co/ai4bharat/MahaDhwani_pretrained_conformer)  |

## Analysis
### 1. Effect of Pretraining -
We compare the performance of ASR models fine-tuned on the IndicVoices dataset, starting with (i) random initialisation (ii) pretrained checkpoint of an English ASR model, Nvidia-En-SSL (iii) the IndicConformer-HMS model pretrained on MahaDhwani. Each of the above is trained 12.5%, 50% and 100% of the labeled training data.

<img width="400" alt="MahaDhwani eval" src="https://github.com/AI4Bharat/MahaDhwani/blob/master/stats/IV12.5%20(1).png" />

### 2. Comparisons with other models -
We compare IndicConformer-HMS(pretrained with MahaDhwani) with existing massively multilingual models, namely, USM, Whisper and MMS. We find that it significantly outperforms all existing models.

<img width="700" alt="MahaDhwani pretrained ckpt comparison" src="https://github.com/AI4Bharat/MahaDhwani/blob/master/stats/mahadhwani%20eval.png" />
