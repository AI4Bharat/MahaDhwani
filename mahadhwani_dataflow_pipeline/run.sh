lang="Sanskrit"
lang_lowercase=$(echo "$lang" | awk '{print tolower($0)}')
python pipeline.py \
    --region us-west1 \
    --input gs://mahadhwani_pipeline_bucket/video_ids/remaining_$lang.txt \
    --output gs://mahadhwani_pipeline_bucket/results/outputs/$lang.txt \
    --runner DataflowRunner \
    --project mahadhwani \
    --temp_location gs://mahadhwani_pipeline_bucket/tmp/ \
    --lang $lang \
    --channel_domain_map_file "/home/asr/deovrat/mahadhwani/mahadhwani_dataflow_pipeline/languages/channel_domain_mapping_all_lang.json" \
    --sdk_container_image "us-east1-docker.pkg.dev/mahadhwani/youtube-pipeline-docker/dataflow/pratinidhi_docker3:v3" \
    --sdk_location "container" \
    --machine_type "n1-standard-1" \
    --job_name "$lang_lowercase" \
    --num_workers 50 \
    --autoscaling_algorithm "NONE"