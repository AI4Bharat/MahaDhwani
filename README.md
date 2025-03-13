# MahaDhwani
MahaDhwani is a corpus comprising 279K hours of raw audio across 22 Indian languages. We propose a framework to create large raw audio datasets for under-represented languages by collating publicly accessible audio content.

<img width="400" alt="MahaDhwani stats" src="https://github.com/AI4Bharat/MahaDhwani/blob/master/stats/mahadhwani%20stats.png" />

## Download 
Use [Dataflow pipeline](https://github.com/AI4Bharat/MahaDhwani/tree/master/dataflow_pipeline) to download audios from youtube.

## Eval
### 1. Effect of Pretraining -
In Figure 3, we compare the performance of 9 ASR models fine-tuned on the IndicVoices dataset, starting with (i) random initialisation (ii) pretrained checkpoint of an English ASR model, Nvidia-En-SSL (iii) the IndicConformer-HMS model pretrained on MahaDhwani. Each of the above is trained 12.5%, 50% and 100% of the labeled training data.

<img width="400" alt="MahaDhwani eval" src="https://github.com/AI4Bharat/MahaDhwani/blob/master/stats/IV12.5%20(1).png" />

### 2. Comparisons with other models -
We compare IndicConformer-HMS with existing massively multilingual models, namely, USM, Whisper and MMS. We find that it significantly outperforms all existing models.

<img width="700" alt="MahaDhwani pretrained ckpt comparison" src="https://github.com/AI4Bharat/MahaDhwani/blob/master/stats/mahadhwani%20eval.png" />
