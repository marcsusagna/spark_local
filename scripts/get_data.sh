#!/bin/bash

mkdir data
# Data can be found in http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html
cd data
wget http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz
file_name=lastfm-dataset-1K.tar.gz
tar xvf "$file_name"