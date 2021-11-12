#!/bin/sh
rm -rf test1.tsv
echo "Reading webpages ..."
python3 read2text.py data/sample.warc.gz
