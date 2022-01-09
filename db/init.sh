rm -rf tdata/*
rm -rf cdata/*

mkdir tdata/news tdata/tweets
mkdir cdata/news cdata/tweets

gsutil -m cp gs://clean_items/news/* tdata/news
gsutil -m cp gs://clean_items/tweets/* tdata/tweets

for file in tdata/news/*
do
	tar xvf "$file" -C cdata/news
done

for file in tdata/tweets/*
do
	tar xvf "$file" -C cdata/tweets
done

python es.py

rm -rf tdata/news/*
rm -rf cdata/news/*

rm -rf tdata/tweets/*
rm -rf cdata/tweets/*